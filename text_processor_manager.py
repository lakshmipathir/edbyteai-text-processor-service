from contextlib import contextmanager
import json
import math
import os
from pathlib import PurePosixPath
from tempfile import TemporaryDirectory
from urllib.parse import unquote

from pydantic import BaseModel, ConfigDict, Field, field_validator

from edbyteai_base_module.base import EdbyteAIBaseModule
from custom_exception import (
    ContentExtractionIncompleteException,
    ContentFileMismatchException,
    ContentPostProcessException,
    InvalidContentPathException,
    InvalidFileTypeException,
    MissingConfigurationException,
    PdfPageCountException,
    SqsMessagePublishException,
    UnknownSqsTriggerException,
)
from sql_queries import (
    sql_get_content_file_for_preprocess,
    sql_lock_content_extractions_for_post_process,
    sql_lock_content_file_for_post_process,
    sql_update_content_text_file_for_post_process,
)


PDF_EXTENSIONS = {".pdf"}
IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".webp", ".bmp", ".tif", ".tiff"}
IMAGE_CONTENT_TYPES = {"image/jpeg", "image/png", "image/webp", "image/bmp", "image/tiff"}
START_TEXT_PRE_PROCESSOR_QUEUE_NAME = "start-text-pre-processor"
START_TEXT_POST_PROCESS_QUEUE_NAME = "start-text-post-process"
START_SYLLABUS_ANALYSIS_QUEUE_NAME = "start-syallabus-analysis"
EVENT_TEMP_ROOT_ENV_NAME = "TEXT_PROCESSOR_EVENT_TEMP_ROOT"
CONTENT_TABLE_NAME = "edbyteai.content"
CONTENT_EXTRACTION_TABLE_NAME = "edbyteai.content_extraction"


class PreProcessorMessageModel(BaseModel):
    tenant_id: int
    academic_year_id: int
    section_id: int
    course_id: int
    content_file_id: int
    bucket_name: str = Field(min_length=1, pattern=r"^\S+$")
    s3_key: str = Field(min_length=1)

    @field_validator("s3_key")
    @classmethod
    def validate_s3_key(cls, value):
        if value.startswith("/") or ".." in PurePosixPath(value).parts:
            raise ValueError("s3_key must be a relative S3 object key")
        return value


class PostProcessorMessageModel(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    tenant_id: int = Field(gt=0)
    academic_year_id: int = Field(gt=0)
    section_id: int = Field(gt=0)
    course_id: int = Field(gt=0)
    content_file_id: int = Field(gt=0)
    content_extraction_id: int | None = Field(default=None, gt=0)
    s3_key: str | None = Field(default=None, min_length=1)
    bucket_name: str | None = Field(default=None, min_length=1, pattern=r"^\S+$")

    @field_validator("s3_key")
    @classmethod
    def validate_s3_key(cls, value):
        if value is None:
            return value
        if value.startswith("/") or ".." in PurePosixPath(value).parts:
            raise ValueError("s3_key must be a relative S3 object key")
        return value


class TextProcessManager(EdbyteAIBaseModule):

    def __init__(self, is_lambda):
        super().__init__(is_lambda)
        self._text_extraction_queue_url = None
        self._syllabus_analysis_queue_url = None

    def get_user_details(self, bearer_token):
        pass

    def check_mandatory_params(self, params) -> None:
        pass

    def define_api_config(self):
        self.api_function_map = {}

    def define_sqs_config(self, params):
        self.sqs_function_map = {
            START_TEXT_PRE_PROCESSOR_QUEUE_NAME: self._process_start_text_pre_processor_record,
            START_TEXT_POST_PROCESS_QUEUE_NAME: self._process_start_text_post_process_record,
        }
        return self._dispatch_sqs_event(params)

    def process_sqs_event(self, event, context):
        self.AWS_CONTEXT_OBJ = context
        self.get_aws_session()
        connection_secret = self.get_connection_secrete()
        self._create_connection_obj(connection_secret)

        try:
            return self.define_sqs_config(event)
        finally:
            if self.conn and not self.conn.closed:
                self.conn.close()
            self.conn = None

    def _dispatch_sqs_event(self, event):
        batch_item_failures = []
        processed_records = []

        for record in event.get("Records", []):
            message_id = record.get("messageId")
            queue_name = self._get_sqs_queue_name(record)
            try:
                handler = self.sqs_function_map.get(queue_name)
                if not callable(handler):
                    raise UnknownSqsTriggerException(f"No SQS handler configured for queue {queue_name}")

                temp_dir_prefix = self._build_event_temp_dir_prefix(queue_name, message_id)
                with self._event_temp_dir(prefix=temp_dir_prefix) as temp_dir:
                    result = handler(record, temp_dir=temp_dir)
                processed_records.append({
                    "message_id": message_id,
                    "queue_name": queue_name,
                    **result,
                })
            except Exception as exc:
                self.log_error("Failed to process SQS message", {
                    "message_id": message_id,
                    "queue_name": queue_name,
                    "error": str(exc),
                })
                if message_id:
                    batch_item_failures.append({"itemIdentifier": message_id})

        return {
            "batchItemFailures": batch_item_failures,
            "processed": processed_records,
        }

    def _process_start_text_pre_processor_record(self, record, temp_dir=None):
        message = self._load_sqs_body(record, PreProcessorMessageModel)
        return self.start_pre_processor(**message.model_dump(), temp_dir=temp_dir)

    def _process_start_text_post_process_record(self, record, temp_dir=None):
        message = self._load_sqs_body(record, PostProcessorMessageModel)
        return self.start_text_post_process(**message.model_dump(), temp_dir=temp_dir)

    @staticmethod
    def _get_sqs_queue_name(record):
        event_source_arn = record.get("eventSourceARN") or record.get("eventSourceArn")
        if event_source_arn:
            return event_source_arn.rsplit(":", 1)[-1]

        event_source = record.get("eventSource") or record.get("EventSource")
        if event_source:
            return event_source.rsplit(":", 1)[-1]

        return None

    @staticmethod
    def _load_sqs_body(record, model_cls):
        body = record.get("body", "{}")
        if isinstance(body, str):
            body = json.loads(body)
        return model_cls(**body)

    def _build_event_temp_dir_prefix(self, queue_name, message_id):
        queue_part = self._safe_local_name(queue_name, fallback="unknown-queue")
        message_part = self._safe_local_name(message_id, fallback="message")
        return f"text-processor-{queue_part}-{message_part}-"

    @contextmanager
    def _event_temp_dir(self, temp_dir=None, prefix="text-processor-event-"):
        if temp_dir:
            os.makedirs(temp_dir, exist_ok=True)
            yield temp_dir
            return

        temp_root = os.getenv(EVENT_TEMP_ROOT_ENV_NAME)
        if temp_root:
            os.makedirs(temp_root, exist_ok=True)

        with TemporaryDirectory(prefix=prefix, dir=temp_root) as created_temp_dir:
            yield created_temp_dir

    @staticmethod
    def _safe_local_name(value, fallback):
        value = str(value or "").strip()
        safe_value = "".join(
            char if char.isalnum() or char in {"-", "_", "."} else "_"
            for char in value
        ).strip("._")
        return safe_value or fallback

    def _download_s3_object_to_event_dir(self, bucket_name, s3_key, temp_dir, local_file_name=None):
        os.makedirs(temp_dir, exist_ok=True)
        file_name = local_file_name or self._safe_local_name(
            PurePosixPath(s3_key).name,
            fallback="s3-object",
        )
        local_file_path = os.path.join(temp_dir, file_name)
        s3_client = self.aws_session.client("s3")
        s3_client.download_file(bucket_name, s3_key, local_file_path)
        return local_file_path

    def start_pre_processor(
        self,
        tenant_id,
        academic_year_id,
        section_id,
        course_id,
        content_file_id,
        s3_key,
        bucket_name,
        temp_dir=None,
    ):
        with self._event_temp_dir(temp_dir, prefix="text-pre-processor-") as event_temp_dir:
            self._validate_s3_path(
                tenant_id=tenant_id,
                academic_year_id=academic_year_id,
                section_id=section_id,
                course_id=course_id,
                s3_key=s3_key,
            )

            content_file = self._ensure_content_file(
                tenant_id=tenant_id,
                academic_year_id=academic_year_id,
                section_id=section_id,
                course_id=course_id,
                content_file_id=content_file_id,
                s3_key=s3_key,
            )
            file_kind = self._get_supported_file_kind(s3_key=s3_key, file_type=content_file.get("file_type"))
            page_count = self._get_page_count(
                file_kind=file_kind,
                s3_key=s3_key,
                bucket_name=bucket_name,
                temp_dir=event_temp_dir,
            )
            page_chunk_size = self._get_page_chunk_size()
            chunks = self._build_page_chunks(page_count=page_count, page_chunk_size=page_chunk_size)

            previous_autocommit = self.conn.autocommit
            self.conn.autocommit = False
            try:
                extraction_rows = self._create_content_extractions(
                    content_file_id=content_file_id,
                    chunks=chunks,
                )
                self.update_record(
                    table_name=CONTENT_TABLE_NAME,
                    data={
                        "id": content_file_id,
                        "number_pages": page_count,
                        "number_of_extraction_chunk": len(extraction_rows),
                        "status": "pre_processing_completed",
                        "error": self.RESET_STRING,
                    },
                    key_list=["id"],
                    mandatory_column_list=[
                        "number_pages",
                        "number_of_extraction_chunk",
                        "status",
                    ],
                    non_mandatory_column_list=["error"],
                )
                self._publish_text_extraction_messages(
                    tenant_id=tenant_id,
                    academic_year_id=academic_year_id,
                    section_id=section_id,
                    course_id=course_id,
                    content_file_id=content_file_id,
                    s3_key=s3_key,
                    extraction_rows=extraction_rows,
                )
                self.conn.commit()
            except Exception:
                self.conn.rollback()
                raise
            finally:
                self.conn.autocommit = previous_autocommit

        return {
            "success": True,
            "message": "Content pre-processing completed",
            "content_file_id": content_file_id,
            "number_pages": page_count,
            "chunk_size": page_chunk_size,
            "chunks_created": len(extraction_rows),
            "sqs_events_raised": len(extraction_rows),
            "content_extraction_ids": [row["id"] for row in extraction_rows],
        }

    def start_text_post_process(
        self,
        tenant_id,
        academic_year_id,
        section_id,
        course_id,
        content_file_id,
        content_extraction_id=None,
        s3_key=None,
        bucket_name=None,
        temp_dir=None,
    ):
        with self._event_temp_dir(temp_dir, prefix="text-post-processor-") as event_temp_dir:
            bucket_name = self._get_content_bucket_name(bucket_name)

            previous_autocommit = self.conn.autocommit
            self.conn.autocommit = False
            try:
                content_file = self._lock_content_file_for_post_process(
                    tenant_id=tenant_id,
                    academic_year_id=academic_year_id,
                    section_id=section_id,
                    course_id=course_id,
                    content_file_id=content_file_id,
                )
                extraction_rows = self._lock_content_extraction_rows_for_post_process(
                    content_file_id=content_file_id,
                )
                self._ensure_all_extractions_processed(extraction_rows)

                text_file_s3_key = self._build_merged_text_output_key(
                    tenant_id=tenant_id,
                    academic_year_id=academic_year_id,
                    section_id=section_id,
                    course_id=course_id,
                    content_file_id=content_file_id,
                    content_file=content_file,
                    fallback_s3_key=s3_key,
                )

                already_processed = self._is_content_post_processed(content_file, text_file_s3_key)
                syllabus_analysis_events_raised = 0

                if not already_processed:
                    merged_text = self._merge_extracted_chunk_text(
                        bucket_name=bucket_name,
                        extraction_rows=extraction_rows,
                        temp_dir=event_temp_dir,
                    )
                    self._upload_merged_text_file(
                        bucket_name=bucket_name,
                        text_file_s3_key=text_file_s3_key,
                        merged_text=merged_text,
                    )
                    self._update_content_text_file(
                        tenant_id=tenant_id,
                        academic_year_id=academic_year_id,
                        section_id=section_id,
                        course_id=course_id,
                        content_file_id=content_file_id,
                        text_file_s3_key=text_file_s3_key,
                    )
                    syllabus_analysis_events_raised = self._publish_syllabus_analysis_messages(
                        tenant_id=tenant_id,
                        academic_year_id=academic_year_id,
                        section_id=section_id,
                        course_id=course_id,
                        content_file_id=content_file_id,
                        extraction_rows=extraction_rows,
                    )

                self.conn.commit()
            except Exception:
                self.conn.rollback()
                raise
            finally:
                self.conn.autocommit = previous_autocommit

        return {
            "success": True,
            "message": "Text post-processing already completed" if already_processed else "Text post-processing completed",
            "content_file_id": content_file_id,
            "trigger_content_extraction_id": content_extraction_id,
            "text_file_s3_key": text_file_s3_key,
            "chunks_merged": len(extraction_rows),
            "syllabus_analysis_events_raised": syllabus_analysis_events_raised,
            "already_processed": already_processed,
        }

    def _ensure_content_file(self, tenant_id, academic_year_id, section_id, course_id, content_file_id, s3_key):
        rows = self.execute_query(
            sql_stmt=sql_get_content_file_for_preprocess,
            parameters={
                "tenant_id": tenant_id,
                "academic_year_id": academic_year_id,
                "section_id": section_id,
                "course_id": course_id,
                "content_file_id": content_file_id,
            },
        )
        if not rows:
            raise ContentFileMismatchException("content_file_id does not match tenant, academic year, section, and course")

        content_file = rows[0]
        if content_file.get("uploaded_by_role") != "teacher":
            raise ContentFileMismatchException("Only teacher-uploaded content can be pre-processed")
        self._validate_content_file_url(content_file=content_file, s3_key=s3_key)
        return content_file

    def _lock_content_file_for_post_process(self, tenant_id, academic_year_id, section_id, course_id, content_file_id):
        rows = self.execute_query(
            sql_stmt=sql_lock_content_file_for_post_process,
            parameters={
                "tenant_id": tenant_id,
                "academic_year_id": academic_year_id,
                "section_id": section_id,
                "course_id": course_id,
                "content_file_id": content_file_id,
            },
        )
        if not rows:
            raise ContentFileMismatchException("content_file_id does not match tenant, academic year, section, and course")

        content_file = rows[0]
        if content_file.get("uploaded_by_role") != "teacher":
            raise ContentFileMismatchException("Only teacher-uploaded content can be post-processed")
        return content_file

    def _lock_content_extraction_rows_for_post_process(self, content_file_id):
        rows = self.execute_query(
            sql_stmt=sql_lock_content_extractions_for_post_process,
            parameters={"content_file_id": content_file_id},
        )
        if not rows:
            raise ContentExtractionIncompleteException("No content_extraction rows found for content_file_id")
        return rows

    @staticmethod
    def _ensure_all_extractions_processed(extraction_rows):
        pending_ids = []
        missing_output_ids = []
        failed_rows = []

        for row in extraction_rows:
            if row.get("error"):
                failed_rows.append({
                    "content_extraction_id": row.get("id"),
                    "error": row.get("error"),
                })
            if not row.get("is_processed"):
                pending_ids.append(row.get("id"))
            if row.get("is_processed") and not row.get("extracted_chunk_s3_file_url"):
                missing_output_ids.append(row.get("id"))

        if pending_ids or missing_output_ids or failed_rows:
            raise ContentExtractionIncompleteException({
                "message": "All content_extraction rows must be processed successfully before post-processing",
                "pending_content_extraction_ids": pending_ids,
                "missing_output_content_extraction_ids": missing_output_ids,
                "failed_content_extractions": failed_rows,
            })

    @staticmethod
    def _build_merged_text_output_key(
        tenant_id,
        academic_year_id,
        section_id,
        course_id,
        content_file_id,
        content_file,
        fallback_s3_key=None,
    ):
        source_name = (
            content_file.get("file_name")
            or PurePosixPath(unquote(fallback_s3_key or "")).name
            or PurePosixPath(unquote(content_file.get("file_url") or "")).name
            or f"content_file_{content_file_id}"
        )
        text_file_name = f"{PurePosixPath(source_name).stem}.txt"
        return (
            f"academic_year_{academic_year_id}/tenant_id_{tenant_id}/"
            f"section_id_{section_id}/course_id_{course_id}/indexing_content/"
            f"textbooks/extracted/{text_file_name}"
        )

    @staticmethod
    def _is_content_post_processed(content_file, text_file_s3_key):
        file_url = content_file.get("file_url") or ""
        return text_file_s3_key in file_url

    def _merge_extracted_chunk_text(self, bucket_name, extraction_rows, temp_dir):
        chunks = []
        for row in extraction_rows:
            s3_key = row.get("extracted_chunk_s3_file_url")
            if not s3_key:
                raise ContentExtractionIncompleteException(
                    f"content_extraction_id {row.get('id')} does not have extracted_chunk_s3_file_url"
                )
            local_file_name = self._safe_local_name(
                f"{row.get('id')}-{PurePosixPath(s3_key).name}",
                fallback=f"{row.get('id') or 'chunk'}.txt",
            )
            chunks.append(
                self._read_text_from_s3(
                    bucket_name=bucket_name,
                    s3_key=s3_key,
                    temp_dir=temp_dir,
                    local_file_name=local_file_name,
                ).rstrip("\n")
            )
        return "\n\n".join(chunks).strip() + "\n"

    def _read_text_from_s3(self, bucket_name, s3_key, temp_dir, local_file_name=None):
        local_file_path = self._download_s3_object_to_event_dir(
            bucket_name=bucket_name,
            s3_key=s3_key,
            temp_dir=temp_dir,
            local_file_name=local_file_name,
        )
        with open(local_file_path, "rb") as file_obj:
            raw_body = file_obj.read()
        if isinstance(raw_body, bytes):
            return raw_body.decode("utf-8")
        if isinstance(raw_body, str):
            return raw_body
        raise ContentPostProcessException(f"Unable to read text from S3 key {s3_key}")

    def _upload_merged_text_file(self, bucket_name, text_file_s3_key, merged_text):
        s3_client = self.aws_session.client("s3")
        s3_client.put_object(
            Bucket=bucket_name,
            Key=text_file_s3_key,
            Body=merged_text.encode("utf-8"),
            ContentType="text/plain; charset=utf-8",
        )

    def _update_content_text_file(self, tenant_id, academic_year_id, section_id, course_id, content_file_id, text_file_s3_key):
        self.execute_query(
            sql_stmt=sql_update_content_text_file_for_post_process,
            parameters={
                "tenant_id": tenant_id,
                "academic_year_id": academic_year_id,
                "section_id": section_id,
                "course_id": course_id,
                "content_file_id": content_file_id,
                "text_file_s3_key": text_file_s3_key,
            },
        )

    @staticmethod
    def _validate_content_file_url(content_file, s3_key):
        if not s3_key:
            return

        file_url = content_file.get("file_url")
        if file_url and s3_key not in unquote(file_url):
            raise ContentFileMismatchException("s3_key does not match the stored content file_url")

    @staticmethod
    def _validate_s3_path(tenant_id, academic_year_id, section_id, course_id, s3_key):
        expected_prefix = (
            f"academic_year_{academic_year_id}/tenant_id_{tenant_id}/"
            f"section_id_{section_id}/course_id_{course_id}/indexing_content/"
        )
        if not s3_key.startswith(expected_prefix):
            raise InvalidContentPathException(
                f"s3_key must start with {expected_prefix}"
            )

        rest = s3_key[len(expected_prefix):]
        allowed_folders = ("question_papers/", "textbooks/")
        if not any(rest.startswith(folder) for folder in allowed_folders):
            raise InvalidContentPathException(
                "indexing_content path must continue with question_papers/ or textbooks/"
            )
        if rest in allowed_folders:
            raise InvalidContentPathException("s3_key must include a file name")

    @staticmethod
    def _get_supported_file_kind(s3_key, file_type):
        suffix = PurePosixPath(s3_key).suffix.lower()
        normalized_file_type = (file_type or "").lower()

        if suffix in PDF_EXTENSIONS or normalized_file_type == "application/pdf":
            return "pdf"
        if suffix in IMAGE_EXTENSIONS or normalized_file_type in IMAGE_CONTENT_TYPES:
            return "image"

        raise InvalidFileTypeException("Only PDF and image files are supported")

    def _get_page_count(self, file_kind, s3_key, bucket_name, temp_dir):
        if file_kind == "image":
            return 1

        try:
            from pypdf import PdfReader
        except ModuleNotFoundError as exc:
            raise PdfPageCountException("pypdf package is required to count PDF pages") from exc

        try:
            local_file_path = self._download_s3_object_to_event_dir(
                bucket_name=bucket_name,
                s3_key=s3_key,
                temp_dir=temp_dir,
            )
            with open(local_file_path, "rb") as pdf_file:
                page_count = len(PdfReader(pdf_file).pages)
        except Exception as exc:
            raise PdfPageCountException("Unable to read PDF page count") from exc

        if page_count <= 0:
            raise PdfPageCountException("PDF has no pages")
        return page_count

    @staticmethod
    def _get_page_chunk_size():
        try:
            page_chunk_size = int(os.getenv("PDF_PAGE_CHUNK_SIZE", "50"))
        except ValueError as exc:
            raise MissingConfigurationException("PDF_PAGE_CHUNK_SIZE must be an integer") from exc

        if page_chunk_size <= 0:
            raise MissingConfigurationException("PDF_PAGE_CHUNK_SIZE must be greater than zero")
        return page_chunk_size

    @staticmethod
    def _get_content_bucket_name(bucket_name=None):
        configured_bucket_name = (
            bucket_name
            or os.getenv("CONTENT_MATERIALS_BUCKET")
            or os.getenv("TEXT_PROCESSOR_CONTENT_BUCKET")
            or os.getenv("TEXT_EXTRACTOR_CONTENT_BUCKET")
        )
        if not configured_bucket_name:
            raise MissingConfigurationException("CONTENT_MATERIALS_BUCKET is not configured")
        return configured_bucket_name

    @staticmethod
    def _build_page_chunks(page_count, page_chunk_size):
        total_chunks = max(1, math.ceil(page_count / page_chunk_size))
        chunks = []
        for chunk_number in range(1, total_chunks + 1):
            from_page = ((chunk_number - 1) * page_chunk_size) + 1
            to_page = min(chunk_number * page_chunk_size, page_count)
            chunks.append({
                "chunk_number": chunk_number,
                "from_page": from_page,
                "to_page": to_page,
            })
        return chunks

    def _create_content_extractions(self, content_file_id, chunks):
        extraction_rows = []
        for chunk in chunks:
            row_data = {
                "content_file_id": content_file_id,
                "chunk_number": chunk["chunk_number"],
                "from_page": chunk["from_page"],
                "to_page": chunk["to_page"],
                "is_processed": False,
                "is_deleted": False,
            }
            content_extraction_id = self.insert_record(
                table_name=CONTENT_EXTRACTION_TABLE_NAME,
                data=row_data,
                mandatory_column_list=[
                    "content_file_id",
                    "chunk_number",
                    "from_page",
                    "to_page",
                ],
                non_mandatory_column_list=[
                    "is_processed",
                    "is_deleted",
                    "error",
                ],
            )
            extraction_rows.append({
                "id": content_extraction_id,
                **row_data,
            })
        return extraction_rows

    def _publish_text_extraction_messages(
        self,
        tenant_id,
        academic_year_id,
        section_id,
        course_id,
        content_file_id,
        s3_key,
        extraction_rows,
    ):
        queue_url = self._get_text_extraction_queue_url()
        sqs_client = self.aws_session.client("sqs")

        for row in extraction_rows:
            message_body = {
                "tenant_id": tenant_id,
                "academic_year_id": academic_year_id,
                "section_id": section_id,
                "course_id": course_id,
                "content_file_id": content_file_id,
                "content_extraction_id": row["id"],
                "s3_key": s3_key,
            }
            response = sqs_client.send_message(
                QueueUrl=queue_url,
                MessageBody=json.dumps(message_body),
            )
            if not response.get("MessageId"):
                raise SqsMessagePublishException("Failed to publish start-text-extraction message")

    def _publish_syllabus_analysis_messages(
        self,
        tenant_id,
        academic_year_id,
        section_id,
        course_id,
        content_file_id,
        extraction_rows,
    ):
        queue_url = self._get_syllabus_analysis_queue_url()
        sqs_client = self.aws_session.client("sqs")
        events_raised = 0

        for row in extraction_rows:
            message_body = {
                "tenant_id": tenant_id,
                "academic_year_id": academic_year_id,
                "section_id": section_id,
                "course_id": course_id,
                "content_file_id": content_file_id,
                "content_extraction_id": row["id"],
                "s3_key": row["extracted_chunk_s3_file_url"],
            }
            response = sqs_client.send_message(
                QueueUrl=queue_url,
                MessageBody=json.dumps(message_body),
            )
            if not response.get("MessageId"):
                raise SqsMessagePublishException("Failed to publish start-syallabus-analysis message")
            events_raised += 1

        return events_raised

    def _get_text_extraction_queue_url(self):
        if self._text_extraction_queue_url:
            return self._text_extraction_queue_url

        queue_url = os.getenv("START_TEXT_EXTRACTION_QUEUE_URL")
        if queue_url:
            self._text_extraction_queue_url = queue_url
            return queue_url

        queue_name = os.getenv("START_TEXT_EXTRACTION_QUEUE_NAME", "start-text-extraction")
        sqs_client = self.aws_session.client("sqs")
        response = sqs_client.get_queue_url(QueueName=queue_name)
        self._text_extraction_queue_url = response["QueueUrl"]
        return self._text_extraction_queue_url

    def _get_syllabus_analysis_queue_url(self):
        if self._syllabus_analysis_queue_url:
            return self._syllabus_analysis_queue_url

        queue_url = (
            os.getenv("START_SYLLABUS_ANALYSIS_QUEUE_URL")
            or os.getenv("START_SYALLABUS_ANALYSIS_QUEUE_URL")
        )
        if queue_url:
            self._syllabus_analysis_queue_url = queue_url
            return queue_url

        queue_name = (
            os.getenv("START_SYLLABUS_ANALYSIS_QUEUE_NAME")
            or os.getenv("START_SYALLABUS_ANALYSIS_QUEUE_NAME")
            or START_SYLLABUS_ANALYSIS_QUEUE_NAME
        )
        sqs_client = self.aws_session.client("sqs")
        response = sqs_client.get_queue_url(QueueName=queue_name)
        self._syllabus_analysis_queue_url = response["QueueUrl"]
        return self._syllabus_analysis_queue_url
