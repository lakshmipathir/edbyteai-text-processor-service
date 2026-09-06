import json
import math
import os
from io import BytesIO
from pathlib import PurePosixPath
from urllib.parse import unquote

from pydantic import BaseModel, Field, field_validator

from edbyteai_base_module.base import EdbyteAIBaseModule
from custom_exception import (
    ContentFileMismatchException,
    InvalidContentPathException,
    InvalidFileTypeException,
    MissingConfigurationException,
    PdfPageCountException,
    SqsMessagePublishException,
    UnknownSqsTriggerException,
)
from sql_queries import (
    sql_get_content_file_for_preprocess,
)


PDF_EXTENSIONS = {".pdf"}
IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".webp", ".bmp", ".tif", ".tiff"}
IMAGE_CONTENT_TYPES = {"image/jpeg", "image/png", "image/webp", "image/bmp", "image/tiff"}
START_PRE_PROCESSOR_QUEUE_NAME = "start-pre-processor"
CONTENT_TABLE_NAME = "edbyteai.content"
CONTENT_EXTRACTION_TABLE_NAME = "edbyteai.content_extraction"


class PreProcessorMessageModel(BaseModel):
    tenant_id: int
    academic_year_id: int
    section_id: int
    course_id: int
    content_file_id: int
    s3_key: str = Field(min_length=1)

    @field_validator("s3_key")
    @classmethod
    def validate_s3_key(cls, value):
        if value.startswith("/") or ".." in PurePosixPath(value).parts:
            raise ValueError("s3_key must be a relative S3 object key")
        return value


class TextProcessManager(EdbyteAIBaseModule):

    def __init__(self, is_lambda):
        super().__init__(is_lambda)
        self._text_extraction_queue_url = None

    def define_api_config(self):
        self.api_function_map = {}

    def define_sqs_config(self, params):
        self.sqs_function_map = {
            START_PRE_PROCESSOR_QUEUE_NAME: self._process_start_pre_processor_record,
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

                result = handler(record)
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

    def _process_start_pre_processor_record(self, record):
        message = self._load_sqs_body(record)
        return self.start_pre_processor(**message.model_dump())

    @staticmethod
    def _get_sqs_queue_name(record):
        event_source_arn = record.get("eventSourceARN") or record.get("eventSourceArn")
        if event_source_arn:
            return event_source_arn.rsplit(":", 1)[-1]

        event_source = record.get("eventSource") or record.get("EventSource")
        if event_source:
            return event_source.rsplit(":", 1)[-1]

        return None

    def _load_sqs_body(self, record):
        body = record.get("body", "{}")
        if isinstance(body, str):
            body = json.loads(body)
        return PreProcessorMessageModel(**body)

    def start_pre_processor(self, tenant_id, academic_year_id, section_id, course_id, content_file_id, s3_key):
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
        page_count = self._get_page_count(file_kind=file_kind, s3_key=s3_key)
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

    def _get_page_count(self, file_kind, s3_key):
        if file_kind == "image":
            return 1

        bucket_name = self._get_content_bucket_name()
        s3_client = self.aws_session.client("s3")
        response = s3_client.get_object(Bucket=bucket_name, Key=s3_key)
        pdf_bytes = response["Body"].read()

        try:
            from pypdf import PdfReader
        except ModuleNotFoundError as exc:
            raise PdfPageCountException("pypdf package is required to count PDF pages") from exc

        try:
            page_count = len(PdfReader(BytesIO(pdf_bytes)).pages)
        except Exception as exc:
            raise PdfPageCountException("Unable to read PDF page count") from exc

        if page_count <= 0:
            raise PdfPageCountException("PDF has no pages")
        return page_count

    @staticmethod
    def _get_content_bucket_name():
        bucket_name = (
            os.getenv("CONTENT_MATERIALS_BUCKET")
            or os.getenv("CONTENT_BUCKET_NAME")
            or os.getenv("CONTENT_BUCKET")
        )
        if not bucket_name:
            raise MissingConfigurationException("CONTENT_MATERIALS_BUCKET is not configured")
        return bucket_name

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
