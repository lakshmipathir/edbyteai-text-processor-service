"""
    File            : test_lambda_handler.py
    Package         :
    Description     : Endpoint test params for exam management APIs.
    Project Name    : edbyteai-base-module
    Created by Lakshmipathi R on 11-06-2026
    Copyright (c) 2026 EdbyteAI. All rights reserved.
"""
import json
import os
import unittest
from copy import deepcopy
from pprint import pprint
from types import SimpleNamespace

from edbyteai_base_module.utilities import Utility

from lambda_handler import lambda_handler

PRINCIPAL_AUTHORISATION = os.getenv(
    "PRINCIPAL_AUTHORISATION") or "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIyIiwidGVuYW50X2lkIjoxLCJqdGkiOiIwMDQ5Mjg4OS04YjM3LTQ0ZDMtYTBiNy05OGM5ZDFmMTQzNWYiLCJzZXNzaW9uX3V1aWQiOiI5MDk3ZmQ1Mi01NjIwLTQwNDctYmZkNi1iNjUwNzYwNTdkMDAiLCJleHAiOjE3ODY4NjAxMzUsImlhdCI6MTc4Njg0OTMzNSwidHlwZSI6ImFjY2VzcyJ9.UO5H3FywDM8CyKAVhFnc5zrUaK2jtH_IjATjmQT_5CU"
CLASS_TEACHER_AUTHORISATION = os.getenv(
    "CLASS_TEACHER_AUTHORISATION") or "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiI0IiwidGVuYW50X2lkIjoxLCJqdGkiOiJmYTA3ZTNiYy1kMjJlLTRkMWUtOGJjYS1kZmU5ZjZmYzUwYjAiLCJzZXNzaW9uX3V1aWQiOiJjZGIxNWU2Yi0zY2YyLTQyMDAtOGY1My04YmEzOTc3YzQ3OWQiLCJleHAiOjE3ODgyODk0NDgsImlhdCI6MTc4ODI3ODY0OCwidHlwZSI6ImFjY2VzcyJ9.XZuycvntBsHMKlVUuImshSViWbfZ1nkhFbOSDvoygCw"

SUBJECT_TEACHER_AUTHORISATION = os.getenv(
    "SUBJECT_TEACHER_AUTHORISATION") or "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIzIiwidGVuYW50X2lkIjoxLCJqdGkiOiI5OGFlNGUxMi05M2YxLTQ0NzItYWMxOC1iNGUwZmQ4NjMwYzMiLCJzZXNzaW9uX3V1aWQiOiIyOGMxMGVjOC02MGU3LTQ2YzgtYjFlZC05NWIzMGY1OTUwYjUiLCJleHAiOjE3ODc5MzUwNzgsImlhdCI6MTc4NzkyNDI3OCwidHlwZSI6ImFjY2VzcyJ9.hGYPvg-Nddl1nAj99XlhlFwv1_FixQPNtpawNH2BLgQ"

STUDENT_AUTHORISATION = os.getenv("STUDENT_AUTHORISATION") or "Bearer <student-access-token>"

COMMON_IDS = {
    "tenant_id": 1,
    "academic_year_id": 1,
    "exam_id": 1,
    "exam_type_id": 1,
    "section_id": 1,
    "course_id": 1,
    "student_id": 190,
}


def build_event(
        path=None,
        method=None,
        token=None,
        query_params=None,
        body=None,
        include_auth=True,
        queue_name="start-pre-processor",
        message_id="test-message-id",
):
    message_body = body if body is not None else query_params or {}
    if not isinstance(message_body, str):
        message_body = json.dumps(message_body)

    return {
        "Records": [
            {
                "messageId": message_id,
                "receiptHandle": "test-receipt-handle",
                "body": message_body,
                "attributes": {
                    "ApproximateReceiveCount": "1",
                    "SentTimestamp": "0",
                    "SenderId": "test-sender",
                    "ApproximateFirstReceiveTimestamp": "0",
                },
                "messageAttributes": {},
                "md5OfBody": "test-md5",
                "eventSource": "aws:sqs",
                "eventSourceARN": f"arn:aws:sqs:ap-south-1:123456789012:{queue_name}",
                "awsRegion": "ap-south-1",
            }
        ],
        "is_lambda": False,
    }


def build_event_from_params(params):
    return build_event(
        path=params.get("path"),
        method=params.get("method"),
        token=params.get("token"),
        query_params=params.get("query_params"),
        body=params.get("body"),
        queue_name=params.get("queue_name", "start-pre-processor"),
        message_id=params.get("message_id", "test-message-id"),
    )


def get_response_body(response):
    body = response.get("body")
    return body if isinstance(body, dict) else json.loads(body)


def print_response(response):
    log_obj = Utility()
    log_obj.log(log_level=1, log_msg="lambda_response", log_obj={
        "statusCode": response.get("statusCode"),
        "body": get_response_body(response),
    })


context = SimpleNamespace(
    function_name="dev",
    function_version="$LATEST",
    invoked_function_arn="arn:aws:lambda:ap-south-1:123456789012:function:local-test-function",
    memory_limit_in_mb="512",
    aws_request_id="dummy-request-id-12345",
    log_group_name="/aws/lambda/local-test-function",
    log_stream_name="local-stream",
    get_remaining_time_in_millis=lambda: 300000,
)


class TestPreProcessorEvent(unittest.TestCase):

    def test_pre_processor_event(self):
        params = {
            "path": "/exam",
            "method": "POST",
            "token": PRINCIPAL_AUTHORISATION,
            "queue_name": "start-text-post-process",
            "body": {
                "tenant_id": 1,
                "academic_year_id": 1,
                "section_id": 1,
                "course_id": 1,
                "content_file_id": 22,
                "bucket_name": "edbyteai-dev-content-materials",
                "s3_key": "academic_year_1/tenant_id_1/section_id_1/course_id_1/indexing_content/textbooks/3f1d2ffb-fb13-45eb-b5ff-bfca477f83a7-10th English Maths Part - 1 2026-27.pdf",
                
            },
        }
        request = build_event_from_params(params)
        response = lambda_handler(request, context)
        pprint(response)

    def test_post_processor_event(self):
        params = {
            "path": "/exam",
            "method": "POST",
            "token": PRINCIPAL_AUTHORISATION,
            "queue_name": "start-text-post-process",
            "body": {
                "tenant_id": 1,
                "academic_year_id": 1,
                "section_id": 1,
                "course_id": 1,
                "content_file_id": 22,
                "bucket_name": "edbyteai-dev-content-materials",
                "s3_key": "academic_year_1/tenant_id_1/section_id_1/course_id_1/indexing_content/textbooks/3f1d2ffb-fb13-45eb-b5ff-bfca477f83a7-10th English Maths Part - 1 2026-27.pdf",

            },
        }
        request = build_event_from_params(params)
        response = lambda_handler(request, context)
        pprint(response)


