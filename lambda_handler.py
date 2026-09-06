"""
    File            : lambda_handler.py
    Package         :
    Description     :
    Project Name    : edbyteai_base_module
    Created by Lakshmipathi R on 18-04-2026
    Copyright (c) 2026 EdbyteAI. All rights reserved.
"""
import json
import traceback
import jwt

from pydantic import ValidationError

from edbyteai_base_module.error_handler import UserSessionOut, UserLoggedOut, UserNOtAuthorised, APINotMapped, \
    APIConfigEmpty, \
    EmptyBearerToken, MissingBearerToken, RateLimitExceeded, MissingParameter, ParamsValidateError, EdbyteAIException
try:
    from edbyteai_base_module.error_handler.validator_custom_messages import CUSTOM_MESSAGES
except ModuleNotFoundError:
    CUSTOM_MESSAGES = {}
from text_processor_manager import TextProcessManager as ManageClass


def success_response(data: dict, status_code: int = 200) -> dict:
  return {
    "statusCode": status_code,
    "headers": {
      "Content-Type": "application/json",
      "Access-Control-Allow-Origin": "*",
    },
    "body": json.dumps(data)
  }


def error_response(status_code: int, message: dict) -> dict:
  return {
    "statusCode": status_code,
    "headers": {
      "Content-Type": "application/json",
      "Access-Control-Allow-Origin": "*",
    },
    "body": json.dumps(message)
  }

def parse_response_body(response):
    body = response.get("body")
    return body if isinstance(body, dict) else json.loads(body)

def format_pydantic_errors(error):
    formatted = []

    for err in error.errors():
        field = ".".join(map(str, err["loc"]))
        msg = CUSTOM_MESSAGES.get(err["type"], err["msg"])

        formatted.append({
            "field": field,
            "message": msg
        })

    return formatted

def lambda_handler(event, context):

    event_type = _detect_source(event)
    path = event.get("path", None)
    method = event.get("httpMethod", None)
    is_lambda = event.get("is_lambda", True)
    if event_type == "API":
        if method == "GET":
            query_params = event.get("queryStringParameters") or {}

            if not isinstance(query_params, dict):
                raise ValueError("queryStringParameters must be a dictionary")

            param = parse_query_params(query_params)
        else:
            param = json.loads(event.get("body", "{}"))
    else:
        param = event

    auth_bearer_token = event.get("headers", {}).get("Authorization", None)

    if not (path, method):
        raise Exception("path or method not defined")

    obj = ManageClass(is_lambda = is_lambda)
    try:

      response = obj(token=auth_bearer_token,
                     path=path,
                     method=method,
                     request_type=event_type,
                     params=param,
                     context=context)
      return success_response(data=response,status_code=200)

    except (jwt.ExpiredSignatureError,jwt.MissingRequiredClaimError,jwt.InvalidTokenError, EmptyBearerToken, MissingBearerToken) as msg:
        return error_response(
            422,
            {
                "success": False,
                "message": "Invalid token, please login back",
                "details": str(msg)
            }
        )
    except (UserSessionOut, UserLoggedOut, UserNOtAuthorised) as msg:
        return error_response(
            401,
            {
                "success": False,
                "message": "User logged out, please login back",
                "details": str(msg)
            }
        )

    except (APINotMapped,APIConfigEmpty) as msg:
        return error_response(
            503,
            {
                "success": False,
                "message": "API mapping issue, Please contact tech support",
                "details": str(msg)
            }
        )

    except ValidationError as msg:
        return error_response(
            401,
            {
                "success": False,
                "message": "Invalid request payload",
                "details": format_pydantic_errors(msg)
            }
        )

    except MissingParameter as msg:
        return error_response(
            400,
            {
                "success": False,
                "message": "Invalid request, missing important parameters",
                "details": msg.data
            }
        )

    except ParamsValidateError as msg:
        return error_response(
            400,
            {
                "success": False,
                "message": "Invalid request payload",
                "details": str(msg)
            }
        )

    except RateLimitExceeded as msg:
        return error_response(
            429,
            {
                "success": False,
                "message": "API mapping issue, Please contact tech support",
                "details": str(msg)
            }
        )

    except EdbyteAIException as msg:
        return error_response(
            400,
            {
                "success": False,
                "message": str(msg),
                "details": msg.data
            }
        )

    except Exception as msg:
      traceback.print_exception(msg)
      return error_response(500, {
                "success": False,
                "message": "Technical error, Please contact tech support",
                "details": str(msg)
            })



def _detect_source(event: dict) -> str:

    if event.get("version") == "2.0" and "requestContext" in event:
        return "API"

    if "httpMethod" in event and "requestContext" in event:
        return "API"

    if event.get("source") and "detail-type" in event:
        return "CRON"

    records = event.get("Records") or []
    if records:
        first = records[0]
        event_source = first.get("eventSource") or first.get("EventSource") or ""

        if "sns" in event_source:
            return "SNS"
        if "sqs" in event_source:
            return "SQS"
        if "s3" in event_source:
            return "S3"

    raise Exception("Unknown Event Found")

def parse_query_params(params: dict | None) -> dict:
    """
    Convert query string parameter values to proper Python types.

    Examples:
        "true"  -> True
        "false" -> False
        "10"    -> 10
        "12.5"  -> 12.5
        "null"  -> None
    """

    if not params:
        return {}

    def convert(value):
        if value is None:
            return None

        if isinstance(value, (bool, int, float)):
            return value

        value = value.strip()
        if value.lower() == "true":
            return True
        if value.lower() == "false":
            return False
        if value.lower() in ("null", "none"):
            return None

        try:
            if value.isdigit() or (
                value.startswith("-") and value[1:].isdigit()
            ):
                return int(value)
        except:
            pass
        try:
            return float(value)
        except:
            pass
        return value

    return {
        key: convert(val)
        for key, val in params.items()
    }

