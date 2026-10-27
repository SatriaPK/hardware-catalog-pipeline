"""One CLI for setup, bounded ingestion, recovery and counts."""

import argparse
import json
from pathlib import Path

from botocore.exceptions import ClientError

from marketplace_pipeline.storage.acquisition import BucketSetupError


def positive(value: str) -> int:
    number = int(value)
    if number <= 0:
        raise argparse.ArgumentTypeError("Must be positive")
    return number


def parser():
    root = argparse.ArgumentParser(prog="marketpipe")
    commands = root.add_subparsers(dest="command", required=True)
    for name in ("init", "pilot", "acquire", "resume", "status", "collect"):
        command = commands.add_parser(name)
        command.add_argument("--namespace", default="mp_acq_dataset")
        if name != "init":
            command.add_argument("--run-id", required=name != "status")
        if name in ("pilot", "acquire", "resume"):
            command.add_argument("--source", choices=("ebay", "rakuten"), required=True)
        if name in ("pilot", "acquire", "resume", "collect"):
            command.add_argument(
                "--max-requests",
                type=positive,
                required=True,
                help="HTTP attempts including OAuth and retries",
            )
            command.add_argument(
                "--interval",
                type=float,
                default=2.0,
                help="Minimum seconds between HTTP attempts (>=1)",
            )
        if name == "collect":
            command.add_argument("--target", type=positive, default=100000)
        if name in ("pilot", "acquire", "collect"):
            command.add_argument(
                "--search-plan", type=Path, default=Path("config/acquisition-plan.yaml")
            )
            command.add_argument(
                "--retention-days",
                type=positive,
                help="Optional actual provider retention limit; no automatic deletion",
            )
    return root


def main(argv=None):
    args = parser().parse_args(argv)
    try:
        from marketplace_pipeline.runtime import execute

        result = execute(args)
        print(json.dumps(result, indent=2, ensure_ascii=True))
        return 1 if result.get("status") == "failed" else 0
    except BucketSetupError as error:
        print(
            json.dumps(
                {
                    "status": "failed",
                    "error": "BucketSetupError",
                    "bucket": error.bucket,
                    "reason": error.reason,
                }
            )
        )
        return 1
    except ClientError as error:
        # Export only known S3 identifiers, never provider Message/headers/URLs.
        known_codes = {
            "AccessDenied",
            "InvalidAccessKeyId",
            "SignatureDoesNotMatch",
            "ExpiredToken",
            "NoSuchBucket",
            "NoSuchKey",
            "NoSuchTagSet",
            "NotFound",
            "404",
            "403",
            "400",
            "PreconditionFailed",
            "412",
            "InvalidRequest",
            "InvalidArgument",
            "NotImplemented",
            "XMinioStorageFull",
            "XMinioAdminBucketQuotaExceeded",
            "SlowDown",
            "InternalError",
            "ServiceUnavailable",
            "RequestTimeTooSkewed",
        }
        known_operations = {
            "HeadBucket",
            "GetBucketTagging",
            "CreateBucket",
            "PutBucketTagging",
            "HeadObject",
            "GetObject",
            "PutObject",
            "ListObjectsV2",
            "ListObjectVersions",
        }
        code = error.response.get("Error", {}).get("Code")
        operation = error.operation_name
        status = error.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        print(
            json.dumps(
                {
                    "status": "failed",
                    "error": "ClientError",
                    "service": "MinIO/S3",
                    "command": args.command,
                    "operation": operation
                    if operation in known_operations
                    else "unrecognized_operation",
                    "storage_error_code": code if code in known_codes else "unrecognized_code",
                    "http_status": status if type(status) is int and 100 <= status <= 599 else None,
                    "message": "Storage failed; credentials and provider message omitted.",
                }
            )
        )
        return 1
    except Exception as error:
        # HTTP/driver exceptions may contain secrets. Never print exception text.
        print(
            json.dumps(
                {
                    "status": "failed",
                    "error": type(error).__name__,
                    "message": "Check configuration, storage and run ID; details redacted.",
                }
            )
        )
        return 1
