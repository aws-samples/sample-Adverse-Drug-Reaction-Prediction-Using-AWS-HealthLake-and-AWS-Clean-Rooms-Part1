# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
"""
uc1_bucket_emptier/index.py

CloudFormation custom resource — empties an S3 bucket (including all object
versions and delete markers) before CloudFormation attempts to delete it.

On Create / Update: no-op — returns SUCCESS immediately.
On Delete: pages through all versions and delete markers, deletes them in
           batches of 1000, then returns SUCCESS so CFN can proceed to delete
           the now-empty bucket.

ResourceProperties:
  BucketName  — the S3 bucket to empty on Delete
"""

import json
import logging
import urllib.request
import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")


def send_cfn_response(event, status, reason="", data=None, physical_id=None):
    body = json.dumps({
        "Status": status,
        "Reason": reason,
        "PhysicalResourceId": physical_id or event.get("PhysicalResourceId", "bucket-emptier"),
        "StackId":            event["StackId"],
        "RequestId":          event["RequestId"],
        "LogicalResourceId":  event["LogicalResourceId"],
        "Data":               data or {},
    }).encode("utf-8")
    req = urllib.request.Request(
        event["ResponseURL"], data=body,
        headers={"Content-Type": "", "Content-Length": str(len(body))},
        method="PUT",
    )
    with urllib.request.urlopen(req) as resp:  # nosec B310 — URL is CFN pre-signed S3 ResponseURL (AWS-generated, HTTPS, not user input)
        logger.info("CFN response sent: %s %s", status, resp.status)


def empty_bucket(bucket_name):
    """
    Delete all object versions and delete markers from the bucket.
    Uses Quiet=False so any per-object errors are visible in the response.
    """
    paginator = s3.get_paginator("list_object_versions")
    total     = 0
    errors    = []

    for page in paginator.paginate(Bucket=bucket_name):
        objects = [
            ({"Key": o["Key"], "VersionId": o["VersionId"]}
             if o.get("VersionId") else {"Key": o["Key"]})
            for o in page.get("Versions", []) + page.get("DeleteMarkers", [])
        ]
        if not objects:
            continue

        # Quiet=False: response contains per-object errors; we log and accumulate them.
        resp = s3.delete_objects(
            Bucket=bucket_name,
            Delete={"Objects": objects, "Quiet": False},
        )
        deleted = len(resp.get("Deleted", []))
        total  += deleted

        for err in resp.get("Errors", []):
            msg = f"{err['Key']} (v{err.get('VersionId','?')}): {err['Code']} — {err['Message']}"
            logger.error("Delete error: %s", msg)
            errors.append(msg)

    logger.info("Emptied %s: deleted %d object versions, %d errors.", bucket_name, total, len(errors))

    if errors:
        raise RuntimeError(
            f"Failed to delete {len(errors)} object(s) from {bucket_name}: {errors[:5]}"
        )


def handler(event, context):
    logger.info("Event: %s", json.dumps(event))
    request_type = event["RequestType"]
    bucket_name  = event["ResourceProperties"]["BucketName"]
    physical_id  = f"bucket-emptier-{bucket_name}"

    if request_type in ("Create", "Update"):
        send_cfn_response(event, "SUCCESS", physical_id=physical_id)
        return

    # Delete — empty the bucket so CFN can delete it
    try:
        empty_bucket(bucket_name)
        send_cfn_response(event, "SUCCESS", physical_id=physical_id)
    except s3.exceptions.NoSuchBucket:
        logger.info("Bucket %s does not exist — nothing to empty.", bucket_name)
        send_cfn_response(event, "SUCCESS", physical_id=physical_id)
    except Exception as exc:
        logger.error("Failed to empty bucket %s: %s", bucket_name, exc)
        send_cfn_response(event, "FAILED", reason=str(exc), physical_id=physical_id)
