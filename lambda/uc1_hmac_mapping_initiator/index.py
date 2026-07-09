# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
"""
uc1_hmac_mapping_initiator/index.py

CFN custom resource — HMAC patient token mapping (initiator, async pattern).

Create/Update:
  1. Store CFN callback state + Phase=WAITING in SSM
  2. Enable the EventBridge poller rule
  3. Return WITHOUT sending CFN response (poller sends it asynchronously)

Delete:
  Best-effort cleanup (S3 CSV, Glue table, SSM key, disable rule) then SUCCESS.

Environment variables (set by CFN template)
-------------------------------------------
  RUN_ID                 — stack run identifier
  FEATURE_OUTPUT_BUCKET  — S3 bucket for mapping CSV
  FEATURE_GLUE_DB        — Glue database name
"""

import json
import logging
import os
import urllib.request

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

REGION                = os.environ.get("AWS_REGION", "us-east-1")
RUN_ID                = os.environ["RUN_ID"]
FEATURE_OUTPUT_BUCKET = os.environ["FEATURE_OUTPUT_BUCKET"]
FEATURE_GLUE_DB       = os.environ["FEATURE_GLUE_DB"]

SSM_CALLBACK_KEY   = f"/healthlake-cleanrooms/{RUN_ID}/hmac-mapping-callback"
MAPPING_S3_KEY     = "mapping/patient_token_mapping.csv"
MAPPING_TABLE_NAME = "patient_token_mapping"

events_client = boto3.client("events", region_name=REGION)
ssm_client    = boto3.client("ssm",    region_name=REGION)


def send_cfn_response(event, context, status, reason="", data=None, physical_id=None):
    body = json.dumps({
        "Status": status,
        "Reason": reason,
        "PhysicalResourceId": physical_id or event.get("PhysicalResourceId", f"uc1-hmac-mapping-{RUN_ID}"),
        "StackId": event["StackId"],
        "RequestId": event["RequestId"],
        "LogicalResourceId": event["LogicalResourceId"],
        "Data": data or {},
    }).encode("utf-8")
    req = urllib.request.Request(
        event["ResponseURL"],
        data=body,
        headers={"Content-Type": "", "Content-Length": str(len(body))},
        method="PUT",
    )
    with urllib.request.urlopen(req) as resp:  # nosec B310 — URL is CFN pre-signed S3 ResponseURL (AWS-generated, HTTPS, not user input)
        logger.info("CFN response sent: %s", resp.status)


def delete_mapping():
    """Best-effort cleanup on stack deletion."""
    s3 = boto3.client("s3", region_name=REGION)
    try:
        s3.delete_object(Bucket=FEATURE_OUTPUT_BUCKET, Key=MAPPING_S3_KEY)
        logger.info("Deleted mapping CSV from S3")
    except Exception as exc:
        logger.warning("Could not delete mapping CSV (non-fatal): %s", exc)
    glue = boto3.client("glue", region_name=REGION)
    try:
        acct = boto3.client("sts").get_caller_identity()["Account"]
        glue.delete_table(CatalogId=acct, DatabaseName=FEATURE_GLUE_DB, Name=MAPPING_TABLE_NAME)
        logger.info("Deleted Glue table '%s'", MAPPING_TABLE_NAME)
    except Exception as exc:
        logger.warning("Could not delete Glue table (non-fatal): %s", exc)


def handler(event, context):
    logger.info("Event: %s", json.dumps(event))
    request_type  = event["RequestType"]
    props         = event.get("ResourceProperties", {})
    poller_rule   = props.get("PollerRuleName", f"hcls-adr-uc1-hmac-mapping-rule-{RUN_ID}")
    physical_id   = f"uc1-hmac-mapping-{RUN_ID}"

    # ------------------------------------------------------------------ Delete
    if request_type == "Delete":
        try:
            events_client.disable_rule(Name=poller_rule)
        except Exception as exc:
            logger.warning("Could not disable poller rule (non-fatal): %s", exc)
        try:
            ssm_client.delete_parameter(Name=SSM_CALLBACK_KEY)
        except Exception as exc:
            logger.warning("Could not delete SSM callback (non-fatal): %s", exc)
        delete_mapping()
        send_cfn_response(event, context, "SUCCESS", physical_id=physical_id)
        return

    # ---------------------------------------------------- Create / Update
    callback_state = {
        "ResponseURL":       event["ResponseURL"],
        "StackId":           event["StackId"],
        "RequestId":         event["RequestId"],
        "LogicalResourceId": event["LogicalResourceId"],
        "PhysicalResourceId": event.get("PhysicalResourceId", physical_id),
        "PollerRuleName":    poller_rule,
        "Phase":             "WAITING",
    }
    ssm_client.put_parameter(
        Name=SSM_CALLBACK_KEY,
        Value=json.dumps(callback_state),
        Type="String",
        Overwrite=True,
    )
    logger.info("Stored callback state. Phase=WAITING. Enabling poller rule: %s", poller_rule)
    events_client.enable_rule(Name=poller_rule)
    logger.info("Poller rule enabled. Returning (async — poller will send CFN response).")
    # Do NOT send CFN response — the poller does it asynchronously.
