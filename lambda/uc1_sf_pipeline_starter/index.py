# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
"""
uc1_sf_pipeline_starter/index.py  —  CloudFormation custom resource.

Starts the Step Functions data pipeline execution and IMMEDIATELY returns
CFN SUCCESS without waiting for the pipeline to finish.

This decouples the CFN stack lifecycle from the multi-hour data pipeline:
  - CloudFormation CREATE_COMPLETE in ~15 minutes (infra only)
  - Step Functions pipeline runs independently for 5-8 hours

On Create/Update: starts a new Step Functions execution with all pipeline
                  parameters as input. Returns SUCCESS immediately.
On Delete: attempts to stop any running execution (best-effort). Returns SUCCESS.

Environment variables (set by CFN template):
  STATE_MACHINE_ARN  — ARN of the Step Functions state machine to start
  RUN_ID             — stack run identifier

ResourceProperties passed from CFN:
  All pipeline runtime values injected as input to the Step Functions execution.
"""

import json
import logging
import os
import urllib.request

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

REGION            = os.environ.get("AWS_REGION", "us-east-1")
STATE_MACHINE_ARN = os.environ["STATE_MACHINE_ARN"]
RUN_ID            = os.environ["RUN_ID"]

sfn = boto3.client("stepfunctions", region_name=REGION)


def send_cfn_response(event, status, reason="", data=None, physical_id=None):
    body = json.dumps({
        "Status":            status,
        "Reason":            reason,
        "PhysicalResourceId": physical_id or event.get("PhysicalResourceId",
                                                        f"uc1-pipeline-{RUN_ID}"),
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
        logger.info("CFN response sent: %s", resp.status)


def handler(event, context):
    logger.info("RequestType=%s", event["RequestType"])
    props      = event.get("ResourceProperties", {})
    physical_id = f"uc1-pipeline-{RUN_ID}"

    if event["RequestType"] == "Delete":
        # Best-effort: stop any running pipeline execution
        try:
            pages = sfn.get_paginator("list_executions").paginate(
                stateMachineArn=STATE_MACHINE_ARN, statusFilter="RUNNING"
            )
            for page in pages:
                for ex in page.get("executions", []):
                    sfn.stop_execution(
                        executionArn=ex["executionArn"],
                        cause="CloudFormation stack deletion",
                    )
                    logger.info("Stopped execution: %s", ex["executionArn"])
        except Exception as exc:
            logger.warning("Could not stop executions (non-fatal): %s", exc)
        send_cfn_response(event, "SUCCESS", physical_id=physical_id)
        return

    # Create / Update — start the pipeline and return SUCCESS immediately
    try:
        # Build execution input from all ResourceProperties
        execution_input = {k: v for k, v in props.items() if k != "ServiceToken"}
        execution_input["RunId"] = RUN_ID

        resp = sfn.start_execution(
            stateMachineArn=STATE_MACHINE_ARN,
            name=f"pipeline-{RUN_ID}",
            input=json.dumps(execution_input),
        )
        execution_arn = resp["executionArn"]
        logger.info("Pipeline execution started: %s", execution_arn)
        logger.info("Stack will be CREATE_COMPLETE now. Pipeline runs in background.")

        send_cfn_response(
            event, "SUCCESS",
            physical_id=physical_id,
            data={"ExecutionArn": execution_arn},
        )
    except Exception as exc:
        logger.error("Failed to start pipeline: %s", exc, exc_info=True)
        send_cfn_response(event, "FAILED", reason=str(exc), physical_id=physical_id)
