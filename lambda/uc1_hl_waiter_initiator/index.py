# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
"""
uc1_hl_waiter_initiator/index.py

CloudFormation custom resource — HealthLake datastore active waiter (initiator).

Behaviour
---------
Create / Update
  1. Store the CFN response URL and physical resource ID in SSM so the poller
     can send the callback when the datastore reaches ACTIVE status.
  2. Enable the EventBridge scheduled rule (fires every 2 min) that triggers
     the poller Lambda.
  3. Return WITHOUT sending a CFN response — CFN will wait until the poller
     sends SUCCESS or FAILED via the pre-signed response URL.

Delete
  Best-effort: disable the poller rule and delete the SSM callback key, then
  immediately send CFN SUCCESS so the stack deletion is not blocked.

Environment variables (set by CFN template)
-------------------------------------------
  RUN_ID  — stack run identifier used to construct SSM key and rule name
"""

import json
import logging
import os
import urllib.request

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

REGION = os.environ.get("AWS_REGION", "us-east-1")
RUN_ID = os.environ["RUN_ID"]

SSM_CALLBACK_KEY = f"/healthlake-cleanrooms/{RUN_ID}/hl-waiter-callback"

events_client = boto3.client("events", region_name=REGION)
ssm_client = boto3.client("ssm", region_name=REGION)


def send_cfn_response(event, context, status, reason="", data=None, physical_id=None):
    """Send a CloudFormation custom resource response."""
    body = json.dumps({
        "Status": status,
        "Reason": reason,
        "PhysicalResourceId": physical_id or event.get("PhysicalResourceId", "uc1-hl-waiter"),
        "StackId": event["StackId"],
        "RequestId": event["RequestId"],
        "LogicalResourceId": event["LogicalResourceId"],
        "Data": data or {},
    }).encode("utf-8")

    url = event["ResponseURL"]
    req = urllib.request.Request(
        url,
        data=body,
        headers={"Content-Type": "", "Content-Length": str(len(body))},
        method="PUT",
    )
    with urllib.request.urlopen(req) as resp:  # nosec B310 — URL is CFN pre-signed S3 ResponseURL (AWS-generated, HTTPS, not user input)
        logger.info("CFN response sent: %s", resp.status)


def handler(event, context):
    logger.info("Event: %s", json.dumps(event))

    request_type = event["RequestType"]
    props = event.get("ResourceProperties", {})
    datastore_id = props.get("DatastoreId", "")
    poller_rule_name = props.get("PollerRuleName", f"hcls-adr-uc1-hl-waiter-rule-{RUN_ID}")

    # ------------------------------------------------------------------ Delete
    if request_type == "Delete":
        # Disable poller rule (best-effort)
        try:
            events_client.disable_rule(Name=poller_rule_name)
            logger.info("Disabled poller rule: %s", poller_rule_name)
        except Exception as exc:
            logger.warning("Could not disable poller rule (non-fatal): %s", exc)

        # Remove SSM callback state (best-effort)
        try:
            ssm_client.delete_parameter(Name=SSM_CALLBACK_KEY)
            logger.info("Deleted SSM callback key: %s", SSM_CALLBACK_KEY)
        except ssm_client.exceptions.ParameterNotFound:
            pass
        except Exception as exc:
            logger.warning("Could not delete SSM callback key (non-fatal): %s", exc)

        send_cfn_response(event, context, "SUCCESS",
                          physical_id=event.get("PhysicalResourceId", "uc1-hl-waiter"))
        return

    # ---------------------------------------------------- Create / Update
    # Store callback state in SSM so the poller can send the CFN response
    callback_state = {
        "ResponseURL": event["ResponseURL"],
        "StackId": event["StackId"],
        "RequestId": event["RequestId"],
        "LogicalResourceId": event["LogicalResourceId"],
        "PhysicalResourceId": event.get("PhysicalResourceId", "uc1-hl-waiter"),
        "DatastoreId": datastore_id,
        "PollerRuleName": poller_rule_name,
        "AccountId": props.get("AccountId", ""),
        "Region": props.get("Region", REGION),
    }

    ssm_client.put_parameter(
        Name=SSM_CALLBACK_KEY,
        Value=json.dumps(callback_state),
        Type="String",
        Overwrite=True,
    )
    logger.info("Stored callback state in SSM key: %s", SSM_CALLBACK_KEY)

    # Enable the EventBridge rule so the poller starts firing
    events_client.enable_rule(Name=poller_rule_name)
    logger.info("Enabled poller rule: %s", poller_rule_name)

    # Do NOT send a CFN response here — the poller will do it asynchronously.
    logger.info("Initiator complete. Waiting for poller to report ACTIVE status.")
