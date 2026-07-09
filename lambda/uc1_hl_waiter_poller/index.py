# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
"""
uc1_hl_waiter_poller/index.py

CloudFormation custom resource — HealthLake datastore active waiter (poller).

Triggered by EventBridge on a rate(2 minutes) schedule. Reads the CFN callback
state from SSM, polls HealthLake DescribeFHIRDatastore, and when the datastore
reaches ACTIVE status:
  1. Discovers the Glue resource link database auto-created by HealthLake.
  2. Extracts the HealthLake service account catalog ID from that resource link.
  3. Sends CFN SUCCESS with DatastoreId, DatastoreEndpoint, HlResourceLinkDbName,
     and HlServiceCatalogId as custom resource outputs.
  4. Disables the EventBridge poller rule.

If the datastore enters FAILED status, sends CFN FAILED immediately.

Resource link discovery strategy
---------------------------------
HealthLake auto-creates a Glue resource link database whose name is NOT
officially documented as deterministic. The poller calls glue:GetDatabases
(paginated) and filters for entries where:
  Database.TargetDatabase.CatalogId != account_id
  (i.e., the DB is a resource link pointing to a foreign catalog)
Among those, it matches the one whose name contains the datastore name pattern
"hcls-adr-uc1-datastore-<RunId>" (case-insensitive prefix match).
The TargetDatabase.CatalogId from that entry is the HlServiceCatalogId.

Environment variables (set by CFN template)
-------------------------------------------
  RUN_ID  — stack run identifier
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

# When True (Stack 2 / scale stack), send CFN SUCCESS as soon as ACTIVE
# without waiting for the Glue resource link — the FHIR import poller handles that.
SKIP_RESOURCE_LINK = os.environ.get("SKIP_RESOURCE_LINK", "false").lower() == "true"

healthlake_client = boto3.client("healthlake", region_name=REGION)
glue_client = boto3.client("glue", region_name=REGION)
events_client = boto3.client("events", region_name=REGION)
ssm_client = boto3.client("ssm", region_name=REGION)
sts_client = boto3.client("sts", region_name=REGION)


def send_cfn_response(callback_state, status, reason="", data=None):
    """Send a CloudFormation custom resource response using the stored callback URL."""
    body = json.dumps({
        "Status": status,
        "Reason": reason,
        "PhysicalResourceId": callback_state.get("PhysicalResourceId", "uc1-hl-waiter"),
        "StackId": callback_state["StackId"],
        "RequestId": callback_state["RequestId"],
        "LogicalResourceId": callback_state["LogicalResourceId"],
        "Data": data or {},
    }).encode("utf-8")

    url = callback_state["ResponseURL"]
    req = urllib.request.Request(
        url,
        data=body,
        headers={"Content-Type": "", "Content-Length": str(len(body))},
        method="PUT",
    )
    with urllib.request.urlopen(req) as resp:  # nosec B310 — URL is CFN pre-signed S3 ResponseURL (AWS-generated, HTTPS, not user input)
        logger.info("CFN response sent: status=%s http=%s", status, resp.status)


def discover_hl_resource_link(account_id, datastore_id):
    """
    Discover the HealthLake Glue resource link database by paginating GetDatabases
    and filtering for cross-account resource links that contain both the RunId
    and the datastore ID in their name.

    This paginated approach works regardless of how the HealthLake datastore was
    named (old hcls_adr_uc1_* prefix or new healthlake_small/large_* prefix).
    The Lambda role is an LF admin, so it has visibility into all databases
    without needing explicit LF grants.

    Returns (db_name, hl_service_catalog_id) or (None, None) if not yet created.
    """
    run_id_norm = RUN_ID.replace("-", "_")
    ds_id_norm  = datastore_id.replace("-", "_")

    logger.info(
        "Searching for HL resource link DB (run_id_fragment=%s, ds_id_fragment=%s)",
        run_id_norm[:12], ds_id_norm[:8],
    )

    try:
        paginator = glue_client.get_paginator("get_databases")
        for page in paginator.paginate(CatalogId=account_id):
            for db in page.get("DatabaseList", []):
                target = db.get("TargetDatabase")
                db_name = db["Name"]
                if (target
                        and target.get("CatalogId")
                        and target["CatalogId"] != account_id
                        and run_id_norm in db_name
                        and ds_id_norm in db_name
                        and "healthlake_view" in db_name):
                    hl_service_catalog_id = target["CatalogId"]
                    logger.info(
                        "Found HL resource link: db=%s → catalog=%s",
                        db_name, hl_service_catalog_id,
                    )
                    return db_name, hl_service_catalog_id
    except Exception as exc:
        logger.warning("GetDatabases pagination failed: %s", exc)
        return None, None

    logger.info(
        "Resource link DB not yet visible (run_id=%s, datastore_id=%s). Retrying next tick.",
        run_id_norm, ds_id_norm,
    )
    return None, None


def handler(event, context):
    """
    Invoked by EventBridge every 2 minutes.
    Reads callback state from SSM, polls HealthLake, and sends CFN response
    when terminal status is reached.
    """
    logger.info("Poller triggered. Event: %s", json.dumps(event))

    # ------------------------------------------------------------------
    # 1. Load callback state from SSM
    # ------------------------------------------------------------------
    try:
        param = ssm_client.get_parameter(Name=SSM_CALLBACK_KEY)
        callback_state = json.loads(param["Parameter"]["Value"])
    except ssm_client.exceptions.ParameterNotFound:
        logger.warning("SSM callback key not found (%s) — poller may have already completed.", SSM_CALLBACK_KEY)
        return
    except Exception as exc:
        logger.error("Failed to load SSM callback state: %s", exc)
        return

    datastore_id = callback_state["DatastoreId"]
    poller_rule_name = callback_state["PollerRuleName"]
    account_id = callback_state.get("AccountId") or sts_client.get_caller_identity()["Account"]

    # ------------------------------------------------------------------
    # 2. Poll HealthLake datastore status
    # ------------------------------------------------------------------
    try:
        response = healthlake_client.describe_fhir_datastore(DatastoreId=datastore_id)
        datastore_properties = response["DatastoreProperties"]
        status = datastore_properties["DatastoreStatus"]
        logger.info("Datastore %s status: %s", datastore_id, status)
    except Exception as exc:
        logger.error("DescribeFHIRDatastore failed: %s", exc)
        return  # Non-fatal: try again on next tick

    # ------------------------------------------------------------------
    # 3. Still creating — nothing to do yet
    # ------------------------------------------------------------------
    if status in ("CREATING", "BOOTSTRAPPING"):
        logger.info("Datastore not yet ACTIVE (%s). Will retry on next schedule.", status)
        return

    # ------------------------------------------------------------------
    # 4. Terminal failure
    # ------------------------------------------------------------------
    if status in ("FAILED", "DELETING", "DELETED"):
        logger.error("Datastore reached terminal failure status: %s", status)
        _disable_rule_and_cleanup(poller_rule_name)
        send_cfn_response(
            callback_state,
            "FAILED",
            reason=f"HealthLake datastore entered status: {status}",
        )
        return

    # ------------------------------------------------------------------
    # 5. ACTIVE — optionally skip resource link (Stack 2 / scale stack)
    # ------------------------------------------------------------------
    if status == "ACTIVE":
        datastore_endpoint = datastore_properties.get("DatastoreEndpoint", "")

        if SKIP_RESOURCE_LINK:
            logger.info("ACTIVE. SKIP_RESOURCE_LINK=true — sending SUCCESS without resource link.")
            _disable_rule_and_cleanup(poller_rule_name)
            send_cfn_response(
                callback_state,
                "SUCCESS",
                data={
                    "DatastoreId":          datastore_id,
                    "DatastoreEndpoint":    datastore_endpoint,
                    "HlResourceLinkDbName": "",
                    "HlServiceCatalogId":   "",
                },
            )
            return
        datastore_endpoint = datastore_properties.get("DatastoreEndpoint", "")

        # Normal path: discover resource link
        hl_resource_link_db_name, hl_service_catalog_id = discover_hl_resource_link(
            account_id, datastore_id
        )

        if not hl_resource_link_db_name:
            # HealthLake may take a short time to create the resource link after
            # the datastore becomes ACTIVE — retry on next tick rather than failing.
            logger.warning(
                "Datastore is ACTIVE but resource link DB not yet visible. Retrying next tick."
            )
            return

        _disable_rule_and_cleanup(poller_rule_name)
        send_cfn_response(
            callback_state,
            "SUCCESS",
            data={
                "DatastoreId": datastore_id,
                "DatastoreEndpoint": datastore_endpoint,
                "HlResourceLinkDbName": hl_resource_link_db_name,
                "HlServiceCatalogId": hl_service_catalog_id,
            },
        )
        logger.info("Sent CFN SUCCESS. DatastoreEndpoint=%s, HlResourceLinkDbName=%s",
                    datastore_endpoint, hl_resource_link_db_name)
        return

    # Unknown status — log and retry
    logger.warning("Unrecognised datastore status '%s'. Will retry next tick.", status)


def _disable_rule_and_cleanup(poller_rule_name):
    """Disable the EventBridge rule and remove the SSM callback state."""
    try:
        events_client.disable_rule(Name=poller_rule_name)
        logger.info("Disabled poller rule: %s", poller_rule_name)
    except Exception as exc:
        logger.warning("Could not disable poller rule (non-fatal): %s", exc)

    try:
        ssm_client.delete_parameter(Name=SSM_CALLBACK_KEY)
        logger.info("Deleted SSM callback key: %s", SSM_CALLBACK_KEY)
    except Exception as exc:
        logger.warning("Could not delete SSM callback key (non-fatal): %s", exc)
