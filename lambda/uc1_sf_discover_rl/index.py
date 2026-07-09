# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
"""
uc1_sf_discover_rl/index.py  —  Step Functions task Lambda.

Discovers the HealthLake Glue resource link database created automatically
when the datastore becomes ACTIVE.  Called by the Step Functions state machine
after FHIR import completes.

Returns:
  {"Found": true,  "HlResourceLinkDbName": "...", "HlServiceCatalogId": "..."}
  {"Found": false, "HlResourceLinkDbName": "",    "HlServiceCatalogId": ""   }

Step Functions retries (via a Choice+Wait loop) until Found == true.

Input (from Step Functions execution context):
  $.RunId           — deployment run identifier
  $.DatastoreId     — HealthLake datastore ID
  $.AccountId       — AWS account ID
"""

import json
import logging
import os

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

REGION = os.environ.get("AWS_REGION", "us-east-1")
glue   = boto3.client("glue", region_name=REGION)


def handler(event, context):
    run_id       = event["RunId"]
    datastore_id = event["DatastoreId"]
    account_id   = event["AccountId"]

    run_id_norm = run_id.replace("-", "_")
    ds_norm     = datastore_id.replace("-", "_")

    logger.info("Searching for HL resource link (run=%s ds=%s)", run_id_norm[:12], ds_norm[:8])

    try:
        paginator = glue.get_paginator("get_databases")
        for page in paginator.paginate(CatalogId=account_id):
            for db in page.get("DatabaseList", []):
                target  = db.get("TargetDatabase")
                db_name = db["Name"]
                if (target
                        and target.get("CatalogId")
                        and target["CatalogId"] != account_id
                        and run_id_norm in db_name
                        and ds_norm in db_name
                        and "healthlake_view" in db_name):
                    logger.info("Found resource link: %s -> %s",
                                db_name, target["CatalogId"])
                    return {
                        "Found":              True,
                        "HlResourceLinkDbName": db_name,
                        "HlServiceCatalogId": target["CatalogId"],
                    }
    except Exception as exc:
        logger.warning("GetDatabases failed (will retry): %s", exc)

    logger.info("Resource link not yet visible.")
    return {"Found": False, "HlResourceLinkDbName": "", "HlServiceCatalogId": ""}
