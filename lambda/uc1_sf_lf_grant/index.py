# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
"""
uc1_sf_lf_grant/index.py  —  Step Functions task Lambda.

Grants Lake Formation permissions so the pipeline role can:
  1. Query FHIR Iceberg tables via the HealthLake resource link (Athena)
  2. Create/manage tables in the feature Glue database (HMAC mapping, CTAS)

Input (Step Functions payload):
  $.AccountId, $.Region, $.LambdaRoleArn,
  $.ResourceLink.HlResourceLinkDbName,
  $.ResourceLink.HlServiceCatalogId,
  $.FeatureGlueDb
"""

import logging
import os
import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

REGION = os.environ.get("AWS_REGION", "us-east-1")
lf     = boto3.client("lakeformation", region_name=REGION)


def _grant(principal, resource, permissions):
    try:
        lf.grant_permissions(
            Principal={"DataLakePrincipalIdentifier": principal},
            Resource=resource,
            Permissions=permissions,
            PermissionsWithGrantOption=[],
        )
        logger.info("Grant applied: %s", permissions)
    except lf.exceptions.AlreadyExistsException:
        logger.info("Grant already exists: %s", permissions)


def handler(event, context):
    account_id    = event["AccountId"]
    role_arn      = event["LambdaRoleArn"]
    rl_db_name    = event["ResourceLink"]["HlResourceLinkDbName"]
    hl_catalog_id = event["ResourceLink"]["HlServiceCatalogId"]
    feat_glue_db  = event.get("FeatureGlueDb", "")

    logger.info("Granting LF on resource link %s (service catalog %s)",
                rl_db_name, hl_catalog_id)

    # Grant 1: DESCRIBE on the resource link database (customer catalog)
    _grant(role_arn,
           {"Database": {"CatalogId": account_id, "Name": rl_db_name}},
           ["DESCRIBE"])

    # Grant 2: SELECT+DESCRIBE on all tables in HealthLake service catalog
    # (TableWildcard is required for cross-account resource link — per AWS LF docs)
    _grant(role_arn,
           {"Table": {"CatalogId": hl_catalog_id,
                      "DatabaseName": rl_db_name,
                      "TableWildcard": {}}},
           ["SELECT", "DESCRIBE"])

    # Grant 3: CREATE_TABLE, DESCRIBE, DROP on the feature Glue database.
    # Required so uc1_sf_compute_hmac and uc1_sf_ctas_prep can register Glue tables.
    # lakeformation:GrantPermissions requires Resource: "*" in IAM — AWS documented
    # limitation, no resource-level restriction supported for this action.
    # The LF resource itself is fully scoped: CatalogId + Name (no wildcards).
    if feat_glue_db:
        _grant(role_arn,
               {"Database": {"CatalogId": account_id, "Name": feat_glue_db}},
               ["CREATE_TABLE", "DESCRIBE", "DROP"])
        logger.info("CREATE_TABLE/DESCRIBE/DROP grant on feature DB %s applied",
                    feat_glue_db)

    return {"Status": "SUCCESS", "HlResourceLinkDbName": rl_db_name}
