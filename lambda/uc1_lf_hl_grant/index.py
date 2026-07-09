# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
"""
uc1_lf_hl_grant/index.py

CloudFormation custom resource — Lake Formation grants on HealthLake resource link.

Two separate grant_permissions calls are required per the LF docs because the
HealthLake Glue integration spans two catalogs (customer account + HL service account):

  Step 1 — DESCRIBE on the resource link database in the customer account.
            This lets the principal "see" the resource link in their catalog.

  Step 2 — SELECT + DESCRIBE on ALL tables in the TARGET database that lives in
            the HealthLake service account, using TableWildcard: {}.
            NOTE: "ALL_TABLES" as a Name literal is INVALID — use TableWildcard.

On Delete: best-effort revoke of both grants (non-fatal — stack deletion must not
be blocked if grants were already removed or the datastore was deleted).

ResourceProperties passed by CFN
---------------------------------
  AccountId           — customer AWS account ID
  Region              — AWS region
  LambdaRoleArn       — ARN of the principal to grant (CustomResourceLambdaRole)
  HlResourceLinkDbName — Glue resource link DB name (from T5 waiter output)
  HlServiceCatalogId  — HealthLake service account catalog ID (from T5 waiter output)
"""

import json
import logging
import urllib.request

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def send_cfn_response(event, context, status, reason="", data=None, physical_id=None):
    body = json.dumps({
        "Status": status,
        "Reason": reason,
        "PhysicalResourceId": physical_id or event.get("PhysicalResourceId", "uc1-lf-hl-grant"),
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
        logger.info("CFN response sent: %s %s", status, resp.status)


def ensure_lf_admin(lf, lambda_role_arn):
    """
    Ensure the Lambda execution role is registered as a Lake Formation data lake
    administrator before attempting grants.

    AWS::LakeFormation::DataLakeSettings with MutationType:APPEND can lose the
    admin entry across stack deletions/re-creates.  The Lambda role has IAM
    permission for lakeformation:PutDataLakeSettings, so we self-register here
    as a safety net.

    NOTE: boto3 uses 'DataLakeAdmins' (not 'Admins' — that is the CFN property name).
    """
    current = lf.get_data_lake_settings()["DataLakeSettings"]
    admins  = current.get("DataLakeAdmins", [])
    arn_entry = {"DataLakePrincipalIdentifier": lambda_role_arn}
    if arn_entry in admins:
        logger.info("Lambda role is already a LF admin.")
        return
    admins.append(arn_entry)
    # Preserve all existing keys; only update DataLakeAdmins
    updated = {**current, "DataLakeAdmins": admins}
    lf.put_data_lake_settings(DataLakeSettings=updated)
    logger.info("Registered Lambda role as LF admin: %s", lambda_role_arn)


def grant_permissions(lf, account_id, lambda_role_arn, hl_resource_link_db_name, hl_service_catalog_id):
    """
    Grant the Lambda role access to query FHIR tables via the HealthLake resource link.

    Only Step 1 is needed from the consumer-account side:
    - DESCRIBE on the resource link database in the CUSTOMER account catalog.
      This allows the query engine to resolve the resource link.

    Step 2 (SELECT+DESCRIBE on the target tables in the HealthLake SERVICE account
    catalog, CatalogId=hl_service_catalog_id) is intentionally omitted.
    HealthLake automatically grants consumer-account access to the underlying
    FHIR Iceberg data when it creates the resource link. Attempting to call
    lf.grant_permissions with a foreign CatalogId results in AccessDeniedException
    because we have no admin rights in the HealthLake service account.
    """

    # Step 1 — DESCRIBE on the resource link database (customer account catalog)
    logger.info(
        "Granting DESCRIBE on resource link DB: catalog=%s db=%s principal=%s",
        account_id, hl_resource_link_db_name, lambda_role_arn,
    )
    lf.grant_permissions(
        Principal={"DataLakePrincipalIdentifier": lambda_role_arn},
        Resource={
            "Database": {
                "CatalogId": account_id,
                "Name": hl_resource_link_db_name,
            }
        },
        Permissions=["DESCRIBE"],
        PermissionsWithGrantOption=[],
    )
    logger.info("Step 1 DESCRIBE grant succeeded.")

    # Step 2 — SELECT + DESCRIBE on the resource link TABLES in the customer
    # account catalog (CatalogId = account_id, NOT the HealthLake service account).
    # HealthLake already granted SELECT on the underlying target tables when it
    # created the resource link. We only need to grant on our side of the link.
    logger.info(
        "Granting SELECT+DESCRIBE on resource link tables: catalog=%s db=%s principal=%s",
        account_id, hl_resource_link_db_name, lambda_role_arn,
    )
    lf.grant_permissions(
        Principal={"DataLakePrincipalIdentifier": lambda_role_arn},
        Resource={
            "Table": {
                "CatalogId": account_id,          # our catalog — not the foreign HL service account
                "DatabaseName": hl_resource_link_db_name,
                "TableWildcard": {},
            }
        },
        Permissions=["SELECT", "DESCRIBE"],
        PermissionsWithGrantOption=[],
    )
    logger.info("Step 2 SELECT+DESCRIBE on resource link tables succeeded.")


def revoke_permissions(lf, account_id, lambda_role_arn, hl_resource_link_db_name, hl_service_catalog_id):
    """Best-effort revoke of both grants."""
    for resource, perms, label in [
        ({"Database": {"CatalogId": account_id, "Name": hl_resource_link_db_name}},
         ["DESCRIBE"], "DESCRIBE on DB"),
        ({"Table": {"CatalogId": account_id, "DatabaseName": hl_resource_link_db_name, "TableWildcard": {}}},
         ["SELECT", "DESCRIBE"], "SELECT+DESCRIBE on tables"),
    ]:
        try:
            lf.revoke_permissions(
                Principal={"DataLakePrincipalIdentifier": lambda_role_arn},
                Resource=resource, Permissions=perms, PermissionsWithGrantOption=[],
            )
            logger.info("Revoked %s.", label)
        except Exception as exc:
            logger.warning("Could not revoke %s (non-fatal): %s", label, exc)


def handler(event, context):
    logger.info("Event: %s", json.dumps(event))

    request_type = event["RequestType"]
    props = event.get("ResourceProperties", {})

    account_id = props["AccountId"]
    region = props["Region"]
    lambda_role_arn = props["LambdaRoleArn"]
    hl_resource_link_db_name = props["HlResourceLinkDbName"]
    hl_service_catalog_id = props["HlServiceCatalogId"]

    lf = boto3.client("lakeformation", region_name=region)
    physical_id = f"uc1-lf-hl-grant-{account_id}-{hl_resource_link_db_name}"

    # ------------------------------------------------------------------ Delete
    if request_type == "Delete":
        revoke_permissions(lf, account_id, lambda_role_arn,
                           hl_resource_link_db_name, hl_service_catalog_id)
        send_cfn_response(event, context, "SUCCESS", physical_id=physical_id)
        return

    # ---------------------------------------------------- Create / Update
    try:
        ensure_lf_admin(lf, lambda_role_arn)
        grant_permissions(lf, account_id, lambda_role_arn,
                          hl_resource_link_db_name, hl_service_catalog_id)
        send_cfn_response(event, context, "SUCCESS", physical_id=physical_id,
                          data={"HlResourceLinkDbName": hl_resource_link_db_name})
    except Exception as exc:
        logger.error("LF grant failed: %s", exc)
        send_cfn_response(event, context, "FAILED",
                          reason=str(exc), physical_id=physical_id)
