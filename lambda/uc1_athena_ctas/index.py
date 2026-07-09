# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
"""
uc1_athena_ctas/index.py

CloudFormation custom resource OR Step Functions task Lambda.

Loads the feature engineering SQL, substitutes placeholders, executes as an
Athena CTAS query, applies Lake Formation grants, and deregisters the S3
location from LF (so Clean Rooms can use the table as a plain external table).

Environment variables (set by CFN template)
-------------------------------------------
  HL_RESOURCE_LINK_DB    — Glue resource link DB name (HealthLake Iceberg tables)
                           May be "placeholder" when called from Stack 2 Step Functions;
                           the real value is passed in the event payload as
                           ResourceProperties.HlResourceLinkDbName and takes precedence.
  FEATURE_OUTPUT_BUCKET  — S3 bucket for Parquet output
  FEATURE_GLUE_DB        — Glue database for the CTAS target table
  ATHENA_WORKGROUP       — Athena workgroup name
  ATHENA_RESULTS_BUCKET  — S3 bucket for Athena query result metadata
  RUN_ID                 — stack run identifier

Invocation modes:
  CFN custom resource:    event has ResponseURL pointing to CloudFormation pre-signed URL
  Step Functions task:    event has ResponseURL = "https://httpbin.org/put" (dummy)
                          — CFN response is skipped when called from Step Functions
"""

import json
import logging
import os
import time
import urllib.request
from pathlib import Path

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

REGION                = os.environ.get("AWS_REGION", "us-east-1")
FEATURE_OUTPUT_BUCKET = os.environ["FEATURE_OUTPUT_BUCKET"]
FEATURE_GLUE_DB       = os.environ["FEATURE_GLUE_DB"]
ATHENA_WORKGROUP      = os.environ["ATHENA_WORKGROUP"]
ATHENA_RESULTS_BUCKET = os.environ["ATHENA_RESULTS_BUCKET"]
RUN_ID                = os.environ["RUN_ID"]
# HL_RESOURCE_LINK_DB may be "placeholder" when deployed via Stack 2 Step Functions.
# The real value is injected from the event payload in handler().
_HL_RESOURCE_LINK_DB_ENV = os.environ.get("HL_RESOURCE_LINK_DB", "placeholder")

POLL_INTERVAL_SECONDS = 10
MAX_WAIT_SECONDS      = 540

athena_client = boto3.client("athena", region_name=REGION)
s3_client     = boto3.client("s3",     region_name=REGION)


def send_cfn_response(event, context, status, reason="", data=None, physical_id=None):
    """Send response to CloudFormation. Skipped when called from Step Functions."""
    response_url = event.get("ResponseURL", "")
    if "httpbin.org" in response_url or not response_url.startswith("https://cloudformation"):
        logger.info("Step Functions invocation — skipping CFN response (status=%s)", status)
        return
    body = json.dumps({
        "Status": status,
        "Reason": reason,
        "PhysicalResourceId": physical_id or event.get("PhysicalResourceId", "uc1-athena-ctas"),
        "StackId": event["StackId"],
        "RequestId": event["RequestId"],
        "LogicalResourceId": event["LogicalResourceId"],
        "Data": data or {},
    }).encode("utf-8")
    req = urllib.request.Request(
        response_url,
        data=body,
        headers={"Content-Type": "", "Content-Length": str(len(body))},
        method="PUT",
    )
    with urllib.request.urlopen(req) as resp:  # nosec B310 — URL is CFN pre-signed S3 ResponseURL (AWS-generated, HTTPS, not user input)
        logger.info("CFN response sent: %s %s", status, resp.status)


def load_sql(hl_resource_link_db):
    sql_path = Path(__file__).parent / "sql" / "feature_engineering.sql"
    template = sql_path.read_text(encoding="utf-8")
    # nosec B608 — all substitution values are CloudFormation outputs or AWS API-returned
    # identifiers (HealthLake resource link DB name). No user-supplied input involved.
    return template.format(  # nosec B608
        hl_resource_link_db=hl_resource_link_db,
        feature_output_bucket=FEATURE_OUTPUT_BUCKET,
        feature_glue_db=FEATURE_GLUE_DB,
    )


def wait_for_data(hl_resource_link_db):
    """Poll SELECT COUNT(*) FROM patient until > 0 (Iceberg tables populated after ACTIVE)."""
    # nosec B608 — HL_RESOURCE_LINK_DB is an AWS API-returned identifier (Lambda env var
    # set from CloudFormation). Not user input. FEATURE_GLUE_DB is a CloudFormation output.
    check_sql = f'SELECT COUNT(*) FROM "{hl_resource_link_db}".patient'  # nosec B608
    logger.info("Waiting for HealthLake Iceberg tables to be populated...")
    attempt = 0
    while True:
        attempt += 1
        try:
            resp = athena_client.start_query_execution(
                QueryString=check_sql,
                WorkGroup=ATHENA_WORKGROUP,
                ResultConfiguration={
                    "OutputLocation": f"s3://{ATHENA_RESULTS_BUCKET}/readiness-check/",
                    "EncryptionConfiguration": {"EncryptionOption": "SSE_S3"},
                },
                QueryExecutionContext={"Database": FEATURE_GLUE_DB},
            )
            qid = resp["QueryExecutionId"]
            for _ in range(30):
                time.sleep(5)
                r = athena_client.get_query_execution(QueryExecutionId=qid)["QueryExecution"]
                if r["Status"]["State"] in ("SUCCEEDED", "FAILED", "CANCELLED"):
                    break
            if r["Status"]["State"] == "SUCCEEDED":
                rows = athena_client.get_query_results(QueryExecutionId=qid)["ResultSet"]["Rows"]
                count = int(rows[-1]["Data"][0].get("VarCharValue", "0"))
                logger.info("Patient table row count: %d (attempt %d)", count, attempt)
                if count > 0:
                    logger.info("HealthLake Iceberg tables ready.")
                    return
            else:
                logger.warning("Readiness check %s (attempt %d)", r["Status"]["State"], attempt)
        except Exception as exc:
            logger.warning("Readiness check error (attempt %d): %s", attempt, exc)
        logger.info("Not ready. Waiting 120s...")
        time.sleep(120)


def ensure_create_table_grant():
    try:
        lf = boto3.client("lakeformation", region_name=REGION)
        role_arn = boto3.client("sts").get_caller_identity()["Arn"]
        if "assumed-role" in role_arn:
            parts = role_arn.split(":")
            acct, role_name = parts[4], parts[5].split("/")[1]
            role_arn = f"arn:aws:iam::{acct}:role/{role_name}"
        lf.grant_permissions(
            Principal={"DataLakePrincipalIdentifier": role_arn},
            Resource={"Database": {
                "CatalogId": boto3.client("sts").get_caller_identity()["Account"],
                "Name": FEATURE_GLUE_DB,
            }},
            Permissions=["CREATE_TABLE", "DESCRIBE", "DROP"],
            PermissionsWithGrantOption=[],
        )
        logger.info("CREATE_TABLE grant applied")
    except Exception as exc:
        logger.warning("CREATE_TABLE grant (non-fatal): %s", exc)


def ensure_feature_table_grant():
    try:
        lf = boto3.client("lakeformation", region_name=REGION)
        acct = boto3.client("sts").get_caller_identity()["Account"]
        lf.grant_permissions(
            Principal={"DataLakePrincipalIdentifier": "IAM_ALLOWED_PRINCIPALS"},
            Resource={"Table": {
                "CatalogId": acct,
                "DatabaseName": FEATURE_GLUE_DB,
                "Name": "healthcare_features",
            }},
            Permissions=["ALL"],
            PermissionsWithGrantOption=[],
        )
        logger.info("IAM_ALLOWED_PRINCIPALS ALL grant applied to healthcare_features")
    except Exception as exc:
        if "AlreadyExists" in str(exc):
            logger.info("LF grant already exists (no-op)")
        else:
            logger.warning("LF grant (non-fatal): %s", exc)


def deregister_feature_table_from_lf():
    """
    Deregister the feature S3 location from LF so Clean Rooms can use the table
    as a plain external Glue table (LF-governed tables are not supported by
    AWS Clean Rooms ConfiguredTable).
    """
    lf = boto3.client("lakeformation", region_name=REGION)
    for resource_arn in [
        f"arn:aws:s3:::{FEATURE_OUTPUT_BUCKET}/features",
        f"arn:aws:s3:::{FEATURE_OUTPUT_BUCKET}",
    ]:
        try:
            lf.describe_resource(ResourceArn=resource_arn)
            lf.deregister_resource(ResourceArn=resource_arn)
            logger.info("Deregistered LF resource: %s", resource_arn)
            return
        except lf.exceptions.EntityNotFoundException:
            continue
        except Exception as exc:
            logger.warning("Could not deregister %s (non-fatal): %s", resource_arn, exc)
            return
    logger.info("Feature S3 location not registered with LF — no deregistration needed.")


def delete_features_s3_prefix():
    """
    Delete all objects under s3://{FEATURE_OUTPUT_BUCKET}/features/ so the
    CTAS can write fresh Parquet files on re-runs.
    Without this, CTAS fails with HIVE_PATH_ALREADY_EXISTS on retries.
    """
    paginator = s3_client.get_paginator("list_objects_v2")
    prefix    = "features/"
    deleted   = 0
    for page in paginator.paginate(Bucket=FEATURE_OUTPUT_BUCKET, Prefix=prefix):
        objects = [{"Key": o["Key"]} for o in page.get("Contents", [])]
        if objects:
            s3_client.delete_objects(
                Bucket=FEATURE_OUTPUT_BUCKET,
                Delete={"Objects": objects, "Quiet": True},
            )
            deleted += len(objects)
    if deleted:
        logger.info("Deleted %d existing object(s) from s3://%s/%s",
                    deleted, FEATURE_OUTPUT_BUCKET, prefix)


def run_ctas_query(hl_resource_link_db):
    """
    Drop the target Glue table + delete existing S3 data (idempotent on retries),
    then execute the CTAS query.
    """
    glue = boto3.client("glue", region_name=REGION)

    # Drop existing Glue table entry
    try:
        glue.delete_table(DatabaseName=FEATURE_GLUE_DB, Name="healthcare_features")
        logger.info("Dropped pre-existing healthcare_features Glue table.")
    except glue.exceptions.EntityNotFoundException:
        pass
    except Exception as exc:
        logger.warning("Could not drop pre-existing Glue table (non-fatal): %s", exc)

    # Delete existing S3 Parquet files so CTAS can write fresh data (idempotent)
    delete_features_s3_prefix()

    sql = load_sql(hl_resource_link_db)
    logger.info("Submitting CTAS query to workgroup: %s", ATHENA_WORKGROUP)

    qid = athena_client.start_query_execution(
        QueryString=sql,
        WorkGroup=ATHENA_WORKGROUP,
        ResultConfiguration={
            "OutputLocation": f"s3://{ATHENA_RESULTS_BUCKET}/ctas-results/",
            "EncryptionConfiguration": {"EncryptionOption": "SSE_S3"},
        },
        QueryExecutionContext={"Database": FEATURE_GLUE_DB},
    )["QueryExecutionId"]
    logger.info("CTAS submitted: %s", qid)

    elapsed = 0
    while elapsed < MAX_WAIT_SECONDS:
        time.sleep(POLL_INTERVAL_SECONDS)
        elapsed += POLL_INTERVAL_SECONDS
        r = athena_client.get_query_execution(QueryExecutionId=qid)["QueryExecution"]
        state = r["Status"]["State"]
        logger.info("Query %s: %s (%ds)", qid, state, elapsed)
        if state == "SUCCEEDED":
            logger.info("CTAS succeeded.")
            return qid
        if state in ("FAILED", "CANCELLED"):
            raise RuntimeError(
                f"Athena query {qid} {state}: {r['Status'].get('StateChangeReason','')}"
            )

    try:
        athena_client.stop_query_execution(QueryExecutionId=qid)
    except Exception:
        pass
    raise TimeoutError(f"CTAS query {qid} did not complete within {MAX_WAIT_SECONDS}s.")


def handler(event, context):
    logger.info("Event: %s", json.dumps(event))

    request_type = event["RequestType"]
    physical_id  = f"uc1-athena-ctas-{RUN_ID}"

    if request_type == "Delete":
        logger.info("Delete — no CTAS cleanup needed.")
        send_cfn_response(event, context, "SUCCESS", physical_id=physical_id)
        return

    # Resolve HL_RESOURCE_LINK_DB: payload takes precedence over env var.
    # Stack 2 (Step Functions) passes HlResourceLinkDbName in ResourceProperties.
    # Stack 1 (CFN custom resource) sets the env var directly.
    props = event.get("ResourceProperties", {})
    hl_resource_link_db = (
        props.get("HlResourceLinkDbName")
        or _HL_RESOURCE_LINK_DB_ENV
    )
    if hl_resource_link_db == "placeholder":
        raise ValueError(
            "HL_RESOURCE_LINK_DB is 'placeholder'. "
            "Pass HlResourceLinkDbName in ResourceProperties when calling from Step Functions."
        )
    logger.info("Using HL_RESOURCE_LINK_DB: %s", hl_resource_link_db)

    try:
        ensure_create_table_grant()
        wait_for_data(hl_resource_link_db)
        qid = run_ctas_query(hl_resource_link_db)
        ensure_feature_table_grant()
        deregister_feature_table_from_lf()
        send_cfn_response(
            event, context, "SUCCESS",
            physical_id=physical_id,
            data={
                "QueryExecutionId": qid,
                "FeatureS3Location": f"s3://{FEATURE_OUTPUT_BUCKET}/features/",
            },
        )
    except Exception as exc:
        logger.error("CTAS failed: %s", exc, exc_info=True)
        send_cfn_response(event, context, "FAILED", reason=str(exc), physical_id=physical_id)
        raise  # Re-raise so Step Functions sees the failure via FunctionError
