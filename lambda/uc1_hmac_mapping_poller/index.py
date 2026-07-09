# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
"""
uc1_hmac_mapping_poller/index.py

CFN custom resource — HMAC patient token mapping (poller, async pattern).

Triggered by EventBridge rate(2 minutes). Implements a two-phase state machine:

Phase WAITING:
  Run SELECT COUNT(*) FROM patient (fast, ~5-15s inline poll).
  - count == 0: Iceberg tables not yet populated — exit, retry next tick.
  - count > 0:  Submit full demographics SELECT (async), store QueryExecutionId,
                transition to QUERY_IN_FLIGHT.

Phase QUERY_IN_FLIGHT:
  Check query status with GetQueryExecution (instant API call).
  - QUEUED/RUNNING:   still executing — exit, retry next tick.
  - FAILED/CANCELLED: send CFN FAILED.
  - SUCCEEDED:        fetch all result rows (pagination), compute HMAC-SHA256
                      tokens, upload CSV to S3, register Glue table,
                      send CFN SUCCESS, disable EventBridge rule, cleanup SSM.

Why two phases? The demographics query for 50K patients takes ~2-3 min to
execute — longer than what a single 60s Lambda can wait synchronously.
Splitting submit and fetch into separate poller ticks handles any patient count.

Environment variables (set by CFN template)
-------------------------------------------
  RUN_ID                — stack run identifier
  HMAC_KEY_SECRET_ARN   — Secrets Manager secret ARN for the HMAC key
  HL_RESOURCE_LINK_DB   — Glue resource link DB name (HealthLake Iceberg tables)
  FEATURE_OUTPUT_BUCKET — S3 bucket for mapping CSV output
  FEATURE_GLUE_DB       — Glue database for mapping table registration
  ATHENA_WORKGROUP      — Athena workgroup name
  ATHENA_RESULTS_BUCKET — S3 bucket for Athena query result metadata
"""

import csv
import hashlib
import hmac as hmac_lib
import io
import json
import logging
import os
import time
import urllib.request

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

REGION                = os.environ.get("AWS_REGION", "us-east-1")
RUN_ID                = os.environ["RUN_ID"]
HMAC_KEY_SECRET_ARN   = os.environ["HMAC_KEY_SECRET_ARN"]
HL_RESOURCE_LINK_DB   = os.environ["HL_RESOURCE_LINK_DB"]
FEATURE_OUTPUT_BUCKET = os.environ["FEATURE_OUTPUT_BUCKET"]
FEATURE_GLUE_DB       = os.environ["FEATURE_GLUE_DB"]
ATHENA_WORKGROUP      = os.environ["ATHENA_WORKGROUP"]
ATHENA_RESULTS_BUCKET = os.environ["ATHENA_RESULTS_BUCKET"]

SSM_CALLBACK_KEY   = f"/healthlake-cleanrooms/{RUN_ID}/hmac-mapping-callback"
MAPPING_S3_KEY     = "mapping/patient_token_mapping.csv"
MAPPING_TABLE_NAME = "patient_token_mapping"
COUNT_POLL_SECONDS = 2   # interval for inline patient count query poll

athena        = boto3.client("athena",         region_name=REGION)
glue          = boto3.client("glue",           region_name=REGION)
s3            = boto3.client("s3",             region_name=REGION)
events_client = boto3.client("events",         region_name=REGION)
ssm_client    = boto3.client("ssm",            region_name=REGION)
sm_client     = boto3.client("secretsmanager", region_name=REGION)
sts_client    = boto3.client("sts",            region_name=REGION)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def send_cfn_response(state, status, reason="", data=None):
    body = json.dumps({
        "Status": status,
        "Reason": reason,
        "PhysicalResourceId": state.get("PhysicalResourceId", f"uc1-hmac-mapping-{RUN_ID}"),
        "StackId": state["StackId"],
        "RequestId": state["RequestId"],
        "LogicalResourceId": state["LogicalResourceId"],
        "Data": data or {},
    }).encode("utf-8")
    req = urllib.request.Request(
        state["ResponseURL"],
        data=body,
        headers={"Content-Type": "", "Content-Length": str(len(body))},
        method="PUT",
    )
    with urllib.request.urlopen(req) as resp:  # nosec B310 — URL is CFN pre-signed S3 ResponseURL (AWS-generated, HTTPS, not user input)
        logger.info("CFN response sent: %s %s", status, resp.status)


def disable_and_cleanup(poller_rule_name):
    """Disable EventBridge rule and remove SSM callback state."""
    try:
        events_client.disable_rule(Name=poller_rule_name)
        logger.info("Disabled poller rule: %s", poller_rule_name)
    except Exception as exc:
        logger.warning("Could not disable poller rule (non-fatal): %s", exc)
    try:
        ssm_client.delete_parameter(Name=SSM_CALLBACK_KEY)
        logger.info("Deleted SSM callback key")
    except Exception as exc:
        logger.warning("Could not delete SSM key (non-fatal): %s", exc)


def get_patient_count() -> int:
    """Run SELECT COUNT(*) and poll inline until complete (~5-15s). Returns int."""
    resp = athena.start_query_execution(
        # nosec B608 — HL_RESOURCE_LINK_DB is an AWS API-returned identifier (Lambda env var
        # set by CFN). ATHENA_WORKGROUP and FEATURE_GLUE_DB are CloudFormation outputs.
        QueryString=f'SELECT COUNT(*) FROM "{HL_RESOURCE_LINK_DB}".patient',  # nosec B608
        WorkGroup=ATHENA_WORKGROUP,
        ResultConfiguration={
            "OutputLocation": f"s3://{ATHENA_RESULTS_BUCKET}/hmac-readiness/",
            "EncryptionConfiguration": {"EncryptionOption": "SSE_S3"},
        },
        QueryExecutionContext={"Database": FEATURE_GLUE_DB},
    )
    qid = resp["QueryExecutionId"]
    for _ in range(30):
        time.sleep(COUNT_POLL_SECONDS)
        r = athena.get_query_execution(QueryExecutionId=qid)["QueryExecution"]
        if r["Status"]["State"] in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
    if r["Status"]["State"] != "SUCCEEDED":
        logger.warning("Count query %s — will retry next tick", r["Status"]["State"])
        return 0
    rows = athena.get_query_results(QueryExecutionId=qid)["ResultSet"]["Rows"]
    return int(rows[-1]["Data"][0].get("VarCharValue", "0"))


def submit_demographics_query() -> str:
    """Submit full patient demographics SELECT. Returns QueryExecutionId (does not wait)."""
    sql = (
        # nosec B608 — HL_RESOURCE_LINK_DB is an AWS API-returned identifier (Lambda env var
        # set by CFN). No user-supplied input involved in SQL construction.
        f'SELECT id, '  # nosec B608
        f'COALESCE(CAST(birthdate AS VARCHAR), \'\') AS birthdate, '
        f'COALESCE(TRIM(TRY(name[1].family)), \'\') AS last_name, '
        f'COALESCE(TRIM(TRY(name[1].given[1])), \'\') AS first_name, '
        f'COALESCE(gender, \'\') AS gender '
        f'FROM "{HL_RESOURCE_LINK_DB}".patient '
        f'WHERE id IS NOT NULL'
    )
    resp = athena.start_query_execution(
        QueryString=sql,
        WorkGroup=ATHENA_WORKGROUP,
        ResultConfiguration={
            "OutputLocation": f"s3://{ATHENA_RESULTS_BUCKET}/hmac-mapping/",
            "EncryptionConfiguration": {"EncryptionOption": "SSE_S3"},
        },
        QueryExecutionContext={"Database": FEATURE_GLUE_DB},
    )
    qid = resp["QueryExecutionId"]
    logger.info("Demographics query submitted (async): %s", qid)
    return qid


def fetch_all_rows(qid: str) -> list:
    """Page through Athena query results. Handles 50K+ patients via NextToken."""
    rows, next_token, first_page = [], None, True
    while True:
        kwargs = {"QueryExecutionId": qid, "MaxResults": 1000}
        if next_token:
            kwargs["NextToken"] = next_token
        resp = athena.get_query_results(**kwargs)
        result_rows = resp["ResultSet"]["Rows"]
        if first_page and result_rows:
            result_rows = result_rows[1:]  # drop header
            first_page = False
        rows.extend(result_rows)
        next_token = resp.get("NextToken")
        if not next_token:
            break
    logger.info("Fetched %d patient rows from Athena", len(rows))
    return rows


def build_and_upload_csv(rows: list, hmac_key: bytes) -> int:
    """Compute HMAC tokens, write CSV to S3. Returns patient count."""
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(["patient_uuid", "patient_token"])
    skipped = 0
    for row in rows:
        cells = [c.get("VarCharValue", "") for c in row["Data"]]
        if len(cells) < 5 or not cells[0]:
            skipped += 1
            continue
        patient_uuid, birthdate, last_name, first_name, gender = cells[:5]
        msg = "|".join([
            birthdate.strip().upper(), first_name.strip().upper(),
            last_name.strip().upper(), gender.strip().upper(),
        ]).encode("utf-8")
        token = hmac_lib.new(hmac_key, msg, hashlib.sha256).hexdigest()
        writer.writerow([patient_uuid, token])
    if skipped:
        logger.warning("Skipped %d rows with incomplete data", skipped)
    content = buf.getvalue()
    s3.put_object(
        Bucket=FEATURE_OUTPUT_BUCKET, Key=MAPPING_S3_KEY,
        Body=content.encode("utf-8"), ContentType="text/csv",
        ServerSideEncryption="AES256",
    )
    patient_count = content.count("\n") - 1
    logger.info("Mapping CSV uploaded: s3://%s/%s  (%d patients)", FEATURE_OUTPUT_BUCKET, MAPPING_S3_KEY, patient_count)
    return patient_count


def ensure_create_table_grant():
    """
    Grant CREATE_TABLE + DESCRIBE on the feature Glue database to this Lambda's role.
    Runs as CustomResourceLambdaRole (LF admin) — grant always succeeds.
    Defensive: ensures table creation works even if LF default permissions did not
    apply to the database at creation time (e.g., due to race with LF admin registration).
    """
    try:
        lf   = boto3.client("lakeformation", region_name=REGION)
        acct = sts_client.get_caller_identity()["Account"]
        arn  = sts_client.get_caller_identity()["Arn"]
        if "assumed-role" in arn:
            parts    = arn.split(":")
            role_arn = f"arn:aws:iam::{parts[4]}:role/{parts[5].split('/')[1]}"
        else:
            role_arn = arn
        lf.grant_permissions(
            Principal={"DataLakePrincipalIdentifier": role_arn},
            Resource={"Database": {"CatalogId": acct, "Name": FEATURE_GLUE_DB}},
            Permissions=["CREATE_TABLE", "DESCRIBE"],
            PermissionsWithGrantOption=[],
        )
        logger.info("CREATE_TABLE grant applied on '%s'", FEATURE_GLUE_DB)
    except Exception as exc:
        if "AlreadyExists" in str(exc):
            logger.info("CREATE_TABLE grant already exists (no-op)")
        else:
            logger.warning("CREATE_TABLE grant (non-fatal): %s", exc)


def register_glue_table():
    ensure_create_table_grant()
    acct = sts_client.get_caller_identity()["Account"]
    table_input = {
        "Name": MAPPING_TABLE_NAME,
        "TableType": "EXTERNAL_TABLE",
        "Parameters": {"classification": "csv", "skip.header.line.count": "1"},
        "StorageDescriptor": {
            "Location": f"s3://{FEATURE_OUTPUT_BUCKET}/mapping/",
            "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
            "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
            "SerdeInfo": {
                "SerializationLibrary": "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
                "Parameters": {"field.delim": ","},
            },
            "Columns": [
                {"Name": "patient_uuid", "Type": "string"},
                {"Name": "patient_token", "Type": "string"},
            ],
        },
    }
    try:
        glue.create_table(CatalogId=acct, DatabaseName=FEATURE_GLUE_DB, TableInput=table_input)
        logger.info("Glue table '%s' created in '%s'", MAPPING_TABLE_NAME, FEATURE_GLUE_DB)
    except glue.exceptions.AlreadyExistsException:
        glue.update_table(CatalogId=acct, DatabaseName=FEATURE_GLUE_DB, TableInput=table_input)
        logger.info("Glue table '%s' updated in '%s'", MAPPING_TABLE_NAME, FEATURE_GLUE_DB)


# ---------------------------------------------------------------------------
# Handler
# ---------------------------------------------------------------------------

def handler(event, context):
    logger.info("Poller triggered.")

    # Load callback state from SSM
    try:
        param = ssm_client.get_parameter(Name=SSM_CALLBACK_KEY)
        state = json.loads(param["Parameter"]["Value"])
    except ssm_client.exceptions.ParameterNotFound:
        logger.warning("SSM callback key not found — poller already completed or not started.")
        return
    except Exception as exc:
        logger.error("Failed to load SSM state: %s", exc)
        return

    phase            = state.get("Phase", "WAITING")
    poller_rule_name = state.get("PollerRuleName", f"hcls-adr-uc1-hmac-mapping-rule-{RUN_ID}")

    # ---------------------------------------------------------------- WAITING
    if phase == "WAITING":
        try:
            count = get_patient_count()
        except Exception as exc:
            logger.warning("Patient count check failed (retry next tick): %s", exc)
            return
        logger.info("Patient count: %d", count)
        if count == 0:
            logger.info("HealthLake Iceberg tables not yet populated. Retry next tick.")
            return
        # Data ready — submit demographics query and advance phase
        try:
            qid = submit_demographics_query()
        except Exception as exc:
            logger.error("Demographics query submission failed: %s", exc)
            disable_and_cleanup(poller_rule_name)
            send_cfn_response(state, "FAILED", reason=f"Demographics query failed: {exc}")
            return
        state["Phase"] = "QUERY_IN_FLIGHT"
        state["QueryExecutionId"] = qid
        ssm_client.put_parameter(
            Name=SSM_CALLBACK_KEY, Value=json.dumps(state),
            Type="String", Overwrite=True,
        )
        logger.info("Phase → QUERY_IN_FLIGHT. QueryExecutionId=%s", qid)
        return

    # --------------------------------------------------------- QUERY_IN_FLIGHT
    if phase == "QUERY_IN_FLIGHT":
        qid = state.get("QueryExecutionId")
        if not qid:
            logger.error("Missing QueryExecutionId in state — failing.")
            disable_and_cleanup(poller_rule_name)
            send_cfn_response(state, "FAILED", reason="Internal error: missing QueryExecutionId")
            return
        try:
            r = athena.get_query_execution(QueryExecutionId=qid)["QueryExecution"]
            query_state = r["Status"]["State"]
            logger.info("Demographics query %s state: %s", qid, query_state)
        except Exception as exc:
            logger.warning("GetQueryExecution failed (retry next tick): %s", exc)
            return

        if query_state in ("QUEUED", "RUNNING"):
            logger.info("Query still running. Retry next tick.")
            return

        if query_state in ("FAILED", "CANCELLED"):
            reason = r["Status"].get("StateChangeReason", "unknown")
            logger.error("Demographics query %s: %s", query_state, reason)
            disable_and_cleanup(poller_rule_name)
            send_cfn_response(state, "FAILED", reason=f"Demographics query {query_state}: {reason}")
            return

        if query_state == "SUCCEEDED":
            try:
                resp     = sm_client.get_secret_value(SecretId=HMAC_KEY_SECRET_ARN)
                hmac_key = (resp.get("SecretString") or "").encode("utf-8")
                rows     = fetch_all_rows(qid)
                count    = build_and_upload_csv(rows, hmac_key)
                register_glue_table()
                logger.info("HMAC mapping complete. Patients: %d", count)
                disable_and_cleanup(poller_rule_name)
                send_cfn_response(state, "SUCCESS", data={"PatientCount": str(count)})
            except Exception as exc:
                logger.error("HMAC processing failed: %s", exc, exc_info=True)
                disable_and_cleanup(poller_rule_name)
                send_cfn_response(state, "FAILED", reason=f"HMAC processing failed: {exc}")
            return

    logger.warning("Unknown phase '%s'. No action.", phase)
