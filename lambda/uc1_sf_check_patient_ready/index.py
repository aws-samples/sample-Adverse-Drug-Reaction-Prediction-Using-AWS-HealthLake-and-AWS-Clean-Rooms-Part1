# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
"""
uc1_sf_check_patient_ready — SFN task.
Runs SELECT COUNT(*) on the HealthLake Iceberg patient table.
Returns {"Ready": true/false} so the state machine can loop until data appears.
"""
import json, logging, os, time
import boto3

logger = logging.getLogger(); logger.setLevel(logging.INFO)

REGION = os.environ.get("AWS_REGION", "us-east-1")
athena = boto3.client("athena", region_name=REGION)


def handler(event, context):
    hl_db    = event["ResourceLink"]["HlResourceLinkDbName"]
    wg       = event["AthenaWorkGroup"]
    res_bkt  = event["AthenaResultsBucket"]
    feat_db  = event["FeatureGlueDb"]

    # nosec B608 — hl_db is the AWS HealthLake resource link DB name (AWS API-returned
    # identifier, not user input). feat_db is a CloudFormation output.
    sql = f'SELECT COUNT(*) FROM "{hl_db}".patient'  # nosec B608
    resp = athena.start_query_execution(
        QueryString=sql,
        WorkGroup=wg,
        ResultConfiguration={
            "OutputLocation": f"s3://{res_bkt}/sf-patient-check/",
            "EncryptionConfiguration": {"EncryptionOption": "SSE_S3"},
        },
        QueryExecutionContext={"Database": feat_db},
    )
    qid = resp["QueryExecutionId"]

    for _ in range(30):
        time.sleep(3)
        r = athena.get_query_execution(QueryExecutionId=qid)["QueryExecution"]
        if r["Status"]["State"] in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break

    if r["Status"]["State"] != "SUCCEEDED":
        logger.warning("Patient count query %s", r["Status"]["State"])
        return {"Ready": False}

    rows  = athena.get_query_results(QueryExecutionId=qid)["ResultSet"]["Rows"]
    count = int(rows[-1]["Data"][0].get("VarCharValue", "0"))
    logger.info("Patient count: %d", count)
    return {"Ready": count > 0}
