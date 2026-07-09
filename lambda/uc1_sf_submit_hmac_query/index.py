# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
"""
uc1_sf_submit_hmac_query — SFN task.
Submits the Athena demographics SELECT query and returns immediately with
the QueryExecutionId. Step Functions polls for completion natively.
"""
import logging, os
import boto3

logger = logging.getLogger(); logger.setLevel(logging.INFO)

REGION = os.environ.get("AWS_REGION", "us-east-1")
athena = boto3.client("athena", region_name=REGION)


def handler(event, context):
    hl_db   = event["ResourceLink"]["HlResourceLinkDbName"]
    wg      = event["AthenaWorkGroup"]
    res_bkt = event["AthenaResultsBucket"]
    feat_db = event["FeatureGlueDb"]

    sql = (
        # nosec B608 — all f-string values are AWS resource identifiers from CloudFormation
        # outputs and the HealthLake resource link DB name (AWS API-returned). No user input.
        f'SELECT id, '  # nosec B608
        f'COALESCE(CAST(birthdate AS VARCHAR), \'\') AS birthdate, '
        f'COALESCE(TRIM(TRY(name[1].family)), \'\') AS last_name, '
        f'COALESCE(TRIM(TRY(name[1].given[1])), \'\') AS first_name, '
        f'COALESCE(gender, \'\') AS gender '
        f'FROM "{hl_db}".patient WHERE id IS NOT NULL'
    )
    resp = athena.start_query_execution(
        QueryString=sql,
        WorkGroup=wg,
        ResultConfiguration={
            "OutputLocation": f"s3://{res_bkt}/sf-hmac-mapping/",
            "EncryptionConfiguration": {"EncryptionOption": "SSE_S3"},
        },
        QueryExecutionContext={"Database": feat_db},
    )
    qid = resp["QueryExecutionId"]
    logger.info("Demographics query submitted: %s", qid)
    return {"QueryExecutionId": qid}
