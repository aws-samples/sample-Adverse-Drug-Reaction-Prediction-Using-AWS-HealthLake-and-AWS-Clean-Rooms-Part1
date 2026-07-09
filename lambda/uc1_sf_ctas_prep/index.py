# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
"""
uc1_sf_ctas_prep -- SFN task.
1. Drops the existing healthcare_features Glue table (idempotent).
2. Deletes all S3 objects at features/ prefix (idempotent on retries).
3. Reads the CTAS SQL template from the bundled sql/ directory,
   substitutes runtime variables.
4. Submits the CTAS query to Athena and returns QueryExecutionId immediately.

Step Functions then polls for completion natively via athena:getQueryExecution
(no Lambda timeout risk regardless of patient count).
"""
import logging
import os
from pathlib import Path

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

REGION = os.environ.get("AWS_REGION", "us-east-1")
s3     = boto3.client("s3",     region_name=REGION)
glue   = boto3.client("glue",   region_name=REGION)
athena = boto3.client("athena", region_name=REGION)


def handler(event, context):
    hl_db    = event["ResourceLink"]["HlResourceLinkDbName"]
    feat_bkt = event["FeatureOutputBucket"]
    feat_db  = event["FeatureGlueDb"]
    wg       = event["AthenaWorkGroup"]
    res_bkt  = event["AthenaResultsBucket"]

    # ------------------------------------------------------------------
    # 1. Drop existing Glue table so CTAS can recreate it cleanly
    # ------------------------------------------------------------------
    try:
        glue.delete_table(DatabaseName=feat_db, Name="healthcare_features")
        logger.info("Dropped pre-existing healthcare_features table")
    except glue.exceptions.EntityNotFoundException:
        pass

    # ------------------------------------------------------------------
    # 2. Delete existing S3 features/ objects so CTAS writes fresh data
    # ------------------------------------------------------------------
    paginator = s3.get_paginator("list_objects_v2")
    deleted = 0
    for page in paginator.paginate(Bucket=feat_bkt, Prefix="features/"):
        objs = [{"Key": o["Key"]} for o in page.get("Contents", [])]
        if objs:
            s3.delete_objects(Bucket=feat_bkt, Delete={"Objects": objs, "Quiet": True})
            deleted += len(objs)
    if deleted:
        logger.info("Deleted %d objects from %s/features/", deleted, feat_bkt)

    # ------------------------------------------------------------------
    # 3. Load and substitute SQL template
    # ------------------------------------------------------------------
    sql_path = Path(__file__).parent / "sql" / "feature_engineering.sql"
    # nosec B608 — values substituted here are CloudFormation outputs (bucket/DB names)
    # and the AWS-discovered HealthLake resource link DB name. No user input involved.
    sql = sql_path.read_text(encoding="utf-8").format(  # nosec B608
        hl_resource_link_db=hl_db,
        feature_output_bucket=feat_bkt,
        feature_glue_db=feat_db,
    )
    logger.info("CTAS SQL prepared (%d chars)", len(sql))

    # ------------------------------------------------------------------
    # 4. Submit CTAS query and return immediately
    #    Lambda (CustomResourceLambdaRole) has all LF/S3/Glue permissions.
    #    Step Functions polls for completion via native athena:getQueryExecution.
    # ------------------------------------------------------------------
    resp = athena.start_query_execution(
        QueryString=sql,
        WorkGroup=wg,
        ResultConfiguration={
            "OutputLocation": f"s3://{res_bkt}/ctas-results/",
            "EncryptionConfiguration": {"EncryptionOption": "SSE_S3"},
        },
        QueryExecutionContext={"Database": feat_db},
    )
    qid = resp["QueryExecutionId"]
    logger.info("CTAS query submitted: %s", qid)

    return {
        "QueryExecutionId": qid,
        "FeatureGlueDb": feat_db,
        "FeatureBucket": feat_bkt,
    }
