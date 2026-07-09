# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
"""
uc1_sf_ctas_grants — SFN task.
1. Grants IAM_ALLOWED_PRINCIPALS ALL on healthcare_features (IAM bypass mode).
2. Deregisters the features/ S3 location from Lake Formation so Clean Rooms
   can use the table as a plain external Glue table.
"""
import logging, os
import boto3

logger = logging.getLogger(); logger.setLevel(logging.INFO)

REGION = os.environ.get("AWS_REGION", "us-east-1")
lf  = boto3.client("lakeformation", region_name=REGION)
sts = boto3.client("sts",           region_name=REGION)


def handler(event, context):
    feat_db  = event["CtasPrep"]["FeatureGlueDb"]
    feat_bkt = event["CtasPrep"]["FeatureBucket"]
    acct     = sts.get_caller_identity()["Account"]

    # Grant ALL to IAM_ALLOWED_PRINCIPALS
    try:
        lf.grant_permissions(
            Principal={"DataLakePrincipalIdentifier": "IAM_ALLOWED_PRINCIPALS"},
            Resource={"Table": {"CatalogId": acct, "DatabaseName": feat_db,
                                "Name": "healthcare_features"}},
            Permissions=["ALL"], PermissionsWithGrantOption=[],
        )
        logger.info("IAM_ALLOWED_PRINCIPALS ALL grant applied")
    except Exception as exc:
        if "AlreadyExists" in str(exc):
            logger.info("Grant already exists (no-op)")
        else:
            logger.warning("LF grant (non-fatal): %s", exc)

    # Deregister S3 location from LF so Clean Rooms can consume as plain table
    for arn in [f"arn:aws:s3:::{feat_bkt}/features",
                f"arn:aws:s3:::{feat_bkt}"]:
        try:
            lf.describe_resource(ResourceArn=arn)
            lf.deregister_resource(ResourceArn=arn)
            logger.info("Deregistered %s", arn)
            break
        except lf.exceptions.EntityNotFoundException:
            continue
        except Exception as exc:
            logger.warning("Deregister (non-fatal): %s", exc)
            break

    return {"Status": "SUCCESS"}
