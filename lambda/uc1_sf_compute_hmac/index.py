# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
"""
uc1_sf_compute_hmac — SFN task.
Paginates a completed Athena demographics query, computes HMAC-SHA256
patient tokens, uploads two S3 files, and registers the Glue table.
Lambda timeout: 900s  Memory: 1024 MB
"""
import csv, hashlib, hmac as hmac_lib, io, json, logging, os
import boto3

logger = logging.getLogger(); logger.setLevel(logging.INFO)

REGION = os.environ.get("AWS_REGION", "us-east-1")
athena = boto3.client("athena",         region_name=REGION)
s3     = boto3.client("s3",             region_name=REGION)
glue   = boto3.client("glue",           region_name=REGION)
sm     = boto3.client("secretsmanager", region_name=REGION)
sts    = boto3.client("sts",            region_name=REGION)


def handler(event, context):
    qid            = event["HmacQuery"]["QueryExecutionId"]
    feat_bucket    = event["FeatureOutputBucket"]
    feat_glue_db   = event["FeatureGlueDb"]
    secret_arn     = event["HmacKeySecretArn"]
    account_id     = event.get("AccountId") or sts.get_caller_identity()["Account"]

    # Fetch HMAC key
    key = (sm.get_secret_value(SecretId=secret_arn).get("SecretString") or "").encode()

    # Paginate Athena results
    rows, next_token, first = [], None, True
    while True:
        kwargs = {"QueryExecutionId": qid, "MaxResults": 1000}
        if next_token:
            kwargs["NextToken"] = next_token
        resp = athena.get_query_results(**kwargs)
        r = resp["ResultSet"]["Rows"]
        if first and r:
            r = r[1:]
            first = False
        rows.extend(r)
        next_token = resp.get("NextToken")
        if not next_token:
            break
    logger.info("Fetched %d patient rows", len(rows))

    # Build CSVs
    map_buf  = io.StringIO(); map_w  = csv.writer(map_buf)
    dem_buf  = io.StringIO(); dem_w  = csv.writer(dem_buf)
    map_w.writerow(["patient_uuid", "patient_token"])
    dem_w.writerow(["birthdate", "first_name", "last_name", "gender"])

    skipped = 0
    for row in rows:
        cells = [c.get("VarCharValue", "") for c in row["Data"]]
        if len(cells) < 5 or not cells[0]:
            skipped += 1; continue
        uuid, birthdate, last, first_n, gender = cells[:5]
        msg   = "|".join([birthdate.strip().upper(), first_n.strip().upper(),
                          last.strip().upper(), gender.strip().upper()]).encode()
        token = hmac_lib.new(key, msg, hashlib.sha256).hexdigest()
        map_w.writerow([uuid, token])
        dem_w.writerow([birthdate, first_n, last, gender])

    if skipped:
        logger.warning("Skipped %d rows with incomplete data", skipped)
    patient_count = map_buf.getvalue().count("\n") - 1
    logger.info("Computed %d HMAC tokens", patient_count)

    # Upload
    for key_s3, buf in [("mapping/patient_token_mapping.csv",       map_buf),
                         ("shared-registry/patient_demographics.csv", dem_buf)]:
        s3.put_object(Bucket=feat_bucket, Key=key_s3,
                      Body=buf.getvalue().encode("utf-8"),
                      ContentType="text/csv", ServerSideEncryption="AES256")
    logger.info("Uploaded mapping and demographics CSVs")

    # Register Glue table patient_token_mapping
    table_input = {
        "Name": "patient_token_mapping",
        "TableType": "EXTERNAL_TABLE",
        "Parameters": {"classification": "csv", "skip.header.line.count": "1"},
        "StorageDescriptor": {
            "Location": f"s3://{feat_bucket}/mapping/",
            "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
            "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
            "SerdeInfo": {"SerializationLibrary":
                          "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
                          "Parameters": {"field.delim": ","}},
            "Columns": [{"Name": "patient_uuid", "Type": "string"},
                        {"Name": "patient_token", "Type": "string"}],
        },
    }
    try:
        glue.create_table(CatalogId=account_id, DatabaseName=feat_glue_db, TableInput=table_input)
    except glue.exceptions.AlreadyExistsException:
        glue.update_table(CatalogId=account_id, DatabaseName=feat_glue_db, TableInput=table_input)
    logger.info("Glue table patient_token_mapping registered")

    return {"PatientCount": patient_count}
