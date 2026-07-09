# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
"""
uc1_hmac_key_manager/index.py

CloudFormation custom resource — shared HMAC key manager.

Creates the shared HMAC-SHA256 key the first time it is called (by either
UC1 Stack 1 / small or Stack 2 / large). Subsequent calls from other stacks
find the existing secret and return its ARN unchanged.

CRITICAL — why this matters:
  Patient tokens are computed as HMAC-SHA256(key, DOB|FirstName|LastName|Gender).
  The Pharma dataset (UC2) and the Healthcare dataset (UC1) must produce
  IDENTICAL tokens for the same patient so that AWS Clean Rooms can JOIN them.
  Both datasets MUST use the same key. If they use different keys, the JOIN
  produces zero matches and the ML model cannot train.

Design decisions:
  - Fixed secret name (no RunId): shared across all UC1 and UC2 deployments.
  - "Create if not exists": first stack to deploy creates the key; subsequent
    stacks reuse it. Race condition handled via ResourceExistsException catch.
  - On Delete: the secret is NOT deleted — another stack may still be using it.
    Delete manually only after every UC1 and UC2 stack is torn down.

Fixed resources (same in every region — SSM and Secrets Manager are region-scoped
so there is no cross-account or cross-region collision):
  Secret name : healthlake-cleanrooms-pprl-hmac-key
  SSM path    : /healthlake-cleanrooms/shared/hmac-key-secret-arn

Manual deletion command (run after ALL stacks are deleted):
  aws secretsmanager delete-secret \\
    --secret-id healthlake-cleanrooms-pprl-hmac-key \\
    --force-delete-without-recovery

Environment variables (set by CFN template):
  RUN_ID  — the deploying stack's RunId (used only for logging / tagging)
"""

import json
import logging
import os
import secrets
import string
import urllib.request

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

REGION      = os.environ.get("AWS_REGION", "us-east-1")
RUN_ID      = os.environ["RUN_ID"]

# Fixed names — no RunId suffix — shared across all UC1/UC2 deployments in this region
SECRET_NAME = "healthlake-cleanrooms-pprl-hmac-key"    # nosec B105 — this is the Secrets Manager secret NAME (resource identifier), not a credential value
SSM_KEY     = "/healthlake-cleanrooms/shared/hmac-key-secret-arn"  # nosec B105 — this is an SSM Parameter Store path (resource identifier), not a credential value

sm  = boto3.client("secretsmanager", region_name=REGION)
ssm = boto3.client("ssm",            region_name=REGION)


def send_cfn_response(event, status, reason="", data=None, physical_id=None):
    body = json.dumps({
        "Status": status,
        "Reason": reason,
        "PhysicalResourceId": physical_id or event.get("PhysicalResourceId", "hmac-key-manager"),
        "StackId":            event["StackId"],
        "RequestId":          event["RequestId"],
        "LogicalResourceId":  event["LogicalResourceId"],
        "Data":               data or {},
    }).encode("utf-8")
    req = urllib.request.Request(
        event["ResponseURL"], data=body,
        headers={"Content-Type": "", "Content-Length": str(len(body))},
        method="PUT",
    )
    with urllib.request.urlopen(req) as resp:  # nosec B310 — URL is CFN pre-signed S3 ResponseURL (AWS-generated, HTTPS, not user input)
        logger.info("CFN response sent: %s", resp.status)


def get_or_create_secret() -> str:
    """
    Return the ARN of the shared HMAC key. Creates it on first call.

    Race-condition handling: if two stacks deploy in parallel and both try
    CreateSecret simultaneously, the loser gets ResourceExistsException and
    falls back to DescribeSecret — same safe outcome either way.
    """
    # Check for existing secret first
    try:
        arn = sm.describe_secret(SecretId=SECRET_NAME)["ARN"]
        logger.info("Found existing HMAC key (RunId=%s): %s", RUN_ID, arn)
        return arn
    except ClientError as e:
        if e.response["Error"]["Code"] != "ResourceNotFoundException":
            raise
        logger.info("Secret not found — creating it (RunId=%s)", RUN_ID)

    # Create the secret with a programmatically generated 32-char alphanumeric key.
    # Note: create_secret does NOT support GenerateSecretString (that's CFN-only).
    # We generate the key using Python's cryptographically secure secrets module.
    alphabet = string.ascii_letters + string.digits
    hmac_key = "".join(secrets.choice(alphabet) for _ in range(32))

    try:
        arn = sm.create_secret(
            Name=SECRET_NAME,
            Description=(
                "Shared HMAC-SHA256 key for Privacy-Preserving Record Linkage (PPRL) "
                "across UC1 (small and large datasets) and UC2. "
                "Patient tokens: HMAC-SHA256(key, DOB|FirstName|LastName|Gender). "
                "IMPORTANT: Do NOT rotate this key without redeploying all UC1 and UC2 stacks. "
                "Delete manually ONLY after every stack is torn down — see README."
            ),
            SecretString=hmac_key,
            Tags=[
                {"Key": "Project",   "Value": "healthlake-cleanrooms"},
                {"Key": "CreatedBy", "Value": f"UC1-RunId-{RUN_ID}"},
                {"Key": "Usage",     "Value": "PPRL-HMAC-key-shared"},
            ],
        )["ARN"]
        logger.info("Created HMAC key secret: %s", arn)
        return arn
    except ClientError as e:
        # Parallel deploy race: another stack won the creation race
        if e.response["Error"]["Code"] in ("ResourceExistsException", "InvalidRequestException"):
            logger.info("Secret created by parallel stack — fetching ARN")
            return sm.describe_secret(SecretId=SECRET_NAME)["ARN"]
        raise


def handler(event, context):
    logger.info("RequestType=%s RunId=%s", event["RequestType"], RUN_ID)
    physical_id = "hmac-key-manager"

    if event["RequestType"] == "Delete":
        # Delete the shared HMAC secret on stack deletion.
        # The secret is only used during the pipeline run to compute patient tokens.
        # Once the pipeline has completed, the tokens are stored in S3 and the secret
        # is no longer needed for ongoing operation.
        # If multiple stacks share this secret, redeploy will recreate it automatically
        # via get_or_create_secret on the next stack creation.
        try:
            sm.delete_secret(SecretId=SECRET_NAME, ForceDeleteWithoutRecovery=True)
            logger.info("Deleted HMAC key secret: %s (RunId=%s)", SECRET_NAME, RUN_ID)
        except ClientError as e:
            if e.response["Error"]["Code"] in ("ResourceNotFoundException", "InvalidParameterException"):
                logger.info("HMAC key secret already deleted or not found — OK")
            else:
                logger.warning("Could not delete HMAC secret (non-fatal): %s", e)
        try:
            ssm.delete_parameter(Name=SSM_KEY)
            logger.info("Deleted SSM key: %s", SSM_KEY)
        except ssm.exceptions.ParameterNotFound:
            pass
        send_cfn_response(event, "SUCCESS", physical_id=physical_id)
        return

    # Create / Update
    try:
        arn = get_or_create_secret()

        # Write to the fixed SSM path so UC2 always finds it regardless of
        # which UC1 stack (small or large) was deployed, and regardless of RunId.
        ssm.put_parameter(Name=SSM_KEY, Value=arn, Type="String", Overwrite=True)
        logger.info("SSM updated: %s → %s", SSM_KEY, arn)

        send_cfn_response(
            event, "SUCCESS",
            physical_id=physical_id,
            data={"HmacKeySecretArn": arn},
        )
    except Exception as exc:
        logger.error("HMAC key manager failed: %s", exc, exc_info=True)
        send_cfn_response(event, "FAILED", reason=str(exc), physical_id=physical_id)
