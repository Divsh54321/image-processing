import os
import uuid
from datetime import datetime, timedelta, timezone

import boto3
from botocore.config import Config
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

REGION = os.getenv("AWS_DEFAULT_REGION", "ap-south-1")
RAW_BUCKET = os.environ["RAW_BUCKET"]
PROCESSED_BUCKET = os.environ["PROCESSED_BUCKET"]
TABLE_NAME = os.environ["TABLE_NAME"]

s3 = boto3.client(
    "s3",
    region_name=REGION,
    endpoint_url=f"https://s3.{REGION}.amazonaws.com",
    config=Config(signature_version="s3v4", s3={"addressing_style": "virtual"}),
)
table = boto3.resource("dynamodb", region_name=REGION).Table(TABLE_NAME)

app = FastAPI()


class UploadRequest(BaseModel):
    content_type: str = "image/jpeg"


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/uploads", status_code=201)
def create_upload(req: UploadRequest):
    job_id = str(uuid.uuid4())
    key = f"uploads/{job_id}.jpg"
    now = datetime.now(timezone.utc)

    table.put_item(Item={
        "job_id": job_id,
        "status": "awaiting_upload",
        "original_key": key,
        "created_at": now.isoformat(),
        "updated_at": now.isoformat(),
        "expires_at": int((now + timedelta(hours=24)).timestamp()),
    })

    url = s3.generate_presigned_url(
        "put_object",
        Params={"Bucket": RAW_BUCKET, "Key": key, "ContentType": req.content_type},
        ExpiresIn=300,
    )
    return {"job_id": job_id, "upload_url": url, "expires_in": 300}


@app.get("/jobs/{job_id}")
def get_job(job_id: str):
    item = table.get_item(Key={"job_id": job_id}).get("Item")
    if not item:
        raise HTTPException(status_code=404, detail="Job not found")

    out = {"job_id": job_id, "status": item["status"]}
    if item["status"] == "completed":
        out["result_url"] = s3.generate_presigned_url(
            "get_object",
            Params={"Bucket": PROCESSED_BUCKET, "Key": item["result_key"]},
            ExpiresIn=300,
        )
    if item["status"] == "failed":
        out["error"] = item.get("error", "unknown error")
    return out
