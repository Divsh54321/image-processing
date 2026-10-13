import io, json, os, urllib.parse
from datetime import datetime, timezone
import boto3
from PIL import Image

sqs = boto3.client("sqs")
s3 = boto3.client("s3")
table = boto3.resource("dynamodb").Table(os.environ["TABLE_NAME"])
QUEUE_URL = os.environ["QUEUE_URL"]
PROCESSED_BUCKET = os.environ["PROCESSED_BUCKET"]
MAX_RECEIVES = 3  # keep in sync with the DLQ maxReceiveCount

def now():
    return datetime.now(timezone.utc).isoformat()

def mark_processing(job_id):
    try:
        table.update_item(
            Key={"job_id": job_id},
            UpdateExpression="SET #s = :p, updated_at = :t",
            ConditionExpression="#s <> :done",
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={":p": "processing", ":done": "completed", ":t": now()},
        )
        return True
    except table.meta.client.exceptions.ConditionalCheckFailedException:
        return False  # already completed by an earlier delivery: skip

def mark_done(job_id, result_key):
    table.update_item(
        Key={"job_id": job_id},
        UpdateExpression="SET #s = :c, result_key = :k, updated_at = :t",
        ExpressionAttributeNames={"#s": "status"},
        ExpressionAttributeValues={":c": "completed", ":k": result_key, ":t": now()},
    )

def mark_failed(job_id, error):
    table.update_item(
        Key={"job_id": job_id},
        UpdateExpression="SET #s = :f, #e = :err, updated_at = :t",
        ExpressionAttributeNames={"#s": "status", "#e": "error"},
        ExpressionAttributeValues={":f": "failed", ":err": error, ":t": now()},
    )

def make_thumbnail(data: bytes) -> bytes:
    img = Image.open(io.BytesIO(data)).convert("RGB")
    img.thumbnail((800, 800))
    out = io.BytesIO()
    img.save(out, format="JPEG", quality=85)
    return out.getvalue()

def process(bucket, key, job_id):
    if not mark_processing(job_id):
        return
    raw = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
    result_key = f"processed/{job_id}.jpg"          # deterministic = idempotent
    s3.put_object(Bucket=PROCESSED_BUCKET, Key=result_key, Body=make_thumbnail(raw), ContentType="image/jpeg")
    mark_done(job_id, result_key)

def handle_message(msg):
    receive_count = int(msg["Attributes"]["ApproximateReceiveCount"])
    for record in json.loads(msg["Body"]).get("Records", []):   # TestEvent has none
        bucket = record["s3"]["bucket"]["name"]
        key = urllib.parse.unquote_plus(record["s3"]["object"]["key"])
        job_id = key.rsplit("/", 1)[-1].rsplit(".", 1)[0]
        try:
            process(bucket, key, job_id)
        except Exception as e:
            if receive_count >= MAX_RECEIVES:
                mark_failed(job_id, str(e))
            raise

while True:
    resp = sqs.receive_message(
        QueueUrl=QUEUE_URL, MaxNumberOfMessages=5, WaitTimeSeconds=20,
        VisibilityTimeout=60, AttributeNames=["ApproximateReceiveCount"],
    )
    for msg in resp.get("Messages", []):
        try:
            handle_message(msg)
            sqs.delete_message(QueueUrl=QUEUE_URL, ReceiptHandle=msg["ReceiptHandle"])
        except Exception as e:
            print(f"processing failed, will retry: {e}")   # no delete: the message reappears
