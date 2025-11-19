import os, json, random, logging
import boto3
from botocore.session import Session
from botocore.awsrequest import AWSRequest
from botocore.auth import SigV4Auth
from botocore.exceptions import ClientError
import urllib.parse
import urllib3
from datetime import datetime


logging.getLogger().setLevel(logging.INFO)

REGION       = os.getenv("REGION", "us-east-1")
QUEUE_URL    = os.environ["QUEUE_URL"]
DDB_TABLE    = os.environ["DDB_TABLE"]
OS_ENDPOINT  = os.environ["OS_ENDPOINT"].rstrip("/")
OS_INDEX     = os.environ["OS_INDEX"]
SES_FROM     = os.environ["SES_FROM"]
NUM_RESULTS  = int(os.getenv("NUM_RESULTS", "3"))

sqs = boto3.client("sqs", region_name=REGION)
ddb = boto3.client("dynamodb", region_name=REGION)
ses = boto3.client("ses", region_name=REGION)

http = urllib3.PoolManager()
credentials = Session().get_credentials().get_frozen_credentials()

def os_signed_request(method, path, body=None, params=None):
    """SigV4-signed HTTP request to OpenSearch without extra deps."""
    if params:
        qs = urllib.parse.urlencode(params)
        url = f"{OS_ENDPOINT}{path}?{qs}"
    else:
        url = f"{OS_ENDPOINT}{path}"
    headers = {"Host": urllib.parse.urlparse(OS_ENDPOINT).netloc, "Content-Type": "application/json"}
    data = body if isinstance(body, (str, bytes)) else (json.dumps(body) if body is not None else None)
    req = AWSRequest(method=method, url=url, data=data, headers=headers)
    SigV4Auth(credentials, "es", REGION).add_auth(req)
    r = http.request(method, url, body=data, headers=dict(req.headers))
    if r.status not in (200, 201):
        raise RuntimeError(f"OpenSearch {r.status}: {r.data.decode('utf-8','ignore')}")
    return json.loads(r.data)

def os_random_ids_by_cuisine(cuisine, size):
    # Use function_score + random_score to randomize
    q = {
      "size": size,
      "query": {
        "function_score": {
          "query": { "term": { "cuisine": cuisine } },
          "random_score": {}
        }
      }
    }
    res = os_signed_request("POST", f"/{OS_INDEX}/_search", body=q)
    hits = res.get("hits", {}).get("hits", [])
    ids = []
    for h in hits:
        src = h.get("_source") or {}
        rid = src.get("restaurant_id") or h.get("_id")
        if rid:
            ids.append(rid)
    return ids

def ddb_get_many(business_ids):
    if not business_ids:
        return {}

    keys = [{"BusinessID": {"S": bid}} for bid in business_ids]

    res = ddb.batch_get_item(
        RequestItems={
            DDB_TABLE: {
                "Keys": keys,
                "ProjectionExpression": "BusinessID, #n, Address",
                "ExpressionAttributeNames": {"#n": "Name"}
            }
        }
    )

    out = {}
    for item in res.get("Responses", {}).get(DDB_TABLE, []):
        def g(d, k, t): return d.get(k, {}).get(t)

        bid = g(item, "BusinessID", "S")
        name = g(item, "Name", "S")
        addr = g(item, "Address", "S")

        out[bid] = {"BusinessID": bid, "Name": name, "Address": addr}

    return out

def format_email(body):
    # --- Build the intro line ---
    intro = f"Hello! Here are my {body['cuisine'].title()} restaurant suggestions"

    if body.get("partySize"):
        intro += f" for {body['partySize']} people"

    # --- Date and time formatting ---
    when_parts = []
    if body.get("date"):
        try:
            date_obj = datetime.strptime(body["date"], "%Y-%m-%d")
            when_parts.append(date_obj.strftime("%A, %B %-d, %Y"))  # e.g., Thursday, October 9, 2025
        except ValueError:
            when_parts.append(body["date"])

    if body.get("time"):
        try:
            # Convert 24-hour (e.g. "19:00") to 12-hour (e.g. "7 pm")
            time_obj = datetime.strptime(body["time"], "%H:%M")
            formatted_time = time_obj.strftime("%-I %p").lower()
            when_parts.append(f"at {formatted_time}")
        except ValueError:
            when_parts.append(f"at {body['time']}")  # fallback if format is weird

    if when_parts:
        intro += f", for {' '.join(when_parts)}"

    intro += ":"

    # --- Add restaurant lines ---
    lines = [intro]
    for i, r in enumerate(body.get("results", []), start=1):
        piece = f"{i}. {r['Name']}"
        if r.get("Address"):
            piece += f", located at {r['Address']}"
        lines.append(piece)

    lines.append("\nEnjoy your meal!")

    return "\n".join(lines)

def send_email(to_addr, subject, text):
    try:
        response = ses.send_email(
            Source=SES_FROM,
            Destination={"ToAddresses": [to_addr]},
            Message={
                "Subject": {"Data": subject},
                "Body": {"Text": {"Data": text}}
            }
        )
        print("Email sent! Message ID:", response["MessageId"])
        return True
    except ClientError as e:
        print("SES failed:", e.response['Error']['Message'])
        return False


def process_one_message(msg):
    body_raw = msg.get("Body") or "{}"
    try:
        payload = json.loads(body_raw)
    except Exception:
        logging.error("Bad SQS body: %s", body_raw)
        return False  # transient failure → retry → eventually DLQ

    cuisine = (payload.get("cuisine") or "").strip().lower()
    email   = (payload.get("email") or "").strip()
    if not cuisine or not email:
        logging.warning("Missing cuisine or email: %s", payload)
        return True  # discard bad message to avoid poison

    # 1) Random IDs from OpenSearch
    try:
        ids = os_random_ids_by_cuisine(cuisine, NUM_RESULTS * 2)
    except Exception as e:
        logging.error("OpenSearch failed: %s", e)
        return False  # transient failure → retry → eventually DLQ

    ids = list(dict.fromkeys(ids))[:NUM_RESULTS]
    if not ids:
        logging.warning("No hits in OpenSearch for cuisine=%s", cuisine)
        return True  # safe to delete

    # # 2) Details from DynamoDB
    try:
        details = ddb_get_many(ids)
    except Exception as e:
        logging.error("DynamoDB failed: %s", e)
        return False  # transient failure → retry → eventually DLQ

    results = [details[i] for i in ids if i in details]
    if not results:
        logging.warning("No DynamoDB matches for %s", ids)
        return True  # safe to delete

    # # 3) Email
    enriched = {
        "cuisine": cuisine,
        "partySize": payload.get("num_people"),
        "date": payload.get("dining_date"),
        "time": payload.get("dining_time"),
        "results": results
    }
    subject = f"{cuisine.title()} restaurant suggestions"
    text = format_email(enriched)

    sent = send_email(email, subject, text)
    if sent:
        logging.info("Sent suggestions to %s", email)
        return True
    else:
        logging.error("Failed to send email for %s", email)
        return False

def lambda_handler(event, context):
    # Pull up to 10 msgs per run
    resp = sqs.receive_message(
        QueueUrl=QUEUE_URL, MaxNumberOfMessages=10,
        VisibilityTimeout=30, WaitTimeSeconds=3
    )
    msgs = resp.get("Messages", [])

    if not msgs:
        return {"ok": True, "processed": 0}

    processed = 0
    for m in msgs:
        ok = False
        try:
            ok = process_one_message(m)
        except Exception as e:
            logging.exception("Processing failed: %s", e)
        if ok:
            sqs.delete_message(QueueUrl=QUEUE_URL, ReceiptHandle=m["ReceiptHandle"])
            processed += 1
    return {"ok": True, "processed": processed}
