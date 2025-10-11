#!/usr/bin/env python3
import json
import urllib3
import urllib.parse
from decimal import Decimal
import boto3
from botocore.session import Session
from botocore.awsrequest import AWSRequest
from botocore.auth import SigV4Auth

REGION = "us-east-1"
SERVICE = "es"
OS_ENDPOINT = "https://search-opensearchdinning-id7jzgvfbjqqpobndub2h3h4am.aos.us-east-1.on.aws"
DDB_TABLE = "yelp-restaurants"
BATCH_SIZE = 500

http = urllib3.PoolManager()
creds = Session().get_credentials().get_frozen_credentials()


def es_bulk(lines):
    url = OS_ENDPOINT + "/_bulk"
    headers = {
        "Host": urllib.parse.urlparse(OS_ENDPOINT).netloc,
        "Content-Type": "application/x-ndjson"
    }
    data = ("\n".join(lines) + "\n").encode("utf-8")
    req = AWSRequest(method="POST", url=url, data=data, headers=headers)
    SigV4Auth(creds, SERVICE, REGION).add_auth(req)
    r = http.request("POST", url, body=data, headers=dict(req.headers))
    print("_bulk ->", r.status, r.data.decode()[:200])


ddb = boto3.resource("dynamodb", region_name=REGION)
table = ddb.Table(DDB_TABLE)


def scan_all():
    items = []
    scan_kwargs = {"ProjectionExpression": "BusinessID, Cuisine"}
    while True:
        r = table.scan(**scan_kwargs)
        items.extend(r.get("Items", []))
        if "LastEvaluatedKey" in r:
            scan_kwargs["ExclusiveStartKey"] = r["LastEvaluatedKey"]
        else:
            break
    return items


if __name__ == "__main__":
    items = scan_all()
    print("Scanned:", len(items))

    batch = []
    indexed = 0
    for it in items:
        rid = it.get("BusinessID")
        cuisine = it.get("Cuisine")
        if not rid or not cuisine:
            continue
        batch.append(json.dumps({"index": {"_index": "restaurants", "_id": str(rid)}}))
        batch.append(json.dumps(
            {"restaurant_id": str(rid), "cuisine": str(cuisine)},
            default=lambda o: float(o) if isinstance(o, Decimal) else o
        ))
        if len(batch) >= BATCH_SIZE * 2:
            es_bulk(batch)
            indexed += len(batch) // 2
            batch = []

    if batch:
        es_bulk(batch)
        indexed += len(batch) // 2

    print("Indexed docs:", indexed)
