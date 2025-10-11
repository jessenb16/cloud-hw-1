#!/usr/bin/env python3
import os
import time
import datetime
import random
from decimal import Decimal
import requests
import boto3

# ========= CONFIG =========
CUISINES = ["chinese", "japanese", "italian", "mexican", "indian"]  # at least 5 cuisines
TARGET_PER_CUISINE = 200
LOCATION = "Manhattan, NY"
TABLE_NAME = "yelp-restaurants"

# Yelp API paging limit
LIMIT = 50
MAX_OFFSET = 240 - LIMIT  # limit + offset <= 240
OFFSETS = list(range(0, MAX_OFFSET + 1, LIMIT))  # [0, 50, 100, 150, 190]
SLEEP_SEC = 0.25
RETRY_429_SLEEP = 2.0
MAX_RETRIES = 3

# Manhattan-only filter parameters
MANHATTAN_ZIP_PREFIXES = ("100", "101", "102")
LAT_MIN, LAT_MAX = 40.69, 40.88
LON_MIN, LON_MAX = -74.02, -73.91

# Region and credentials
REGION = os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION") or "us-east-1"
YELP_API_KEY = os.getenv("YELP_API_KEY")
assert YELP_API_KEY, "❌ Please set your Yelp API key: export YELP_API_KEY='your_key_here'"

# AWS setup
dynamodb = boto3.resource("dynamodb", region_name=REGION)
table = dynamodb.Table(TABLE_NAME)


# ========= HELPERS =========
def is_manhattan(biz):
    """Strictly check if a business is in Manhattan based on ZIP or coordinates."""
    loc = biz.get("location") or {}
    zip_code = (loc.get("zip_code") or "").strip()

    # ZIP-based filter: only 100xx, 101xx, 102xx
    if not zip_code.startswith(MANHATTAN_ZIP_PREFIXES):
        return False

    # Coordinate-based filter (safety check)
    coords = biz.get("coordinates") or {}
    lat, lon = coords.get("latitude"), coords.get("longitude")
    if lat is None or lon is None:
        return False
    try:
        lat = float(lat)
        lon = float(lon)
    except (TypeError, ValueError):
        return False

    # Strict Manhattan bounding box
    return (LAT_MIN <= lat <= LAT_MAX) and (LON_MIN <= lon <= LON_MAX)


def yelp_search(term, location, limit=50, offset=0):
    """Call Yelp API with retries and backoff."""
    url = "https://api.yelp.com/v3/businesses/search"
    headers = {"Authorization": f"Bearer {YELP_API_KEY}"}
    params = {
        "term": term,
        "location": location,
        "limit": limit,
        "offset": offset,
        "sort_by": "best_match",
        "radius": 40000,  # covers Manhattan region
        "categories": "restaurants"
    }

    for attempt in range(1, MAX_RETRIES + 1):
        resp = requests.get(url, headers=headers, params=params, timeout=20)
        if resp.status_code == 200:
            return resp.json()
        elif resp.status_code == 429:
            print(f"⚠️ Rate limited (attempt {attempt}) — retrying in {RETRY_429_SLEEP}s")
            time.sleep(RETRY_429_SLEEP * attempt)
        else:
            print(f"Error {resp.status_code}: {resp.text}")
            time.sleep(1)
    return {}


def to_ddb_item(biz, cuisine):
    """Convert Yelp business object to DynamoDB item."""
    loc = biz.get("location") or {}
    coords = biz.get("coordinates") or {}

    address = ", ".join(filter(None, [loc.get("address1"), loc.get("address2"), loc.get("address3")]))
    inserted_ts = datetime.datetime.utcnow().isoformat(timespec="seconds") + "Z"

    return {
        "BusinessID": biz["id"],
        "Name": biz.get("name", ""),
        "Address": address,
        "City": loc.get("city", ""),
        "State": loc.get("state", ""),
        "ZipCode": loc.get("zip_code", ""),
        "Coordinates": {
            "lat": str(coords.get("latitude", "")),
            "lon": str(coords.get("longitude", "")),
        },
        "NumReviews": Decimal(str(biz.get("review_count", 0))),
        "Rating": Decimal(str(biz.get("rating", 0.0))),
        "Cuisine": cuisine,
        "InsertedAtTimestamp": inserted_ts,
    }


def batch_write(items):
    """Write multiple items to DynamoDB efficiently."""
    if not items:
        return
    with table.batch_writer(overwrite_by_pkeys=["BusinessID"]) as batch:
        for item in items:
            batch.put_item(Item=item)


def collect_for_cuisine(cuisine):
    """Collect and store ~200 restaurants for one cuisine."""
    print(f"\n🍽 Collecting {TARGET_PER_CUISINE} {cuisine} restaurants...")
    have = {}
    total_added = 0

    for off in OFFSETS:
        term = f"{cuisine} restaurants"
        data = yelp_search(term=term, location=LOCATION, limit=LIMIT, offset=off)
        businesses = data.get("businesses") or []
        if not businesses:
            continue

        new_items = []
        for biz in businesses:
            if not is_manhattan(biz):
                continue
            bid = biz["id"]
            if bid in have:
                continue
            have[bid] = True
            new_items.append(to_ddb_item(biz, cuisine))
            total_added += 1
            if total_added >= TARGET_PER_CUISINE:
                break

        if new_items:
            batch_write(new_items)
            print(f"  ✅ Added {len(new_items)} (total {total_added})")

        if total_added >= TARGET_PER_CUISINE:
            break
        time.sleep(SLEEP_SEC)

    print(f"✅ Finished {cuisine}: {total_added} restaurants stored.")
    return total_added


def main():
    total = 0
    for cuisine in CUISINES:
        total += collect_for_cuisine(cuisine)
    print(f"\n🎉 Done! Inserted about {total} Manhattan restaurants into DynamoDB.")


if __name__ == "__main__":
    main()