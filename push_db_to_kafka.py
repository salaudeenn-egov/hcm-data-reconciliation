"""
push_db_to_kafka.py
Push DB records missing from Elasticsearch directly into Kafka.
The ES indexer picks them up and re-syncs to ES.

Flow:
  1. Fetch IDs from DB        (DB_QUERY)
  2. Fetch IDs from ES        (ES_QUERY_BODY + scroll)
  3. Compute missing          (DB - ES)
  4. Fetch full objects from HCM search API (batched)
  5. Filter isDeleted + null required fields
  6. Push API response objects directly to Kafka (no manual transform)
  7. Summary

Credentials are read from environment variables / .env file.
Set these before running:
  DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD, DB_SSLMODE
  ES_BASE_URL, ES_USERNAME_B64, ES_PASSWORD_B64
  KAFKA_BOOTSTRAP_SERVERS
  API_BASE, AUTH_TOKEN, TENANT_ID

Usage:
  python push_db_to_kafka.py
"""

import base64
import gc
import json
import os
import sys
import warnings
from datetime import datetime

import psycopg2
import psycopg2.extras
import requests
from dotenv import load_dotenv
from kafka import KafkaProducer
from tqdm import tqdm

warnings.filterwarnings("ignore")
load_dotenv()

# =====================================================
# DB CONFIG — from environment variables
# =====================================================
DB_HOST            = os.environ["DB_HOST"]
DB_PORT            = int(os.environ.get("DB_PORT", 5432))
DB_NAME            = os.environ["DB_NAME"]
DB_USER            = os.environ["DB_USER"]
DB_PASSWORD        = os.environ["DB_PASSWORD"]
DB_SSLMODE         = os.environ.get("DB_SSLMODE", "require")
DB_CONNECT_TIMEOUT = 30

# =====================================================
# ES CONFIG — from environment variables
# =====================================================
ES_BASE_URL     = os.environ["ES_BASE_URL"]
ES_USERNAME_B64 = os.environ["ES_USERNAME_B64"]
ES_PASSWORD_B64 = os.environ["ES_PASSWORD_B64"]
ES_SCROLL_TIME  = "2m"
ES_BATCH_SIZE   = 1000

# =====================================================
# KAFKA CONFIG — from environment variables
# =====================================================
KAFKA_BOOTSTRAP_SERVERS = os.environ["KAFKA_BOOTSTRAP_SERVERS"]

# =====================================================
# HCM API CONFIG — from environment variables
# =====================================================
API_BASE   = os.environ["API_BASE"]    # e.g. https://mozambique-hcm.digit.org
AUTH_TOKEN = os.environ["AUTH_TOKEN"]  # OAuth token
TENANT_ID  = os.environ["TENANT_ID"]  # e.g. mz, ko, bo

# =====================================================
# ENTITY CONFIG — update per campaign / category
# (not secrets — edit directly in this file)
# =====================================================
ENTITY_TYPE = "project_task"            # project_task | stock | project_beneficiary

DB_TABLE    = "bo.project_task"         # update schema prefix per campaign e.g. ko, bo, oy
DB_ID_COL   = "clientreferenceid"

KAFKA_TOPIC = "save-project-task"       # configurable — update per entity

ES_INDEX    = "bo-project-task-index-v1"
ES_ID_FIELD = "Data.taskClientReferenceId"   # dot-path to ID field in ES _source

# HCM API search endpoint and payload keys — update per entity
#
# project_task:
#   API_SEARCH_PATH  = "/project/task/v1/_search"
#   API_REQUEST_KEY  = "Task"
#   API_RESPONSE_KEY = "Task"
#   API_SEARCH_FIELD = "clientReferenceId"
#   API_PARAMS       = {"limit": 200, "offset": 0, "tenantId": TENANT_ID, "includeDeleted": "true"}
#
# stock:
#   API_SEARCH_PATH  = "/stock/v1/_search"
#   API_REQUEST_KEY  = "Stock"
#   API_RESPONSE_KEY = "Stock"
#   API_SEARCH_FIELD = "id"
#   API_PARAMS       = {"limit": 100, "offset": 0, "tenantId": TENANT_ID}
#
# project_beneficiary:
#   API_SEARCH_PATH  = "/project/beneficiary/v1/_search"
#   API_REQUEST_KEY  = "ProjectBeneficiary"
#   API_RESPONSE_KEY = "ProjectBeneficiary"
#   API_SEARCH_FIELD = "clientReferenceId"
#   API_PARAMS       = {"limit": 100, "offset": 0, "tenantId": TENANT_ID}

API_SEARCH_PATH  = "/project/task/v1/_search"
API_REQUEST_KEY  = "Task"
API_RESPONSE_KEY = "Task"
API_SEARCH_FIELD = "clientReferenceId"   # field used inside request body to search by
API_PARAMS       = {                     # query params — update per entity
    "limit":          200,
    "offset":         0,
    "tenantId":       TENANT_ID,
    "includeDeleted": "true",
}

# Fields that must be non-null in the API response before pushing to Kafka
# project_task        → ["clientReferenceId", "projectBeneficiaryClientReferenceId"]
# stock               → ["clientReferenceId", "facilityId"]
# project_beneficiary → ["clientReferenceId", "beneficiaryClientReferenceId"]
REQUIRED_FIELDS = [
    "clientReferenceId",
    "projectBeneficiaryClientReferenceId",
]

# =====================================================
# DB QUERY — update per campaign / category
# =====================================================
DB_QUERY = f"""
SELECT {DB_ID_COL}
FROM bo.project_task pt
JOIN bo.project p ON pt.projectid = p.id
WHERE pt.status = 'ADMINISTRATION_SUCCESS'
AND EXISTS (
    SELECT 1
    FROM jsonb_array_elements(pt.additionaldetails->'fields') f
    WHERE f->>'key' = 'doseIndex'
      AND f->>'value' = '01'
)
"""

# =====================================================
# ES QUERY BODY — update per campaign / category
# =====================================================
ES_QUERY_BODY = {
    "query": {
        "bool": {
            "filter": [
                {
                    "terms": {
                        "Data.administrationStatus.keyword": ["ADMINISTRATION_SUCCESS"]
                    }
                },
                {
                    "bool": {
                        "should": [
                            {
                                "term": {
                                    "Data.additionalDetails.doseIndex.keyword": {"value": "01"}
                                }
                            }
                        ],
                        "minimum_should_match": 1
                    }
                }
            ]
        }
    }
}

# =====================================================
# RUNTIME SETTINGS
# =====================================================
DB_CHUNK_SIZE     = 10000   # IDs per DB IN query
API_BATCH_SIZE    = 100     # IDs per HCM API search call
KAFKA_FLUSH_EVERY = 1000    # flush Kafka producer every N messages
FAILED_JSON       = f"failed_kafka_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"

REQUEST_INFO = {
    "apiId":     "Rainmaker",
    "ver":       ".01",
    "ts":        None,
    "action":    "_create",
    "did":       "1",
    "key":       "",
    "msgId":     "reindex",
    "authToken": AUTH_TOKEN,
    "userInfo":  None,
}

# =====================================================
# HELPERS
# =====================================================

def get_es_headers():
    username = base64.b64decode(ES_USERNAME_B64).decode()
    password = base64.b64decode(ES_PASSWORD_B64).decode()
    encoded  = base64.b64encode(f"{username}:{password}".encode()).decode()
    return {
        "Content-Type": "application/json",
        "Authorization": f"Basic {encoded}",
    }


def fetch_db_ids(conn):
    ids = set()
    with conn.cursor(name="db_ids_cursor") as cur:   # server-side cursor — streams rows
        cur.execute(DB_QUERY)
        while True:
            rows = cur.fetchmany(50000)
            if not rows:
                break
            for r in rows:
                if r[0]:
                    ids.add(str(r[0]))
    return ids


def fetch_es_ids(headers):
    ids        = set()
    field_path = tuple(ES_ID_FIELD.split("."))
    query      = {**ES_QUERY_BODY, "_source": [ES_ID_FIELD], "size": ES_BATCH_SIZE}

    res       = requests.post(
        f"{ES_BASE_URL}/{ES_INDEX}/_search?scroll={ES_SCROLL_TIME}",
        headers=headers, data=json.dumps(query), verify=False,
    )
    data      = res.json()
    scroll_id = data["_scroll_id"]
    hits      = data["hits"]["hits"]

    while hits:
        for hit in hits:
            val = hit.get("_source", {})
            for key in field_path:
                val = val.get(key, {}) if isinstance(val, dict) else None
            if val:
                ids.add(str(val))

        res       = requests.post(
            f"{ES_BASE_URL}/_search/scroll",
            headers=headers,
            data=json.dumps({"scroll": ES_SCROLL_TIME, "scroll_id": scroll_id}),
            verify=False,
        )
        data      = res.json()
        hits      = data["hits"]["hits"]
        scroll_id = data["_scroll_id"]

    return ids


def fetch_objects_from_api(ids):
    """Call HCM search API in batches, yield individual objects."""
    api_url = f"{API_BASE}{API_SEARCH_PATH}"
    headers = {"Content-Type": "application/json", "Accept": "application/json"}

    for i in range(0, len(ids), API_BATCH_SIZE):
        batch   = ids[i:i + API_BATCH_SIZE]
        payload = {
            "RequestInfo":   {"authToken": AUTH_TOKEN},
            API_REQUEST_KEY: {API_SEARCH_FIELD: batch},
        }
        try:
            resp = requests.post(
                api_url, headers=headers, params=API_PARAMS, json=payload, timeout=60
            )
            resp.raise_for_status()
            objects = resp.json().get(API_RESPONSE_KEY, [])
            yield from objects
        except Exception as e:
            print(f"\n  API batch failed (ids {i}–{i + len(batch)}): {e}")


def is_deleted(obj):
    return str(obj.get("isDeleted", "false")).lower() in ("true", "1")


def null_required_field(obj):
    """Return the first required field that is null/empty, or None if all present."""
    for field in REQUIRED_FIELDS:
        val = obj.get(field)
        if val is None or str(val).strip() in ("", "None", "nan"):
            return field
    return None


def make_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        security_protocol="PLAINTEXT",
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        retries=3,
        acks="all",
    )


# =====================================================
# MAIN
# =====================================================

def main():
    print(f"\n{'='*58}")
    print(f"  Entity   : {ENTITY_TYPE}")
    print(f"  Table    : {DB_TABLE}")
    print(f"  ES Index : {ES_INDEX}")
    print(f"  Topic    : {KAFKA_TOPIC}")
    print(f"  API      : {API_BASE}{API_SEARCH_PATH}")
    print(f"{'='*58}")

    # STEP 1 — DB IDs
    print("\nSTEP 1 — Fetching IDs from DB")
    try:
        conn   = psycopg2.connect(
            host=DB_HOST, port=DB_PORT, database=DB_NAME,
            user=DB_USER, password=DB_PASSWORD,
            sslmode=DB_SSLMODE, connect_timeout=DB_CONNECT_TIMEOUT,
        )
        db_ids = fetch_db_ids(conn)
        conn.close()
        print(f"  DB IDs : {len(db_ids):,}")
    except Exception as e:
        print(f"  DB Error: {e}")
        sys.exit(1)

    # STEP 2 — ES IDs
    print("\nSTEP 2 — Fetching IDs from ES")
    try:
        es_ids = fetch_es_ids(get_es_headers())
        print(f"  ES IDs : {len(es_ids):,}")
    except Exception as e:
        print(f"  ES Error: {e}")
        sys.exit(1)

    # STEP 3 — Compare
    print("\nSTEP 3 — Comparing")
    missing = list(db_ids - es_ids)
    print(f"  Matched       : {len(db_ids & es_ids):,}")
    print(f"  Missing in ES : {len(missing):,}")

    if not missing:
        print("\nNothing to push. DB and ES are in sync.")
        return

    del db_ids, es_ids
    gc.collect()

    # STEP 4 — Fetch from HCM API + push to Kafka
    print(f"\nSTEP 4 — Fetching from HCM API + Pushing to Kafka [{KAFKA_TOPIC}]")
    print(f"  Batching {len(missing):,} IDs in groups of {API_BATCH_SIZE} ...")

    producer          = make_producer()
    pushed            = 0
    dropped_deleted   = 0
    dropped_null      = 0
    failures          = []
    send_count        = 0
    null_field_counts = {}
    returned_ids      = set()

    def on_send_error(exc, record_id):
        failures.append({"id": record_id, "error": str(exc)})

    with tqdm(total=len(missing), unit="rec") as pbar:
        for obj in fetch_objects_from_api(missing):
            record_id = str(obj.get("clientReferenceId", ""))
            returned_ids.add(record_id)

            # filter isDeleted
            if is_deleted(obj):
                dropped_deleted += 1
                pbar.update(1)
                continue

            # filter null required fields
            bad_field = null_required_field(obj)
            if bad_field:
                dropped_null += 1
                null_field_counts[bad_field] = null_field_counts.get(bad_field, 0) + 1
                pbar.update(1)
                continue

            try:
                payload = {
                    "RequestInfo":   REQUEST_INFO,
                    API_RESPONSE_KEY: obj,
                }
                future = producer.send(KAFKA_TOPIC, key=record_id, value=payload)
                future.add_errback(lambda exc, rid=record_id: on_send_error(exc, rid))
                pushed     += 1
                send_count += 1
            except Exception as e:
                failures.append({"id": record_id, "error": str(e)})

            if send_count % KAFKA_FLUSH_EVERY == 0:
                producer.flush()

            pbar.update(1)

    producer.flush()

    # IDs in missing list but not returned by API at all
    not_in_api = len(set(missing) - returned_ids)

    # STEP 5 — Save failures
    if failures:
        with open(FAILED_JSON, "w") as f:
            json.dump(failures, f, indent=2, default=str)

    # STEP 6 — Summary
    print(f"\n{'='*58}")
    print(f"  SUMMARY")
    print(f"{'='*58}")
    print(f"  Missing in ES        : {len(missing):,}")
    print(f"  Pushed to Kafka      : {pushed:,}")
    print(f"  Dropped (deleted)    : {dropped_deleted:,}")
    print(f"  Dropped (null field) : {dropped_null:,}")
    if null_field_counts:
        for field, count in null_field_counts.items():
            print(f"    └─ {field}: {count:,}")
    print(f"  Not found in API     : {not_in_api:,}")
    print(f"  Kafka failed         : {len(failures):,}")
    if failures:
        print(f"  Failures saved       : {FAILED_JSON}")
    print(f"{'='*58}")


if __name__ == "__main__":
    main()
