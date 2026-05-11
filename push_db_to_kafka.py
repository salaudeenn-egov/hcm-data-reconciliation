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

# ── CREDENTIALS (from .env) ───────────────────────────────────────────────────
DB_HOST            = os.environ["DB_HOST"]
DB_PORT            = int(os.environ.get("DB_PORT", 5432))
DB_NAME            = os.environ["DB_NAME"]
DB_USER            = os.environ["DB_USER"]
DB_PASSWORD        = os.environ["DB_PASSWORD"]
DB_SSLMODE         = os.environ.get("DB_SSLMODE", "require")
DB_CONNECT_TIMEOUT = 30

ES_BASE_URL     = os.environ["ES_BASE_URL"]
ES_USERNAME_B64 = os.environ["ES_USERNAME_B64"]
ES_PASSWORD_B64 = os.environ["ES_PASSWORD_B64"]

KAFKA_BOOTSTRAP_SERVERS = os.environ["KAFKA_BOOTSTRAP_SERVERS"]

API_BASE   = os.environ["API_BASE"]
AUTH_TOKEN = os.environ["AUTH_TOKEN"]
TENANT_ID  = os.environ["TENANT_ID"]

# ── CAMPAIGN CONFIG (edit per run) ────────────────────────────────────────────

# Central instance → schema per tenant (bo, so, ko ...)
# Individual instance → no schema prefix
IS_CENTRAL_INSTANCE = True
DB_SCHEMA           = "so"

ENTITY_TYPE = "project_task"   # project_task | stock | project_beneficiary

KAFKA_TOPIC = "save-project-task"

ES_INDEX_BASE = "project-task-index-v1"   # prefix (so-, bo-) is auto-added from DB_SCHEMA
ES_ID_FIELD   = "Data.taskClientReferenceId"

# project_task:        /project/task/v1/_search         Task              clientReferenceId   limit=200
# stock:               /stock/v1/_search                Stock             id                  limit=100
# project_beneficiary: /project/beneficiary/v1/_search  ProjectBeneficiary clientReferenceId  limit=100
API_SEARCH_PATH  = "/project/task/v1/_search"
API_REQUEST_KEY  = "Task"
API_RESPONSE_KEY = "Task"
API_SEARCH_FIELD = "clientReferenceId"
API_PARAMS       = {
    "limit":          200,
    "offset":         0,
    "tenantId":       TENANT_ID,
    "includeDeleted": "true",
}

# Drop records missing any of these fields in the API response
# project_task        → ["clientReferenceId", "projectBeneficiaryClientReferenceId"]
# stock               → ["clientReferenceId", "facilityId"]
# project_beneficiary → ["clientReferenceId", "beneficiaryClientReferenceId"]
REQUIRED_FIELDS = [
    "clientReferenceId",
    "projectBeneficiaryClientReferenceId",
]

# Records created by users with these keywords in their username are skipped
TEST_USER_KEYWORDS = ["test", "demo", "uat", "qa"]

# ── RUNTIME SETTINGS ──────────────────────────────────────────────────────────
ES_SCROLL_TIME    = "2m"
ES_BATCH_SIZE     = 1000
DB_CHUNK_SIZE     = 10000
API_BATCH_SIZE    = 100
KAFKA_FLUSH_EVERY = 1000

# ── DERIVED (auto-built from config above) ────────────────────────────────────
_s        = f"{DB_SCHEMA}." if IS_CENTRAL_INSTANCE else ""
_es       = f"{DB_SCHEMA}-" if IS_CENTRAL_INSTANCE else ""

DB_TABLE  = f"{_s}project_task"
DB_ID_COL = "clientreferenceid"
ES_INDEX  = f"{_es}{ES_INDEX_BASE}"

_test_user_filter = " OR ".join(
    f"i.username ILIKE '%{kw}%'" for kw in TEST_USER_KEYWORDS
)

DB_QUERY = f"""
SELECT {DB_ID_COL}
FROM {_s}project_task pt
JOIN {_s}project p ON pt.projectid = p.id
WHERE pt.status = 'ADMINISTRATION_SUCCESS'
AND (pt.isdeleted = false OR pt.isdeleted IS NULL)
AND NOT EXISTS (
    SELECT 1 FROM {_s}individual i
    WHERE i.useruuid = pt.createdby
    AND ({_test_user_filter})
)
AND EXISTS (
    SELECT 1
    FROM jsonb_array_elements(pt.additionaldetails->'fields') f
    WHERE f->>'key' = 'doseIndex'
      AND f->>'value' = '01'
)
"""

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

FAILED_JSON  = f"failed_kafka_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
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

# ── HELPERS ───────────────────────────────────────────────────────────────────

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
    with conn.cursor(name="db_ids_cursor") as cur:
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
            yield from resp.json().get(API_RESPONSE_KEY, [])
        except Exception as e:
            print(f"\n  API batch failed (ids {i}–{i + len(batch)}): {e}")


def null_required_field(obj):
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


# ── MAIN ──────────────────────────────────────────────────────────────────────

def main():
    print(f"\n{'='*58}")
    print(f"  Entity   : {ENTITY_TYPE}")
    print(f"  Schema   : {_s.rstrip('.') or '(none)'}")
    print(f"  Table    : {DB_TABLE}")
    print(f"  ES Index : {ES_INDEX}")
    print(f"  Topic    : {KAFKA_TOPIC}")
    print(f"  API      : {API_BASE}{API_SEARCH_PATH}")
    print(f"{'='*58}")

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

    print("\nSTEP 2 — Fetching IDs from ES")
    try:
        es_ids = fetch_es_ids(get_es_headers())
        print(f"  ES IDs : {len(es_ids):,}")
    except Exception as e:
        print(f"  ES Error: {e}")
        sys.exit(1)

    print("\nSTEP 3 — Comparing")
    missing = list(db_ids - es_ids)
    print(f"  Matched       : {len(db_ids & es_ids):,}")
    print(f"  Missing in ES : {len(missing):,}")

    if not missing:
        print("\nNothing to push. DB and ES are in sync.")
        return

    del db_ids, es_ids
    gc.collect()

    print(f"\nSTEP 4 — Fetching from HCM API + Pushing to Kafka [{KAFKA_TOPIC}]")
    print(f"  Batching {len(missing):,} IDs in groups of {API_BATCH_SIZE} ...")

    producer          = make_producer()
    pushed            = 0
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

            bad_field = null_required_field(obj)
            if bad_field:
                dropped_null += 1
                null_field_counts[bad_field] = null_field_counts.get(bad_field, 0) + 1
                pbar.update(1)
                continue

            try:
                payload = {
                    "RequestInfo":    REQUEST_INFO,
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

    not_in_api = len(set(missing) - returned_ids)

    if failures:
        with open(FAILED_JSON, "w") as f:
            json.dump(failures, f, indent=2, default=str)

    print(f"\n{'='*58}")
    print(f"  SUMMARY")
    print(f"{'='*58}")
    print(f"  Missing in ES        : {len(missing):,}")
    print(f"  Pushed to Kafka      : {pushed:,}")
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
