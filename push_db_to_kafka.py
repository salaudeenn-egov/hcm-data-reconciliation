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


TEST_USER_KEYWORDS = ["test", "demo", "uat", "qa"]
TEST_USER_FILTER   = " OR ".join(f"i.username ILIKE '%{kw}%'" for kw in TEST_USER_KEYWORDS)

def exclude_test_users(alias, individual_table):
    return f"""NOT EXISTS (
        SELECT 1 FROM {individual_table} i
        WHERE i.useruuid = {alias}.createdby
        AND ({TEST_USER_FILTER})
    )"""


JOBS = [
    {
        "name":             "so / project_task",
        "tenant_id":        "so",
        "db_query":         f"""
            SELECT clientreferenceid
            FROM so.project_task pt
            JOIN so.project p ON pt.projectid = p.id
            WHERE pt.status = 'ADMINISTRATION_SUCCESS'
            AND (pt.isdeleted = false OR pt.isdeleted IS NULL)
            AND {exclude_test_users("pt", "so.individual")}
            AND EXISTS (
                SELECT 1 FROM jsonb_array_elements(pt.additionaldetails->'fields') f
                WHERE f->>'key' = 'doseIndex' AND f->>'value' = '01'
            )
        """,
        "es_query_body":    {
            "query": {"bool": {"filter": [
                {"terms": {"Data.administrationStatus.keyword": ["ADMINISTRATION_SUCCESS"]}},
                {"bool": {
                    "should": [{"term": {"Data.additionalDetails.doseIndex.keyword": {"value": "01"}}}],
                    "minimum_should_match": 1,
                }},
            ]}}
        },
        "es_index":         "so-project-task-index-v1",
        "es_id_field":      "Data.taskClientReferenceId",
        "kafka_topic":      "save-project-task",
        "api_base":         "http://project:8080",
        "api_search_path":  "/project/task/v1/_search",
        "api_request_key":  "Task",
        "api_response_key": "Task",
        "api_search_field": "clientReferenceId",
        "required_fields":  ["clientReferenceId", "projectBeneficiaryClientReferenceId"],
    },
]


ES_SCROLL_TIME    = "2m"
ES_BATCH_SIZE     = 1000
API_BATCH_SIZE    = 100
KAFKA_FLUSH_EVERY = 1000

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


def get_es_headers():
    username = base64.b64decode(ES_USERNAME_B64).decode()
    password = base64.b64decode(ES_PASSWORD_B64).decode()
    encoded  = base64.b64encode(f"{username}:{password}".encode()).decode()
    return {
        "Content-Type":  "application/json",
        "Authorization": f"Basic {encoded}",
    }


def fetch_db_ids(conn, query):
    ids = set()
    with conn.cursor(name="db_ids_cursor") as cur:
        cur.execute(query)
        while True:
            rows = cur.fetchmany(50000)
            if not rows:
                break
            for r in rows:
                if r[0]:
                    ids.add(str(r[0]))
    return ids


def fetch_es_ids(headers, es_index, es_query_body, es_id_field):
    ids        = set()
    field_path = tuple(es_id_field.split("."))
    query      = {**es_query_body, "_source": [es_id_field], "size": ES_BATCH_SIZE}

    res       = requests.post(
        f"{ES_BASE_URL}/{es_index}/_search?scroll={ES_SCROLL_TIME}",
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


def fetch_objects_from_api(ids, job):
    api_url = f"{job.get('api_base', API_BASE)}{job['api_search_path']}"
    params  = {
        "limit":          200,
        "offset":         0,
        "tenantId":       job["tenant_id"],
        "includeDeleted": "true",
    }
    headers = {"Content-Type": "application/json", "Accept": "application/json"}

    for i in range(0, len(ids), API_BATCH_SIZE):
        batch   = ids[i:i + API_BATCH_SIZE]
        payload = {
            "RequestInfo":          {"authToken": AUTH_TOKEN},
            job["api_request_key"]: {job["api_search_field"]: batch},
        }
        try:
            resp = requests.post(
                api_url, headers=headers, params=params, json=payload, timeout=60
            )
            resp.raise_for_status()
            yield from resp.json().get(job["api_response_key"], [])
        except Exception as e:
            print(f"\n  API batch failed (ids {i}–{i + len(batch)}): {e}")


def null_required_field(obj, required_fields):
    for field in required_fields:
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


def run_job(job, conn, producer, es_headers):
    name      = job["name"]
    ts        = datetime.now().strftime("%Y%m%d_%H%M%S")
    safe_name = name.replace(" ", "_").replace("/", "-")

    print(f"\n{'='*60}")
    print(f"  Job      : {name}")
    print(f"  Tenant   : {job['tenant_id']}")
    print(f"  ES Index : {job['es_index']}")
    print(f"  Topic    : {job['kafka_topic']}")
    print(f"  API      : {job.get('api_base', API_BASE)}{job['api_search_path']}")
    print(f"{'='*60}")

    print("\nStep 1 — Fetching IDs from database")
    try:
        db_ids = fetch_db_ids(conn, job["db_query"])
        print(f"  Found {len(db_ids):,} records in DB")
    except Exception as e:
        print(f"  Failed: {e}")
        return

    print("\nStep 2 — Fetching IDs from Elasticsearch")
    try:
        es_ids = fetch_es_ids(es_headers, job["es_index"], job["es_query_body"], job["es_id_field"])
        print(f"  Found {len(es_ids):,} records in ES")
    except Exception as e:
        print(f"  Failed: {e}")
        return

    print("\nStep 3 — Comparing")
    missing = list(db_ids - es_ids)
    print(f"  Already in sync  : {len(db_ids & es_ids):,}")
    print(f"  Missing in ES    : {len(missing):,}")

    if not missing:
        print("\n  Everything is in sync. Nothing to push.")
        return

    del db_ids, es_ids
    gc.collect()

    print(f"\nStep 4 — Fetching full records from API and pushing to Kafka")
    print(f"  Processing {len(missing):,} records in batches of {API_BATCH_SIZE} ...")

    pushed            = 0
    dropped_null      = 0
    failures          = []
    send_count        = 0
    null_field_counts = {}
    returned_ids      = set()

    def on_send_error(exc, record_id):
        failures.append({"id": record_id, "error": str(exc)})

    with tqdm(total=len(missing), unit="rec") as pbar:
        for obj in fetch_objects_from_api(missing, job):
            record_id = str(obj.get("clientReferenceId", ""))
            returned_ids.add(record_id)

            bad_field = null_required_field(obj, job["required_fields"])
            if bad_field:
                dropped_null += 1
                null_field_counts[bad_field] = null_field_counts.get(bad_field, 0) + 1
                pbar.update(1)
                continue

            try:
                payload = {
                    "RequestInfo":           REQUEST_INFO,
                    job["api_response_key"]: obj,
                }
                future = producer.send(job["kafka_topic"], key=record_id, value=payload)
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
        failed_file = f"failed_kafka_{safe_name}_{ts}.json"
        with open(failed_file, "w") as f:
            json.dump(failures, f, indent=2, default=str)

    print(f"\n{'='*60}")
    print(f"  Summary — {name}")
    print(f"{'='*60}")
    print(f"  Missing in ES        : {len(missing):,}")
    print(f"  Pushed to Kafka      : {pushed:,}")
    print(f"  Dropped (null field) : {dropped_null:,}")
    if null_field_counts:
        for field, count in null_field_counts.items():
            print(f"    └─ {field}: {count:,}")
    print(f"  Not found in API     : {not_in_api:,}")
    print(f"  Kafka send failures  : {len(failures):,}")
    if failures:
        print(f"  Failures saved to    : {failed_file}")
    print(f"{'='*60}")


def main():
    if not JOBS:
        print("No jobs configured. Add entries to the JOBS list.")
        sys.exit(0)

    print(f"\nStarting reconciliation — {len(JOBS)} job(s) to run")

    try:
        conn = psycopg2.connect(
            host=DB_HOST, port=DB_PORT, database=DB_NAME,
            user=DB_USER, password=DB_PASSWORD,
            sslmode=DB_SSLMODE, connect_timeout=DB_CONNECT_TIMEOUT,
        )
    except Exception as e:
        print(f"Could not connect to database: {e}")
        sys.exit(1)

    producer   = make_producer()
    es_headers = get_es_headers()

    try:
        for job in JOBS:
            run_job(job, conn, producer, es_headers)
    finally:
        producer.flush()
        producer.close()
        conn.close()

    print("\nAll jobs complete.")


if __name__ == "__main__":
    main()
