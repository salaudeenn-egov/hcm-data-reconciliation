import base64
import gc
import json
import logging
import os
import sys
import time
import uuid
import warnings
from datetime import datetime

import psycopg2
import requests
from dotenv import load_dotenv
from kafka import KafkaProducer
from urllib3.exceptions import InsecureRequestWarning

warnings.filterwarnings("ignore", category=InsecureRequestWarning)
load_dotenv()

# ─────────────────────────────────────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────────────────────────────────────

os.makedirs("logs", exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(f"logs/push_db_to_kafka_{datetime.now().strftime('%Y%m%d')}.log"),
        logging.StreamHandler(),
    ]
)

log = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# CREDENTIALS
# ─────────────────────────────────────────────────────────────────────────────

_REQUIRED_VARS = [
    "DB_HOST", "DB_NAME", "DB_USER", "DB_PASSWORD",
    "ES_BASE_URL", "ES_USERNAME_B64", "ES_PASSWORD_B64",
    "KAFKA_BOOTSTRAP_SERVERS", "AUTH_TOKEN",
]
_missing_vars = [v for v in _REQUIRED_VARS if not os.environ.get(v)]
if _missing_vars:
    raise SystemExit(f"Missing required env vars: {', '.join(_missing_vars)}")

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
AUTH_TOKEN              = os.environ["AUTH_TOKEN"]

# ─────────────────────────────────────────────────────────────────────────────
# CAMPAIGN CONFIG
# ─────────────────────────────────────────────────────────────────────────────

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
        "api_base":         "http://project.egov:8080",
        "api_search_path":  "/project/task/v1/_search",
        "api_request_key":  "Task",
        "api_response_key": "Tasks",
        "api_search_field": "clientReferenceId",
        "required_fields":  ["clientReferenceId", "projectBeneficiaryClientReferenceId"],
    },
]

# ─────────────────────────────────────────────────────────────────────────────
# RUNTIME SETTINGS
# ─────────────────────────────────────────────────────────────────────────────

ES_SCROLL_TIME    = "5m"
ES_BATCH_SIZE     = 5000
API_BATCH_SIZE    = 100
API_RETRIES       = 3
KAFKA_FLUSH_EVERY = 1000

# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _decode_cred(val):
    try:
        return base64.b64decode(val).decode()
    except Exception:
        return val


def get_es_headers():
    username = _decode_cred(ES_USERNAME_B64)
    password = _decode_cred(ES_PASSWORD_B64)
    encoded  = base64.b64encode(f"{username}:{password}".encode()).decode()
    return {
        "Content-Type":  "application/json",
        "Authorization": f"Basic {encoded}",
    }


def fetch_db_ids(conn, query):
    ids         = set()
    cursor_name = f"db_cur_{uuid.uuid4().hex[:12]}"
    conn.autocommit = False
    with conn.cursor(name=cursor_name) as cur:
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
    scroll_id  = None

    try:
        res = requests.post(
            f"{ES_BASE_URL}/{es_index}/_search?scroll={ES_SCROLL_TIME}",
            headers=headers, data=json.dumps(query), verify=False,
        )
        res.raise_for_status()
        data = res.json()
        if "error" in data:
            raise RuntimeError(f"ES error: {data['error']}")

        scroll_id = data["_scroll_id"]
        hits      = data["hits"]["hits"]
        batch_num = 0

        while hits:
            for hit in hits:
                val = hit.get("_source", {})
                for key in field_path:
                    val = val.get(key, {}) if isinstance(val, dict) else None
                if val:
                    ids.add(str(val))

            batch_num += 1
            if batch_num % 10 == 0:
                log.info("  ES scroll: %d batches / ~%d ids so far", batch_num, len(ids))

            res = requests.post(
                f"{ES_BASE_URL}/_search/scroll",
                headers=headers,
                data=json.dumps({"scroll": ES_SCROLL_TIME, "scroll_id": scroll_id}),
                verify=False,
            )
            res.raise_for_status()
            data = res.json()
            if "error" in data:
                raise RuntimeError(f"ES scroll error: {data['error']}")
            hits      = data["hits"]["hits"]
            scroll_id = data.get("_scroll_id", scroll_id)

    finally:
        if scroll_id:
            try:
                requests.delete(
                    f"{ES_BASE_URL}/_search/scroll",
                    headers=headers,
                    json={"scroll_id": scroll_id},
                    verify=False,
                )
            except Exception:
                pass

    return ids


def fetch_objects_from_api(ids, job):
    """Returns (objects, failed_batches). failed_batches contains IDs that could not be fetched after retries."""
    api_url = f"{job['api_base']}{job['api_search_path']}"
    params  = {"limit": 200, "offset": 0, "tenantId": job["tenant_id"]}
    headers = {"Content-Type": "application/json", "Accept": "application/json"}

    objects        = []
    failed_batches = []
    total          = len(ids)

    for i in range(0, total, API_BATCH_SIZE):
        batch   = ids[i:i + API_BATCH_SIZE]
        payload = {
            "RequestInfo":          {"authToken": AUTH_TOKEN},
            job["api_request_key"]: {job["api_search_field"]: batch},
        }
        for attempt in range(API_RETRIES):
            try:
                resp = requests.post(
                    api_url, headers=headers, params=params, json=payload, timeout=60
                )
                resp.raise_for_status()
                data = resp.json()
                objects.extend(data.get(job["api_response_key"], []))
                break
            except Exception as e:
                if attempt == API_RETRIES - 1:
                    log.error("API batch failed after %d attempts (offset %d): %s", API_RETRIES, i, e)
                    failed_batches.append({"offset": i, "ids": batch, "error": str(e)})
                else:
                    wait = 2 ** attempt
                    log.warning("  API attempt %d/%d failed, retrying in %ds: %s",
                                attempt + 1, API_RETRIES, wait, e)
                    time.sleep(wait)

        if (i // API_BATCH_SIZE + 1) % 10 == 0:
            log.info("  API fetch: %d / %d done", min(i + API_BATCH_SIZE, total), total)

    return objects, failed_batches


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
        enable_idempotence=True,
        acks="all",
        retries=10,
        max_in_flight_requests_per_connection=1,
        linger_ms=20,
        batch_size=65536,
    )

# ─────────────────────────────────────────────────────────────────────────────
# JOB
# ─────────────────────────────────────────────────────────────────────────────

def run_job(job, conn, producer, es_headers):
    name      = job["name"]
    ts        = datetime.now().strftime("%Y%m%d_%H%M%S")
    safe_name = name.replace(" ", "_").replace("/", "-")

    log.info("Job started: %s | tenant=%s | index=%s | topic=%s",
             name, job["tenant_id"], job["es_index"], job["kafka_topic"])

    try:
        db_ids = fetch_db_ids(conn, job["db_query"])
        log.info("DB: %d records", len(db_ids))
    except Exception as e:
        log.error("DB fetch failed: %s", e)
        return True  # has_failures

    try:
        es_ids = fetch_es_ids(es_headers, job["es_index"], job["es_query_body"], job["es_id_field"])
        log.info("ES: %d records", len(es_ids))
    except Exception as e:
        log.error("ES fetch failed: %s", e)
        return True  # has_failures

    missing = list(db_ids - es_ids)
    log.info("Matched: %d | Missing in ES: %d", len(db_ids & es_ids), len(missing))

    if not missing:
        log.info("In sync. Nothing to push.")
        return False

    del db_ids, es_ids
    gc.collect()

    log.info("Fetching %d records from API and pushing to Kafka (batch=%d)", len(missing), API_BATCH_SIZE)

    objects, api_failures = fetch_objects_from_api(missing, job)

    if api_failures:
        failed_api_file = f"failed_api_{safe_name}_{ts}.json"
        with open(failed_api_file, "w") as f:
            json.dump(api_failures, f, indent=2, default=str)
        total_api_failed_ids = sum(len(fb["ids"]) for fb in api_failures)
        for fb in api_failures:
            log.warning("API_FAILED offset=%d ids=%s", fb["offset"], ",".join(fb["ids"][:3]))
        log.warning("API failures: %d batches / %d IDs — saved to %s",
                    len(api_failures), total_api_failed_ids, failed_api_file)

    pushed            = 0
    dropped_null      = 0
    send_count        = 0
    kafka_failures    = []
    null_field_counts = {}
    returned_ids      = set()

    def on_send_error(exc, record_id):
        kafka_failures.append({"id": record_id, "error": str(exc)})
        log.warning("KAFKA_FAILED id=%s error=%s", record_id, exc)

    for obj in objects:
        record_id = str(obj.get(job["api_search_field"], ""))
        if record_id:
            returned_ids.add(record_id)

        bad_field = null_required_field(obj, job["required_fields"])
        if bad_field:
            dropped_null += 1
            null_field_counts[bad_field] = null_field_counts.get(bad_field, 0) + 1
            continue

        try:
            future = producer.send(job["kafka_topic"], key=record_id, value=[obj])
            future.add_errback(lambda exc, rid=record_id: on_send_error(exc, rid))
            pushed     += 1
            send_count += 1
        except Exception as e:
            kafka_failures.append({"id": record_id, "error": str(e)})
            log.warning("KAFKA_FAILED id=%s error=%s", record_id, e)

        if send_count % KAFKA_FLUSH_EVERY == 0:
            producer.flush(timeout=300)

    producer.flush(timeout=300)

    not_in_api = len(set(missing) - returned_ids)

    if kafka_failures:
        failed_kafka_file = f"failed_kafka_{safe_name}_{ts}.json"
        with open(failed_kafka_file, "w") as f:
            json.dump(kafka_failures, f, indent=2, default=str)
        log.warning("Kafka failures: %d — saved to %s", len(kafka_failures), failed_kafka_file)

    if null_field_counts:
        for field, count in null_field_counts.items():
            log.warning("Dropped null field '%s': %d records", field, count)

    has_failures = bool(api_failures or kafka_failures)

    log.info(
        "Done | Missing: %d | Fetched: %d | Dropped: %d | Not in API: %d | Pushed: %d | "
        "API failures: %d | Kafka failures: %d",
        len(missing), len(returned_ids), dropped_null, not_in_api, pushed,
        sum(len(fb["ids"]) for fb in api_failures), len(kafka_failures),
    )

    return has_failures

# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def main():
    if not JOBS:
        log.error("No jobs configured.")
        sys.exit(0)

    log.info("Reconciliation started — %d job(s)", len(JOBS))

    try:
        conn = psycopg2.connect(
            host=DB_HOST, port=DB_PORT, database=DB_NAME,
            user=DB_USER, password=DB_PASSWORD,
            sslmode=DB_SSLMODE, connect_timeout=DB_CONNECT_TIMEOUT,
        )
        conn.autocommit = False
    except Exception as e:
        log.error("DB connection failed: %s", e)
        sys.exit(1)

    producer   = make_producer()
    es_headers = get_es_headers()
    any_failures = False

    try:
        for job in JOBS:
            job_failed = run_job(job, conn, producer, es_headers)
            if job_failed:
                any_failures = True
    finally:
        producer.flush(timeout=300)
        producer.close()
        conn.close()

    log.info("Reconciliation complete")
    sys.exit(1 if any_failures else 0)


if __name__ == "__main__":
    main()
