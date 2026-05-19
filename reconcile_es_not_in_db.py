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
        logging.FileHandler(f"logs/reconcile_es_not_in_db_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# CREDENTIALS  (env vars only — same as push_db_to_kafka.py)
# ─────────────────────────────────────────────────────────────────────────────

_REQUIRED = [
    "DB_HOST", "DB_NAME", "DB_USER", "DB_PASSWORD",
    "ES_BASE_URL", "ES_USERNAME_B64", "ES_PASSWORD_B64",
    "AUTH_TOKEN",
]
_missing = [v for v in _REQUIRED if not os.environ.get(v)]
if _missing:
    raise SystemExit(f"Missing required env vars: {', '.join(_missing)}")

DB_HOST     = os.environ["DB_HOST"]
DB_PORT     = int(os.environ.get("DB_PORT", 5432))
DB_NAME     = os.environ["DB_NAME"]
DB_USER     = os.environ["DB_USER"]
DB_PASSWORD = os.environ["DB_PASSWORD"]
DB_SSLMODE  = os.environ.get("DB_SSLMODE", "require")

ES_BASE_URL     = os.environ["ES_BASE_URL"]
ES_USERNAME_B64 = os.environ["ES_USERNAME_B64"]
ES_PASSWORD_B64 = os.environ["ES_PASSWORD_B64"]

AUTH_TOKEN = os.environ["AUTH_TOKEN"]

# ─────────────────────────────────────────────────────────────────────────────
# TRAINING KEYWORDS
# ─────────────────────────────────────────────────────────────────────────────

TRAINING_KEYWORDS = ["-tr-", "training", "demo"]

# ─────────────────────────────────────────────────────────────────────────────
# JOBS  — set enabled=True on the job(s) you want to run
# ─────────────────────────────────────────────────────────────────────────────

JOBS = [
    {
        "name":       "oy / project_task",
        "enabled":    False,
        "tenant_id":  "oy",
        "db_query": """
            SELECT clientreferenceid
            FROM oy.project_task
            WHERE (isdeleted = false OR isdeleted IS NULL)
        """,
        "es_index":      "oy-project-task-index-v1",
        "es_id_field":   "Data.taskClientReferenceId",
        "es_query_body": {"query": {"match_all": {}}},
        "es_data_path":  "Data.task",
        "training_fields": ["Data.userName", "Data.nameOfUser"],
        "beneficiary_check": {
            "field":    "projectBeneficiaryClientReferenceId",
            "db_query": "SELECT clientreferenceid FROM oy.project_beneficiary WHERE isdeleted = false",
        },
        "api_base":               "https://oyo-hcm.digit.org",
        "api_create_path":        "/project/task/v1/bulk/_create",
        "api_create_request_key": "Tasks",
    },
    {
        "name":       "oy / household",
        "enabled":    False,
        "tenant_id":  "oy",
        "db_query": """
            SELECT clientreferenceid
            FROM oy.household
            WHERE (isdeleted = false OR isdeleted IS NULL)
        """,
        "es_index":        "oy-household-index-v1",
        "es_id_field":     "_id",
        "es_query_body":   {"query": {"match_all": {}}},
        "es_data_path":    "Data.household",
        "training_fields": ["Data.userName", "Data.nameOfUser"],
        "api_base":               "https://oyo-hcm.digit.org",
        "api_create_path":        "/household/v1/bulk/_create",
        "api_create_request_key": "Households",
    },
    {
        "name":       "oy / household_member",
        "enabled":    False,
        "tenant_id":  "oy",
        "db_query": """
            SELECT hm.clientreferenceid
            FROM oy.household_member hm
            JOIN oy.household h ON h.clientreferenceid = hm.householdclientreferenceid
            WHERE EXISTS (
                SELECT 1 FROM oy.address a
                WHERE a.id = h.addressid
                AND EXISTS (SELECT 1 FROM oy.project p WHERE p.referenceid = a.localitycode)
            )
            AND hm.isdeleted = false
        """,
        "es_index":        "oy-household-member-index-v1",
        "es_id_field":     "_id",
        "es_query_body":   {"query": {"match_all": {}}},
        "es_data_path":    "Data.householdMember",
        "training_fields": ["Data.userName", "Data.nameOfUser"],
        "api_base":               "https://oyo-hcm.digit.org",
        "api_create_path":        "/household/member/v1/bulk/_create",
        "api_create_request_key": "HouseholdMembers",
    },
    {
        "name":       "oy / project_beneficiary",
        "enabled":    False,
        "tenant_id":  "oy",
        "db_query": """
            SELECT clientreferenceid
            FROM oy.project_beneficiary
            WHERE isdeleted = false
        """,
        "es_index":        "oy-project-beneficiary-index-v1",
        "es_id_field":     "clientReferenceId",
        "es_query_body":   {"query": {"match_all": {}}},
        "es_data_path":    "",
        "training_fields": [],
        "api_base":               "https://oyo-hcm.digit.org",
        "api_create_path":        "/project/beneficiary/v1/bulk/_create",
        "api_create_request_key": "ProjectBeneficiaries",
    },
]

ES_SCROLL_TIME = "5m"
ES_BATCH_SIZE  = 5000
API_BATCH_SIZE = 50
API_RETRIES    = 3

# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _get_es_headers():
    u   = base64.b64decode(ES_USERNAME_B64).decode()
    p   = base64.b64decode(ES_PASSWORD_B64).decode()
    enc = base64.b64encode(f"{u}:{p}".encode()).decode()
    return {"Content-Type": "application/json", "Authorization": f"Basic {enc}"}


def _dot_get(obj, path):
    """Extract a value from a nested dict using a dot-separated path."""
    val = obj
    for key in path.split("."):
        if not isinstance(val, dict):
            return None
        val = val.get(key)
    return val

# ─────────────────────────────────────────────────────────────────────────────
# DB
# ─────────────────────────────────────────────────────────────────────────────

def fetch_db_ids(conn, query):
    ids         = set()
    cursor_name = f"rc_cur_{uuid.uuid4().hex[:12]}"
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

# ─────────────────────────────────────────────────────────────────────────────
# ES
# ─────────────────────────────────────────────────────────────────────────────

def fetch_es_ids(es_headers, index, query_body, id_field):
    ids       = set()
    use_id    = (id_field == "_id")
    source    = False if use_id else [id_field]
    query     = {**query_body, "_source": source, "size": ES_BATCH_SIZE}
    scroll_id = None

    try:
        res = requests.post(
            f"{ES_BASE_URL}/{index}/_search?scroll={ES_SCROLL_TIME}",
            headers=es_headers, json=query, verify=False, timeout=60,
        )
        res.raise_for_status()
        data      = res.json()
        scroll_id = data["_scroll_id"]
        hits      = data["hits"]["hits"]
        batch_num = 0

        while hits:
            for hit in hits:
                val = hit["_id"] if use_id else _dot_get(hit.get("_source", {}), id_field)
                if val:
                    ids.add(str(val))
            batch_num += 1
            if batch_num % 10 == 0:
                log.info("  ES scroll: batch %d / ~%d ids", batch_num, len(ids))

            res = requests.post(
                f"{ES_BASE_URL}/_search/scroll",
                headers=es_headers,
                json={"scroll": ES_SCROLL_TIME, "scroll_id": scroll_id},
                verify=False, timeout=60,
            )
            res.raise_for_status()
            data      = res.json()
            hits      = data["hits"]["hits"]
            scroll_id = data.get("_scroll_id", scroll_id)

    finally:
        if scroll_id:
            try:
                requests.delete(
                    f"{ES_BASE_URL}/_search/scroll",
                    headers=es_headers, json={"scroll_id": scroll_id},
                    verify=False, timeout=10,
                )
            except Exception:
                pass

    return ids


def fetch_es_docs_for_ids(es_headers, index, id_field, ids, batch_size=500):
    """Fetch full ES documents for a specific set of IDs."""
    docs     = []
    use_id   = (id_field == "_id")
    id_list  = list(ids)

    for i in range(0, len(id_list), batch_size):
        batch = id_list[i: i + batch_size]
        if use_id:
            terms_query = {"terms": {"_id": batch}}
        else:
            terms_query = {"terms": {f"{id_field}.keyword": batch}}

        try:
            res = requests.post(
                f"{ES_BASE_URL}/{index}/_search",
                headers=es_headers,
                json={"query": terms_query, "size": batch_size},
                verify=False, timeout=60,
            )
            res.raise_for_status()
            hits = res.json().get("hits", {}).get("hits", [])
            docs.extend(hits)
            log.info(
                "  ES doc fetch: %d/%d done (batch got %d)",
                min(i + batch_size, len(id_list)), len(id_list), len(hits),
            )
        except Exception as e:
            log.error("  ES doc fetch offset=%d failed: %s", i, e)

    return docs

# ─────────────────────────────────────────────────────────────────────────────
# TRAINING FILTER
# ─────────────────────────────────────────────────────────────────────────────

def is_training(source, training_fields):
    """Return True if any training_fields value contains any TRAINING_KEYWORD."""
    if not TRAINING_KEYWORDS or not training_fields:
        return False
    for field_path in training_fields:
        val = str(_dot_get(source, field_path) or "").lower()
        if any(kw in val for kw in TRAINING_KEYWORDS):
            return True
    return False

# ─────────────────────────────────────────────────────────────────────────────
# API
# ─────────────────────────────────────────────────────────────────────────────

_SESSION = requests.Session()
_SESSION.headers.update({"Content-Type": "application/json"})


def bulk_create(objects, job, batch_size=API_BATCH_SIZE):
    url    = f"{job['api_base']}{job['api_create_path']}"
    params = {"tenantId": job["tenant_id"]}
    req_info = {"authToken": AUTH_TOKEN}
    success = failed = 0

    for i in range(0, len(objects), batch_size):
        batch = objects[i: i + batch_size]
        try:
            resp = _SESSION.post(
                url, params=params,
                json={"RequestInfo": req_info, job["api_create_request_key"]: batch},
                timeout=60, verify=False,
            )
            data = resp.json()
            ok = resp.status_code in (200, 202) and (
                data.get("ResponseInfo", {}).get("status") == "successful"
                or data.get("status") == "successful"
            )
            if ok:
                log.info(
                    "  bulk CREATE [%d/%d] SUCCESS count=%d",
                    min(i + batch_size, len(objects)), len(objects), len(batch),
                )
                success += len(batch)
            else:
                log.error(
                    "  bulk CREATE [%d/%d] FAILED status=%d | %s",
                    min(i + batch_size, len(objects)), len(objects),
                    resp.status_code, json.dumps(data)[:400],
                )
                failed += len(batch)
        except Exception as e:
            log.error("  bulk CREATE [%d/%d] ERROR %s", min(i + batch_size, len(objects)), len(objects), e)
            failed += len(batch)
        time.sleep(0.2)

    return success, failed

# ─────────────────────────────────────────────────────────────────────────────
# JOB
# ─────────────────────────────────────────────────────────────────────────────

def run_job(job, conn, es_headers):
    name = job["name"]
    log.info("=" * 60)
    log.info("JOB: %s | tenant=%s | index=%s", name, job["tenant_id"], job["es_index"])

    # 1. DB IDs
    try:
        db_ids = fetch_db_ids(conn, job["db_query"])
        log.info("[%s] DB: %d records", name, len(db_ids))
    except Exception as e:
        log.error("[%s] DB fetch failed: %s", name, e)
        return True

    # 2. ES IDs
    try:
        es_ids = fetch_es_ids(
            es_headers, job["es_index"],
            job.get("es_query_body", {"query": {"match_all": {}}}),
            job["es_id_field"],
        )
        log.info("[%s] ES: %d records", name, len(es_ids))
    except Exception as e:
        log.error("[%s] ES fetch failed: %s", name, e)
        return True

    # 3. Find missing in DB
    missing = list(es_ids - db_ids)
    log.info("[%s] Matched: %d | Missing in DB: %d", name, len(db_ids & es_ids), len(missing))

    if not missing:
        log.info("[%s] ES and DB are in sync. Nothing to create.", name)
        return False

    del db_ids, es_ids
    gc.collect()

    # 4. Fetch full ES docs for missing IDs
    log.info("[%s] Fetching %d full ES docs ...", name, len(missing))
    hits = fetch_es_docs_for_ids(es_headers, job["es_index"], job["es_id_field"], missing)
    log.info("[%s] ES docs fetched: %d", name, len(hits))

    # 5. Filter training records
    training_fields = job.get("training_fields", [])
    valid_hits, skipped = [], 0
    for hit in hits:
        if is_training(hit.get("_source", {}), training_fields):
            skipped += 1
        else:
            valid_hits.append(hit)
    log.info("[%s] Training skipped: %d | Valid: %d", name, skipped, len(valid_hits))

    # 6. Beneficiary check (optional — used by project_task)
    bc = job.get("beneficiary_check")
    if bc and bc.get("field") and bc.get("db_query"):
        try:
            ben_ids = fetch_db_ids(conn, bc["db_query"])
            log.info("[%s] Beneficiary IDs loaded: %d", name, len(ben_ids))
            field     = bc["field"]
            data_path = job.get("es_data_path", "")
            after, skipped_ben, missing_refs = [], 0, []
            for hit in valid_hits:
                source = hit.get("_source", {})
                entity = _dot_get(source, data_path) if data_path else source
                ref    = (entity or {}).get(field) if isinstance(entity, dict) else None
                if ref and str(ref).strip() in ben_ids:
                    after.append(hit)
                else:
                    skipped_ben += 1
                    missing_refs.append(ref or "?")
            log.info("[%s] Skipped (no beneficiary): %d | After check: %d", name, skipped_ben, len(after))
            if missing_refs:
                out = f"logs/missing_beneficiaries_{name.replace(' ','_').replace('/','-')}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
                with open(out, "w") as fh:
                    fh.write("\n".join(str(x) for x in missing_refs))
                log.info("[%s] Missing beneficiary refs saved → %s", name, out)
            valid_hits = after
        except Exception as e:
            log.error("[%s] Beneficiary check failed: %s", name, e)

    if not valid_hits:
        log.info("[%s] No valid records to create.", name)
        return False

    # 7. Extract entity payload from ES data path
    data_path = job.get("es_data_path", "")
    objects   = []
    for hit in valid_hits:
        source = hit.get("_source", {})
        obj    = _dot_get(source, data_path) if data_path else source
        if obj and isinstance(obj, dict):
            objects.append(obj)
        else:
            log.warning("[%s] Skipping doc _id=%s — no data at path '%s'", name, hit.get("_id"), data_path)

    log.info("[%s] Records ready for bulk create: %d", name, len(objects))

    # 8. Bulk create
    success, failed = bulk_create(objects, job)

    log.info("[%s] DONE | Created: %d | Failed: %d", name, success, failed)
    return failed > 0

# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def main():
    enabled_jobs = [j for j in JOBS if j.get("enabled", False)]
    log.info("reconcile_es_not_in_db started — %d job(s) enabled", len(enabled_jobs))
    log.info("Training keywords: %s", TRAINING_KEYWORDS)

    if not enabled_jobs:
        log.error("No enabled jobs. Set enabled=True on at least one job in JOBS.")
        sys.exit(0)

    try:
        conn = psycopg2.connect(
            host=DB_HOST, port=DB_PORT, database=DB_NAME,
            user=DB_USER, password=DB_PASSWORD,
            sslmode=DB_SSLMODE, connect_timeout=30,
        )
        conn.autocommit = False
    except Exception as e:
        log.error("DB connection failed: %s", e)
        sys.exit(1)

    es_headers   = _get_es_headers()
    any_failures = False

    try:
        for job in enabled_jobs:
            try:
                failed = run_job(job, conn, es_headers)
                if failed:
                    any_failures = True
            except Exception as e:
                log.exception("[%s] Unexpected error: %s", job.get("name", "?"), e)
                any_failures = True
    finally:
        conn.close()

    log.info("=" * 60)
    log.info("reconcile_es_not_in_db complete | any_failures=%s", any_failures)
    sys.exit(1 if any_failures else 0)


if __name__ == "__main__":
    main()
