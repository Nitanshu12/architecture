"""
Phase 4 — Gold Core Build
District Evolution Intelligence System v0.3

Steps 1-7: DDL, dimensions, registry, snapshot, quarantine, validation gate.
Database: DuckDB at data/gold/district_evolution.duckdb
"""

import json
import logging
import os
import re
import sys
import uuid
from collections import defaultdict
from pathlib import Path
from datetime import datetime

import duckdb
import pandas as pd
import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[2]
GOLD_DIR = PROJECT_ROOT / "data" / "gold"
GOLD_CORE = GOLD_DIR / "core"
DB_PATH = GOLD_DIR / "district_evolution.duckdb"
IDENTITY_OUT = PROJECT_ROOT / "outputs" / "identity"
CONFIG_DIR = PROJECT_ROOT / "config"

PIPELINE_VERSION = "0.3.0"
PIPELINE_RUN_ID = str(uuid.uuid4())
ANCHOR_YEAR = 1951

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()])
log = logging.getLogger(__name__)

def chk(label, passed, detail=""):
    status = "✓ PASS" if passed else "✗ FAIL"
    log.info("  [%s] %s%s", status, label, f": {detail}" if detail else "")
    return label, passed, detail

def abort(msg):
    log.error("ABORT: %s", msg)
    sys.exit(1)

# ── STEP 0 ───────────────────────────────────────────────────────────────────

def step0_inspect():
    log.info("=" * 70)
    log.info("STEP 0 — DATA INSPECTION")
    log.info("=" * 70)
    with open(GOLD_CORE / "ck_registry.json") as f:
        reg = json.load(f)
    entries = reg["entries"]
    log.info("(a) CK Registry: %d entries", len(entries))
    years = [e["established_year"] for e in entries.values() if e.get("established_year")]
    log.info("    Year range: %d–%d", min(years), max(years))
    bad = [ck for ck in entries if not re.match(r"^IND-\d{6}$", ck)]
    if bad:
        abort(f"CK format violations: {bad[:5]}")
    log.info("    Format check: 0 violations")

    with open(GOLD_CORE / "source_pk_to_ck_mapping.json") as f:
        mapping_data = json.load(f)
    all_m = mapping_data["mappings"]
    matched = [m for m in all_m if m["match_status"] == "MATCHED"]
    unmatched = [m for m in all_m if m["match_status"] == "UNMATCHED"]
    methods = defaultdict(int)
    for m in all_m:
        methods[m["match_method"]] += 1
    log.info("(b) Mapping: %d total, %d matched, %d unmatched", len(all_m), len(matched), len(unmatched))
    log.info("    Methods: %s", dict(methods))

    rq = pd.read_csv(IDENTITY_OUT / "residual_quarantine.csv")
    log.info("(e) Residual quarantine: %d records", len(rq))
    return reg, entries, all_m, matched, unmatched, rq


# ── STEP 1 — DDL ─────────────────────────────────────────────────────────────

DDL = [
    ("dim_time", """
        CREATE TABLE IF NOT EXISTS dim_time (
            year_sk      INTEGER PRIMARY KEY,
            census_year  INTEGER NOT NULL UNIQUE,
            is_census_reference_year BOOLEAN NOT NULL DEFAULT FALSE,
            decade       INTEGER NOT NULL,
            century      INTEGER NOT NULL,
            pipeline_run_id UUID NOT NULL,
            pipeline_version TEXT NOT NULL
        )"""),
    ("dim_source", """
        CREATE TABLE IF NOT EXISTS dim_source (
            source_sk    INTEGER PRIMARY KEY,
            source_name  TEXT NOT NULL UNIQUE,
            source_type  TEXT NOT NULL,
            provenance   TEXT NOT NULL,
            legal_authority_rank  INTEGER NOT NULL CHECK (legal_authority_rank BETWEEN 1 AND 5),
            spatial_precision_rank INTEGER NOT NULL CHECK (spatial_precision_rank BETWEEN 1 AND 5),
            pipeline_run_id UUID NOT NULL,
            pipeline_version TEXT NOT NULL
        )"""),
    ("dim_event_type", """
        CREATE TABLE IF NOT EXISTS dim_event_type (
            event_type_sk  INTEGER PRIMARY KEY,
            event_type_code TEXT NOT NULL UNIQUE,
            creates_new_identity BOOLEAN,
            preserves_identity   BOOLEAN,
            description    TEXT,
            pipeline_run_id UUID NOT NULL,
            pipeline_version TEXT NOT NULL
        )"""),
    ("dim_administrative_unit", """
        CREATE TABLE IF NOT EXISTS dim_administrative_unit (
            unit_sk      INTEGER PRIMARY KEY,
            unit_type    TEXT NOT NULL CHECK (unit_type IN ('COUNTRY','STATE','UT','DIVISION')),
            unit_name    TEXT NOT NULL,
            unit_code    TEXT,
            parent_unit_sk INTEGER REFERENCES dim_administrative_unit(unit_sk),
            valid_from_year INTEGER,
            valid_to_year   INTEGER,
            pipeline_run_id UUID NOT NULL,
            pipeline_version TEXT NOT NULL
        )"""),
    ("canonical_key_registry", """
        CREATE TABLE IF NOT EXISTS canonical_key_registry (
            ck_sk        INTEGER PRIMARY KEY,
            canonical_key TEXT NOT NULL UNIQUE CHECK (canonical_key SIMILAR TO 'IND-[0-9]{6}'),
            established_date DATE NOT NULL,
            established_date_precision TEXT NOT NULL CHECK (
                established_date_precision IN ('EXACT','MONTH','YEAR','DECADE','CENTURY','UNKNOWN')),
            display_name TEXT NOT NULL,
            state_at_creation TEXT NOT NULL,
            is_active    BOOLEAN NOT NULL,
            display_code TEXT NOT NULL,
            allocation_reason TEXT,
            pipeline_run_id UUID NOT NULL,
            pipeline_version TEXT NOT NULL
        )"""),
    ("source_pk_to_ck_mapping", """
        CREATE TABLE IF NOT EXISTS source_pk_to_ck_mapping (
            mapping_sk   INTEGER PRIMARY KEY,
            source_pk    TEXT NOT NULL,
            source_dataset TEXT NOT NULL,
            source_year  INTEGER NOT NULL,
            source_layer TEXT,
            canonical_key TEXT REFERENCES canonical_key_registry(canonical_key),
            district_name TEXT NOT NULL,
            state        TEXT,
            match_method TEXT NOT NULL,
            match_score  DOUBLE,
            match_status TEXT NOT NULL CHECK (match_status IN ('MATCHED','UNMATCHED','QUARANTINED')),
            evidence     TEXT,
            pipeline_run_id UUID NOT NULL,
            pipeline_version TEXT NOT NULL,
            UNIQUE (source_pk, source_dataset, source_year)
        )"""),
    ("fact_district_snapshot", """
        CREATE TABLE IF NOT EXISTS fact_district_snapshot (
            snapshot_sk  INTEGER PRIMARY KEY,
            canonical_key TEXT NOT NULL REFERENCES canonical_key_registry(canonical_key),
            time_sk      INTEGER NOT NULL REFERENCES dim_time(year_sk),
            parent_unit_sk INTEGER REFERENCES dim_administrative_unit(unit_sk),
            valid_from_est DATE NOT NULL,
            valid_from_precision TEXT NOT NULL CHECK (
                valid_from_precision IN ('EXACT','MONTH','YEAR','DECADE','CENTURY','UNKNOWN')),
            valid_to_est DATE,
            valid_to_precision TEXT CHECK (
                valid_to_precision IS NULL OR
                valid_to_precision IN ('EXACT','MONTH','YEAR','DECADE','CENTURY','UNKNOWN')),
            primary_name TEXT NOT NULL,
            snapshot_type TEXT NOT NULL CHECK (
                snapshot_type IN ('INITIAL','CENSUS_REFERENCE','NAME_CHANGE')),
            has_geometry BOOLEAN NOT NULL DEFAULT FALSE,
            reconciliation_id TEXT,
            identity_confidence  DOUBLE,
            temporal_confidence  DOUBLE,
            evidence_strength    TEXT,
            evidence_type TEXT NOT NULL CHECK (evidence_type IN ('OBSERVED','DERIVED','CURATED')),
            source_pk    TEXT,
            source_dataset TEXT,
            pipeline_run_id UUID NOT NULL,
            pipeline_version TEXT NOT NULL
        )"""),
    ("residual_quarantine", """
        CREATE TABLE IF NOT EXISTS residual_quarantine (
            quarantine_sk INTEGER PRIMARY KEY,
            source_pk    TEXT NOT NULL,
            dataset_name TEXT NOT NULL,
            census_year  INTEGER NOT NULL,
            raw_name     TEXT,
            normalized_name TEXT,
            state_name   TEXT,
            diagnosis_category TEXT NOT NULL,
            diagnosis_detail   TEXT,
            pipeline_run_id UUID NOT NULL,
            pipeline_version TEXT NOT NULL,
            quarantined_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )"""),
]


def step1_ddl(con):
    log.info("=" * 70)
    log.info("STEP 1 — DDL")
    log.info("=" * 70)
    for name, stmt in DDL:
        con.execute(stmt)
        log.info("  Created/confirmed: %s", name)
    tables = [r[0] for r in con.execute("SHOW TABLES").fetchall()]
    log.info("  Tables: %s", tables)
    expected = {n for n, _ in DDL}
    missing = expected - set(tables)
    if missing:
        abort(f"Missing tables: {missing}")
    log.info("  All %d tables confirmed.", len(expected))


# ── STEP 2 — DIMENSIONS ───────────────────────────────────────────────────────

def step2_dim_time(con, matched_records):
    log.info("── dim_time ──")
    year_counts = defaultdict(int)
    for m in matched_records:
        year_counts[int(m["source_year"])] += 1
    census_ref_years = {y for y, c in year_counts.items() if c > 50}
    log.info("  Census reference years: %s", sorted(census_ref_years))
    rows = [{"year_sk": y, "census_year": y,
              "is_census_reference_year": y in census_ref_years,
              "decade": (y // 10) * 10, "century": (y // 100) * 100,
              "pipeline_run_id": PIPELINE_RUN_ID, "pipeline_version": PIPELINE_VERSION}
             for y in range(1800, 2101)]
    df = pd.DataFrame(rows)
    con.execute("DELETE FROM dim_time")
    con.execute("INSERT INTO dim_time SELECT * FROM df")
    count = con.execute("SELECT COUNT(*) FROM dim_time").fetchone()[0]
    cref  = con.execute("SELECT COUNT(*) FROM dim_time WHERE is_census_reference_year").fetchone()[0]
    log.info("  dim_time: %d rows, %d census reference years", count, cref)


def step2_dim_source(con):
    log.info("── dim_source ──")
    with open(CONFIG_DIR / "sources.yaml") as f:
        src_cfg = yaml.safe_load(f)
    rows = []
    sk = 1
    for sname, smeta in src_cfg["sources"].items():
        if sname == "events":
            lr, sr = 4, 5
        else:
            lr = smeta.get("legal_authority_rank", 5)
            sr = smeta.get("spatial_precision_rank", 5)
        log.info("  %s → legal_rank=%d, spatial_rank=%d", sname, lr, sr)
        rows.append({"source_sk": sk, "source_name": sname,
                     "source_type": smeta.get("type", "unknown"),
                     "provenance": smeta.get("provenance", sname),
                     "legal_authority_rank": lr, "spatial_precision_rank": sr,
                     "pipeline_run_id": PIPELINE_RUN_ID, "pipeline_version": PIPELINE_VERSION})
        sk += 1
    df = pd.DataFrame(rows)
    con.execute("DELETE FROM dim_source")
    con.execute("INSERT INTO dim_source SELECT * FROM df")
    log.info("  dim_source rows:\n%s",
             con.execute("SELECT * FROM dim_source ORDER BY source_sk").fetchdf().to_string())
    bad = con.execute("SELECT COUNT(*) FROM dim_source WHERE legal_authority_rank < 1 "
                      "OR legal_authority_rank > 5 OR spatial_precision_rank < 1 "
                      "OR spatial_precision_rank > 5").fetchone()[0]
    if bad:
        abort(f"dim_source rank out of range: {bad} rows")


def step2_dim_event_type(con):
    log.info("── dim_event_type ──")
    events = pd.read_csv(PROJECT_ROOT / "data" / "bronze" / "events" / "district_evolution_master.csv")
    event_types = sorted(events["event_type"].dropna().unique().tolist())
    log.info("  Event types from data: %s", event_types)
    meta = {
        "NEW_DISTRICT": (True, False, "New administrative district with no predecessor"),
        "SPLIT": (True, False, "Parent district split into children"),
        "RENAME": (False, True, "District renamed; identity preserved"),
    }
    rows = [{"event_type_sk": i, "event_type_code": et,
              "creates_new_identity": meta.get(et, (None, None, ""))[0],
              "preserves_identity": meta.get(et, (None, None, ""))[1],
              "description": meta.get(et, (None, None, et))[2],
              "pipeline_run_id": PIPELINE_RUN_ID, "pipeline_version": PIPELINE_VERSION}
             for i, et in enumerate(event_types, 1)]
    df = pd.DataFrame(rows)
    con.execute("DELETE FROM dim_event_type")
    con.execute("INSERT INTO dim_event_type SELECT * FROM df")
    log.info("  dim_event_type:\n%s",
             df[["event_type_code","creates_new_identity","preserves_identity"]].to_string())


def step2_dim_admin_unit(con, all_mappings):
    log.info("── dim_administrative_unit ──")
    state_years = defaultdict(set)
    for m in all_mappings:
        if m["match_status"] == "MATCHED" and m.get("state"):
            state_years[m["state"].strip()].add(int(m["source_year"]))
    log.info("  Distinct states in data: %d", len(state_years))

    con.execute("DELETE FROM dim_administrative_unit")

    # Insert country row first to satisfy self-referential FK
    country_row = pd.DataFrame([{"unit_sk": 1, "unit_type": "COUNTRY", "unit_name": "India",
        "unit_code": "IND", "parent_unit_sk": None,
        "valid_from_year": 1947, "valid_to_year": None,
        "pipeline_run_id": PIPELINE_RUN_ID, "pipeline_version": PIPELINE_VERSION}])
    con.execute("INSERT INTO dim_administrative_unit SELECT * FROM country_row")

    sk = 2
    state_to_sk = {}
    ut_markers = {"andaman","chandigarh","dadra","daman","delhi","lakshadweep",
                  "puducherry","pondicherry","ladakh","laccadive","pondichery"}
    state_rows = []
    for state in sorted(state_years.keys()):
        nl = state.lower()
        utype = "UT" if any(x in nl for x in ut_markers) else "STATE"
        state_rows.append({"unit_sk": sk, "unit_type": utype, "unit_name": state,
                     "unit_code": None, "parent_unit_sk": 1,
                     "valid_from_year": min(state_years[state]),
                     "valid_to_year": None,
                     "pipeline_run_id": PIPELINE_RUN_ID, "pipeline_version": PIPELINE_VERSION})
        state_to_sk[nl] = sk
        sk += 1

    df = pd.DataFrame(state_rows)
    con.execute("INSERT INTO dim_administrative_unit SELECT * FROM df")

    counts = con.execute("SELECT unit_type, COUNT(*) FROM dim_administrative_unit "
                          "GROUP BY unit_type ORDER BY unit_type").fetchdf()
    log.info("  dim_administrative_unit by type:\n%s", counts.to_string())
    return state_to_sk


def step2_dimensions(con, matched_records, all_mappings):
    log.info("=" * 70)
    log.info("STEP 2 — LOAD DIMENSION TABLES")
    log.info("=" * 70)
    step2_dim_time(con, matched_records)
    step2_dim_source(con)
    step2_dim_event_type(con)
    return step2_dim_admin_unit(con, all_mappings)


# ── STEP 3 — REGISTRY + MAPPING ──────────────────────────────────────────────

def step3_registry(con, entries):
    log.info("=" * 70)
    log.info("STEP 3 — LOAD CANONICAL_KEY_REGISTRY + MAPPING")
    log.info("=" * 70)
    rows = []
    for i, (ck, e) in enumerate(sorted(entries.items()), 1):
        yr = e.get("established_year")
        est_date = f"{yr}-01-01" if yr else "1947-01-01"
        date_prec = "YEAR" if yr else "UNKNOWN"
        rows.append({"ck_sk": i, "canonical_key": ck,
                     "established_date": est_date,
                     "established_date_precision": date_prec,
                     "display_name": e.get("display_name", ""),
                     "state_at_creation": e.get("state", ""),
                     "is_active": e.get("is_active", True),
                     "display_code": ck,
                     "allocation_reason": e.get("allocation_reason", ""),
                     "pipeline_run_id": PIPELINE_RUN_ID,
                     "pipeline_version": PIPELINE_VERSION})
    df = pd.DataFrame(rows)
    df["established_date"] = pd.to_datetime(df["established_date"]).dt.date
    con.execute("DELETE FROM canonical_key_registry")
    con.execute("INSERT INTO canonical_key_registry SELECT * FROM df")
    count = con.execute("SELECT COUNT(*) FROM canonical_key_registry").fetchone()[0]
    log.info("  canonical_key_registry: %d rows", count)
    bad = con.execute("SELECT COUNT(*) FROM canonical_key_registry "
                      "WHERE canonical_key NOT SIMILAR TO 'IND-[0-9]{6}'").fetchone()[0]
    if bad:
        abort(f"CK format violations: {bad}")
    log.info("  Format check: 0 violations")


def step3_mapping(con, matched):
    log.info("  Loading %d matched mappings ...", len(matched))
    rows = [{"mapping_sk": i, "source_pk": m["source_pk"],
              "source_dataset": m["source_dataset"],
              "source_year": int(m["source_year"]),
              "source_layer": m.get("source_layer"),
              "canonical_key": m["canonical_key"],
              "district_name": m["district_name"],
              "state": m.get("state"),
              "match_method": m["match_method"],
              "match_score": m.get("match_score"),
              "match_status": "MATCHED",
              "evidence": json.dumps(m.get("evidence", [])),
              "pipeline_run_id": PIPELINE_RUN_ID,
              "pipeline_version": PIPELINE_VERSION}
             for i, m in enumerate(matched, 1)]
    df = pd.DataFrame(rows)
    con.execute("DELETE FROM source_pk_to_ck_mapping")
    con.execute("INSERT INTO source_pk_to_ck_mapping SELECT * FROM df")
    count = con.execute("SELECT COUNT(*) FROM source_pk_to_ck_mapping").fetchone()[0]
    log.info("  source_pk_to_ck_mapping: %d rows", count)
    dups = con.execute("SELECT COUNT(*) FROM (SELECT source_pk, source_dataset, source_year, COUNT(*) c "
                       "FROM source_pk_to_ck_mapping GROUP BY source_pk, source_dataset, source_year "
                       "HAVING c > 1)").fetchone()[0]
    if dups:
        abort(f"Duplicate (source_pk, source_dataset): {dups}")
    log.info("  Uniqueness check: 0 duplicates")


# ── STEP 4 — OVERLAP SMOKE TEST ───────────────────────────────────────────────

def step4_overlap_smoke_test(con):
    log.info("=" * 70)
    log.info("STEP 4 — TEMPORAL OVERLAP SMOKE TEST")
    log.info("=" * 70)
    test_ck = "IND-000001"
    run_id = str(uuid.uuid4())
    next_sk = con.execute("SELECT COALESCE(MAX(snapshot_sk),0)+1 FROM fact_district_snapshot").fetchone()[0]
    con.execute("""INSERT INTO fact_district_snapshot VALUES
        (?,?,1951,NULL,'1951-01-01','YEAR','1960-12-31','YEAR',
         'TEST1','INITIAL',FALSE,NULL,1.0,0.7,'ACADEMIC','OBSERVED','TPK1','test',?,?)""",
        [next_sk, test_ck, run_id, PIPELINE_VERSION])
    con.execute("""INSERT INTO fact_district_snapshot VALUES
        (?,?,1955,NULL,'1955-01-01','YEAR','1970-12-31','YEAR',
         'TEST2','CENSUS_REFERENCE',FALSE,NULL,1.0,0.7,'ACADEMIC','OBSERVED','TPK2','test',?,?)""",
        [next_sk+1, test_ck, run_id, PIPELINE_VERSION])
    overlaps = con.execute("""
        SELECT COUNT(*) FROM fact_district_snapshot a
        JOIN fact_district_snapshot b
          ON a.canonical_key=b.canonical_key AND a.snapshot_sk<b.snapshot_sk
         AND a.valid_from_est <= COALESCE(b.valid_to_est,'9999-12-31'::DATE)
         AND COALESCE(a.valid_to_est,'9999-12-31'::DATE) >= b.valid_from_est
        WHERE a.pipeline_run_id=?""", [run_id]).fetchone()[0]
    log.info("  Intentional overlap detected: %d (expected 1)", overlaps)
    if overlaps != 1:
        abort("Overlap smoke test failed — query did not detect intentional overlap")
    con.execute("DELETE FROM fact_district_snapshot WHERE pipeline_run_id=?", [run_id])
    log.info("  ✓ Overlap detection confirmed, test rows cleaned")


# ── STEP 5 — FACT_DISTRICT_SNAPSHOT ──────────────────────────────────────────

def step5_snapshot(con, matched_records, state_to_sk):
    log.info("=" * 70)
    log.info("STEP 5 — LOAD FACT_DISTRICT_SNAPSHOT")
    log.info("=" * 70)
    ck_records = defaultdict(list)
    for m in matched_records:
        ck_records[m["canonical_key"]].append({
            "year": int(m["source_year"]), "name": m["district_name"],
            "state": m.get("state",""), "source_pk": m["source_pk"],
            "dataset": m["source_dataset"], "score": m.get("match_score",1.0),
            "method": m["match_method"]})
    for ck in ck_records:
        ck_records[ck].sort(key=lambda x: x["year"])

    rows = []
    sk = 1
    for ck in sorted(ck_records.keys()):
        recs = ck_records[ck]
        for i, rec in enumerate(recs):
            year = rec["year"]
            prev_name = recs[i-1]["name"] if i > 0 else None
            curr_name = rec["name"]
            valid_from = f"{year}-01-01"
            valid_to = f"{recs[i+1]['year']-1}-12-31" if i < len(recs)-1 else None
            if i == 0:
                snap_type = "INITIAL"
            elif prev_name and curr_name.lower().strip() != prev_name.lower().strip():
                snap_type = "NAME_CHANGE"
            else:
                snap_type = "CENSUS_REFERENCE"
            method = rec["method"]
            ev_strength = ("CENSUS_SECONDARY" if method.startswith("EVENT_SUPPORTED") else "ACADEMIC")
            rows.append({
                "snapshot_sk": sk, "canonical_key": ck, "time_sk": year,
                "parent_unit_sk": state_to_sk.get(rec["state"].strip().lower()),
                "valid_from_est": valid_from, "valid_from_precision": "YEAR",
                "valid_to_est": valid_to,
                "valid_to_precision": "YEAR" if valid_to else None,
                "primary_name": curr_name, "snapshot_type": snap_type,
                "has_geometry": False, "reconciliation_id": None,
                "identity_confidence": float(rec["score"]) if rec["score"] else 1.0,
                "temporal_confidence": 0.70, "evidence_strength": ev_strength,
                "evidence_type": "OBSERVED",
                "source_pk": rec["source_pk"], "source_dataset": rec["dataset"],
                "pipeline_run_id": PIPELINE_RUN_ID, "pipeline_version": PIPELINE_VERSION})
            sk += 1

    df = pd.DataFrame(rows)
    df["valid_from_est"] = pd.to_datetime(df["valid_from_est"]).dt.date
    df["valid_to_est"] = pd.to_datetime(df["valid_to_est"], errors="coerce").dt.date

    BATCH = 500
    con.execute("DELETE FROM fact_district_snapshot")
    for i in range(0, len(df), BATCH):
        batch = df.iloc[i:i+BATCH]
        con.execute("INSERT INTO fact_district_snapshot SELECT * FROM batch")
    total = con.execute("SELECT COUNT(*) FROM fact_district_snapshot").fetchone()[0]
    log.info("  fact_district_snapshot total: %d rows", total)
    null_from = con.execute("SELECT COUNT(*) FROM fact_district_snapshot WHERE valid_from_est IS NULL").fetchone()[0]
    if null_from:
        abort(f"NULL valid_from_est: {null_from} rows")
    log.info("  NULL valid_from_est: 0")
    return total


# ── STEP 6 — RESIDUAL QUARANTINE ─────────────────────────────────────────────

def step6_residual(con, rq_df):
    log.info("=" * 70)
    log.info("STEP 6 — LOAD RESIDUAL_QUARANTINE")
    log.info("=" * 70)
    rows = []
    for i, row in rq_df.iterrows():
        diag = str(row.get("diagnosis",""))
        cat = diag.split(":")[0].strip() if ":" in diag else diag
        rows.append({"quarantine_sk": i+1,
                     "source_pk": str(row.get("source_pk","")),
                     "dataset_name": str(row.get("source_dataset","")),
                     "census_year": int(row.get("source_year",0)),
                     "raw_name": str(row.get("district_name","")),
                     "normalized_name": str(row.get("normalized_name", row.get("district_name",""))),
                     "state_name": str(row.get("state","")),
                     "diagnosis_category": cat,
                     "diagnosis_detail": diag,
                     "pipeline_run_id": PIPELINE_RUN_ID,
                     "pipeline_version": PIPELINE_VERSION,
                     "quarantined_at": datetime.utcnow().isoformat()})
    df = pd.DataFrame(rows)
    df["quarantined_at"] = pd.to_datetime(df["quarantined_at"])
    con.execute("DELETE FROM residual_quarantine")
    con.execute("INSERT INTO residual_quarantine SELECT * FROM df")
    count = con.execute("SELECT COUNT(*) FROM residual_quarantine").fetchone()[0]
    log.info("  residual_quarantine: %d rows", count)
    cats = con.execute("SELECT diagnosis_category, COUNT(*) c FROM residual_quarantine "
                       "GROUP BY diagnosis_category").fetchdf()
    log.info("  Categories:\n%s", cats.to_string())
    return count


# ── STEP 7 — VALIDATION GATE ─────────────────────────────────────────────────

def step7_validation(con, entries, matched, rq_count):
    log.info("=" * 70)
    log.info("STEP 7 — VALIDATION GATE")
    log.info("=" * 70)
    R = []

    # Schema
    R.append(chk("canonical_key CHECK exists",
        len(con.execute("SELECT * FROM information_schema.columns WHERE table_name='canonical_key_registry' AND column_name='canonical_key'").fetchall()) > 0))
    R.append(chk("valid_from_precision column exists",
        len(con.execute("SELECT * FROM information_schema.columns WHERE table_name='fact_district_snapshot' AND column_name='valid_from_precision'").fetchall()) > 0))
    R.append(chk("evidence_type column exists",
        len(con.execute("SELECT * FROM information_schema.columns WHERE table_name='fact_district_snapshot' AND column_name='evidence_type'").fetchall()) > 0))
    R.append(chk("dim_source rank columns exist",
        len(con.execute("SELECT * FROM information_schema.columns WHERE table_name='dim_source' AND column_name='legal_authority_rank'").fetchall()) > 0))

    # Data counts
    ck_count = con.execute("SELECT COUNT(*) FROM canonical_key_registry").fetchone()[0]
    R.append(chk(f"canonical_key_registry: {len(entries)} rows", ck_count == len(entries), f"actual={ck_count}"))
    bad_ck = con.execute("SELECT COUNT(*) FROM canonical_key_registry WHERE canonical_key NOT SIMILAR TO 'IND-[0-9]{6}'").fetchone()[0]
    R.append(chk("canonical_key_registry: all format-compliant", bad_ck == 0))
    map_count = con.execute("SELECT COUNT(*) FROM source_pk_to_ck_mapping").fetchone()[0]
    R.append(chk(f"source_pk_to_ck_mapping: {len(matched)} rows", map_count == len(matched), f"actual={map_count}"))
    map_dups = con.execute("SELECT COUNT(*) FROM (SELECT source_pk, source_dataset, source_year, COUNT(*) c FROM source_pk_to_ck_mapping GROUP BY source_pk, source_dataset, source_year HAVING c > 1)").fetchone()[0]
    R.append(chk("source_pk_to_ck_mapping: no duplicate source_pk+dataset+year", map_dups == 0))
    snap_count = con.execute("SELECT COUNT(*) FROM fact_district_snapshot").fetchone()[0]
    R.append(chk(f"fact_district_snapshot: {len(matched)} rows", snap_count == len(matched), f"actual={snap_count}"))
    null_from = con.execute("SELECT COUNT(*) FROM fact_district_snapshot WHERE valid_from_est IS NULL").fetchone()[0]
    R.append(chk("fact_district_snapshot: zero NULL valid_from_est", null_from == 0))
    overlaps = con.execute("""
        SELECT COUNT(*) FROM fact_district_snapshot a
        JOIN fact_district_snapshot b
          ON a.canonical_key=b.canonical_key AND a.snapshot_sk<b.snapshot_sk
         AND a.valid_from_est <= COALESCE(b.valid_to_est,'9999-12-31'::DATE)
         AND COALESCE(a.valid_to_est,'9999-12-31'::DATE) >= b.valid_from_est
    """).fetchone()[0]
    R.append(chk("fact_district_snapshot: zero overlapping periods", overlaps == 0, f"overlaps={overlaps}"))
    null_ev = con.execute("SELECT COUNT(*) FROM fact_district_snapshot WHERE evidence_type IS NULL OR pipeline_run_id IS NULL").fetchone()[0]
    R.append(chk("fact_district_snapshot: zero NULL evidence_type/run_id", null_ev == 0))
    geom = con.execute("SELECT COUNT(*) FROM fact_district_snapshot WHERE has_geometry").fetchone()[0]
    R.append(chk("fact_district_snapshot: zero has_geometry=TRUE", geom == 0))
    rq_actual = con.execute("SELECT COUNT(*) FROM residual_quarantine").fetchone()[0]
    R.append(chk(f"residual_quarantine: {rq_count} rows", rq_actual == rq_count, f"actual={rq_actual}"))

    # Referential integrity
    orphan_ck = con.execute("SELECT COUNT(*) FROM fact_district_snapshot s LEFT JOIN canonical_key_registry r ON s.canonical_key=r.canonical_key WHERE r.canonical_key IS NULL").fetchone()[0]
    R.append(chk("All snapshot.canonical_key in registry", orphan_ck == 0))
    orphan_time = con.execute("SELECT COUNT(*) FROM fact_district_snapshot s LEFT JOIN dim_time t ON s.time_sk=t.year_sk WHERE t.year_sk IS NULL").fetchone()[0]
    R.append(chk("All snapshot.time_sk in dim_time", orphan_time == 0))
    orphan_unit = con.execute("SELECT COUNT(*) FROM fact_district_snapshot s LEFT JOIN dim_administrative_unit u ON s.parent_unit_sk=u.unit_sk WHERE s.parent_unit_sk IS NOT NULL AND u.unit_sk IS NULL").fetchone()[0]
    R.append(chk("All snapshot.parent_unit_sk in dim_admin_unit", orphan_unit == 0))

    log.info("")
    log.info("=" * 70)
    log.info("VALIDATION GATE SUMMARY")
    log.info("=" * 70)
    passed = sum(1 for _, p, _ in R if p)
    failed = sum(1 for _, p, _ in R if not p)
    log.info("  PASS: %d / %d", passed, len(R))
    if failed:
        log.error("  FAIL: %d", failed)
        for lbl, p, d in R:
            if not p:
                log.error("    ✗ %s %s", lbl, d)
    return failed == 0


# ── MAIN ─────────────────────────────────────────────────────────────────────

def main():
    log.info("=" * 70)
    log.info("PHASE 4 — GOLD CORE BUILD")
    log.info("Run ID: %s", PIPELINE_RUN_ID)
    log.info("=" * 70)

    reg, entries, all_m, matched, unmatched, rq_df = step0_inspect()

    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    if DB_PATH.exists():
        DB_PATH.unlink()
        log.info("Removed existing DB for clean run")

    con = duckdb.connect(str(DB_PATH))
    try:
        step1_ddl(con)
        state_to_sk = step2_dimensions(con, matched, all_m)
        step3_registry(con, entries)
        step3_mapping(con, matched)
        step4_overlap_smoke_test(con)
        snap_count = step5_snapshot(con, matched, state_to_sk)
        rq_count = step6_residual(con, rq_df)
        all_pass = step7_validation(con, entries, matched, rq_count)
        log.info("")
        log.info("=" * 70)
        log.info("PHASE 4 %s", "COMPLETE — ALL CHECKS PASS" if all_pass else "COMPLETE — SOME CHECKS FAILED")
        log.info("  DB: %s", DB_PATH)
        log.info("  CKs: %d | Matched: %d | Snapshots: %d | Quarantine: %d",
                 len(entries), len(matched), snap_count, rq_count)
        log.info("=" * 70)
    finally:
        con.close()

if __name__ == "__main__":
    main()
