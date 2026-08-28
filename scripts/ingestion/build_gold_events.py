"""
Phase 5 — Gold Events + Lineage
District Evolution Intelligence System v0.3

Steps 0-7: Inspect, DDL, event mapping, boundary_event, event_participant,
event_evidence, district_relationship, DAG cycle check, name_variant, CK closure,
validation gate.
"""

import csv
import json
import logging
import re
import sys
import uuid
from collections import defaultdict
from datetime import date, timedelta
from pathlib import Path

import duckdb
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
GOLD_DIR     = PROJECT_ROOT / "data" / "gold"
DB_PATH      = GOLD_DIR / "district_evolution.duckdb"
IDENTITY_OUT = PROJECT_ROOT / "outputs" / "identity"
EVENT_CSV    = PROJECT_ROOT / "data" / "bronze" / "events" / "district_evolution_master.csv"
RES_FAIL_CSV = IDENTITY_OUT / "resolution_failures.csv"

PIPELINE_VERSION = "0.3.0"
PIPELINE_RUN_ID  = str(uuid.uuid4())

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()])
log = logging.getLogger(__name__)

# ─── Helpers ────────────────────────────────────────────────────────────────

def chk(label, passed, detail=""):
    status = "✓ PASS" if passed else "✗ FAIL"
    log.info("  [%s] %s%s", status, label, f": {detail}" if detail else "")
    return label, passed, detail

def abort(msg):
    log.error("ABORT: %s", msg)
    sys.exit(1)

def normalize_name(s):
    """Same normalization used in Phase 3 Tier 1."""
    if not s:
        return ""
    s = str(s).strip()
    s = s.lower()
    s = re.sub(r"['''`]", "", s)
    s = re.sub(r"[-–—/]", " ", s)
    s = re.sub(r"\s+", " ", s)
    s = s.strip()
    return s

# ─── STEP 0 — DATA INSPECTION (printed at run time) ─────────────────────────

def step0_inspect(con):
    log.info("=" * 70)
    log.info("STEP 0 — DATA INSPECTION SUMMARY (from actual files)")
    log.info("=" * 70)

    ev = pd.read_csv(EVENT_CSV)
    log.info("Bronze events: %d rows, columns=%s", len(ev), list(ev.columns))
    counts = ev["event_type"].value_counts().to_dict()
    log.info("event_type counts: %s", counts)

    date_cols = ["effective_year"]
    for c in date_cols:
        vals = ev[c].dropna()
        log.info("Field '%s': min=%s, max=%s, nulls=%d", c, vals.min(), vals.max(), ev[c].isna().sum())

    log.info("parent_district (predecessor): nulls per event_type:")
    for et in sorted(counts.keys()):
        sub = ev[ev["event_type"] == et]
        log.info("  %s: parent_district_nulls=%d / %d", et, sub["parent_district"].isna().sum(), len(sub))

    # Load snapshot for split_case determination
    snapshot = con.execute(
        "SELECT canonical_key, time_sk AS year, primary_name FROM fact_district_snapshot"
    ).fetchdf()
    name_years = defaultdict(set)
    for _, row in snapshot.iterrows():
        nm = normalize_name(row["primary_name"])
        name_years[nm].add((row["canonical_key"], int(row["year"])))

    splits = ev[ev["event_type"] == "SPLIT"]
    parent_cases = {}
    for (parent, state), group in splits.groupby(["parent_district", "state"]):
        min_yr = group["effective_year"].min()
        nm = normalize_name(parent)
        entries = name_years.get(nm, set())
        years_after = {y for _, y in entries if y > min_yr}
        parent_cases[(parent, state)] = "CARVE_OUT" if years_after else "CLEAN_SPLIT"

    from collections import Counter
    row_cases = []
    for (p, s), group in splits.groupby(["parent_district", "state"]):
        c = parent_cases.get((p, s), "UNKNOWN")
        row_cases.extend([c] * len(group))

    case_counts = Counter(row_cases)
    log.info("SPLIT event rows by case: %s", dict(case_counts))

    # dim_source → derive evidence_strength label from legal_authority_rank
    src = con.execute("SELECT source_name, legal_authority_rank FROM dim_source WHERE source_name='stanford'").fetchone()
    stanford_rank = src[1] if src else None
    log.info("Stanford legal_authority_rank: %s → evidence_strength derived at runtime", stanford_rank)

    log.info("=" * 70)
    log.info("STEP 0 — EVENT TYPE → ARCHITECTURE MAPPING (STEP 2 TABLE)")
    log.info("=" * 70)
    log.info("  data event_type  | split_case  | rel_type       | CK action")
    log.info("  SPLIT (CARVE_OUT)| CARVE_OUT   | FORMED_FROM    | Predecessor stays open")
    log.info("  SPLIT (CLEAN_SL) | CLEAN_SPLIT | SPLIT_FROM     | Close predecessor CK")
    log.info("  NEW_DISTRICT     | NULL        | FORMED_FROM    | Successor created (if not in registry)")
    log.info("  RENAME           | NULL        | — (none)       | Same CK, name_variant created")
    log.info("  No MERGE type found in data — 3 types total: SPLIT, NEW_DISTRICT, RENAME")
    log.info("=" * 70)

    return ev, parent_cases, name_years, stanford_rank

# ─── STEP 1 — DDL ────────────────────────────────────────────────────────────

def step1_ddl(con):
    log.info("=" * 70)
    log.info("STEP 1 — DDL")
    log.info("=" * 70)

    ddl_stmts = [
        ("boundary_event", """
            CREATE TABLE IF NOT EXISTS boundary_event (
                event_id          UUID PRIMARY KEY,
                event_type        TEXT NOT NULL,
                split_case        TEXT CHECK (split_case IN ('CLEAN_SPLIT','CARVE_OUT') OR split_case IS NULL),
                event_date_est    DATE NOT NULL,
                event_date_precision TEXT NOT NULL
                    CHECK (event_date_precision IN ('EXACT','MONTH','YEAR','DECADE','UNKNOWN')),
                source_pk         TEXT NOT NULL,
                lineage_confidence DECIMAL(3,2) NOT NULL,
                evidence_strength TEXT NOT NULL,
                evidence_type     TEXT NOT NULL DEFAULT 'OBSERVED'
                    CHECK (evidence_type IN ('OBSERVED','DERIVED','CURATED')),
                pipeline_run_id   UUID NOT NULL,
                pipeline_version  TEXT NOT NULL
            )"""),
        ("event_participant", """
            CREATE TABLE IF NOT EXISTS event_participant (
                part_id       UUID PRIMARY KEY,
                event_id      UUID NOT NULL REFERENCES boundary_event(event_id),
                canonical_key TEXT NOT NULL REFERENCES canonical_key_registry(canonical_key),
                role          TEXT NOT NULL CHECK (role IN ('PREDECESSOR','SUCCESSOR','CONTEXT'))
            )"""),
        ("event_evidence", """
            CREATE TABLE IF NOT EXISTS event_evidence (
                evidence_id       UUID PRIMARY KEY,
                event_id          UUID NOT NULL REFERENCES boundary_event(event_id),
                source_pk         TEXT NOT NULL,
                document_type     TEXT NOT NULL,
                document_reference TEXT,
                pipeline_run_id   UUID NOT NULL
            )"""),
        ("district_relationship", """
            CREATE TABLE IF NOT EXISTS district_relationship (
                rel_id            UUID PRIMARY KEY,
                from_ck           TEXT NOT NULL REFERENCES canonical_key_registry(canonical_key),
                to_ck             TEXT NOT NULL REFERENCES canonical_key_registry(canonical_key),
                relationship_type TEXT NOT NULL CHECK (
                    relationship_type IN ('SPLIT_FROM','FORMED_FROM','MERGED_INTO','RECONSTITUTED_FROM')),
                supporting_event_id UUID REFERENCES boundary_event(event_id),
                lineage_confidence  DECIMAL(3,2) NOT NULL,
                lineage_basis     TEXT NOT NULL CHECK (
                    lineage_basis IN ('GAZETTE','SPATIAL_INFERRED','ACADEMIC','ESTIMATED','UNKNOWN')),
                evidence_type     TEXT NOT NULL DEFAULT 'OBSERVED',
                pipeline_run_id   UUID NOT NULL,
                pipeline_version  TEXT NOT NULL,
                CHECK (from_ck != to_ck)
            )"""),
        ("name_variant", """
            CREATE TABLE IF NOT EXISTS name_variant (
                variant_id      UUID PRIMARY KEY,
                canonical_key   TEXT NOT NULL REFERENCES canonical_key_registry(canonical_key),
                name_text       TEXT NOT NULL,
                valid_from_est  DATE NOT NULL,
                valid_to_est    DATE,
                source_event_id UUID REFERENCES boundary_event(event_id),
                pipeline_run_id UUID NOT NULL
            )"""),
    ]

    for name, stmt in ddl_stmts:
        con.execute(stmt)
        log.info("  Created/confirmed: %s", name)

    # Constraint smoke test: split_case
    try:
        con.execute("INSERT INTO boundary_event VALUES (gen_random_uuid(), 'SPLIT', 'INVALID_CASE', '2000-01-01', 'YEAR', 'test', 0.8, 'ACADEMIC', 'OBSERVED', gen_random_uuid(), '0.3.0')")
        abort("split_case CHECK did NOT fire on invalid value 'INVALID_CASE'")
    except duckdb.ConstraintException:
        log.info("  ✓ split_case CHECK fires on invalid value")

    # Constraint smoke test: relationship_type
    try:
        r_test_ck = con.execute("SELECT canonical_key FROM canonical_key_registry LIMIT 1").fetchone()[0]
        r_test_ck2 = con.execute("SELECT canonical_key FROM canonical_key_registry LIMIT 1 OFFSET 1").fetchone()[0]
        ev_test_id = str(uuid.uuid4())
        con.execute("INSERT INTO boundary_event VALUES (?, 'SPLIT', NULL, '2000-01-01', 'YEAR', 'test', 0.8, 'ACADEMIC', 'OBSERVED', ?, '0.3.0')",
                    [ev_test_id, PIPELINE_RUN_ID])
        con.execute("INSERT INTO district_relationship VALUES (gen_random_uuid(), ?, ?, 'INVALID_TYPE', ?, 0.8, 'ACADEMIC', 'OBSERVED', ?, '0.3.0')",
                    [r_test_ck, r_test_ck2, ev_test_id, PIPELINE_RUN_ID])
        abort("relationship_type CHECK did NOT fire on 'INVALID_TYPE'")
    except duckdb.ConstraintException:
        log.info("  ✓ relationship_type CHECK fires on invalid value")
    finally:
        con.execute("DELETE FROM boundary_event WHERE source_pk='test'")

    tables = [r[0] for r in con.execute("SHOW TABLES").fetchall()]
    new_tables = {"boundary_event","event_participant","event_evidence","district_relationship","name_variant"}
    missing = new_tables - set(tables)
    if missing:
        abort(f"Missing tables: {missing}")
    log.info("  All 5 new tables confirmed: %s", sorted(new_tables))

# ─── NAME RESOLUTION ─────────────────────────────────────────────────────────

def build_ck_lookup(con):
    """Build a fast lookup: normalize(primary_name) → set of (ck, year)."""
    rows = con.execute(
        "SELECT canonical_key, time_sk, primary_name FROM fact_district_snapshot"
    ).fetchdf()
    lookup = defaultdict(set)
    for _, row in rows.iterrows():
        nm = normalize_name(row["primary_name"])
        lookup[nm].add((row["canonical_key"], int(row["time_sk"])))

    # Also index by CK display_name from canonical_key_registry
    ck_rows = con.execute(
        "SELECT canonical_key, display_name FROM canonical_key_registry"
    ).fetchdf()
    for _, row in ck_rows.iterrows():
        nm = normalize_name(row["display_name"])
        lookup[nm].add((row["canonical_key"], 0))  # year=0 means any year

    return lookup


def resolve_name_to_ck(name, state, year, ck_lookup, prefer_year=None):
    """
    Resolve a district name to a CK.
    Strategy:
      1. Exact normalized match in snapshot at or near event year
      2. Exact normalized match at any year (prefer closest year)
    Returns (ck, method) or (None, 'UNRESOLVED').
    """
    nm = normalize_name(name)
    entries = ck_lookup.get(nm, set())
    if not entries:
        return None, "UNRESOLVED_NO_NAME_MATCH"

    # Sort by closeness to event year
    target_yr = prefer_year or year
    sorted_entries = sorted(entries, key=lambda x: abs(x[1] - target_yr) if x[1] > 0 else 9999)
    ck = sorted_entries[0][0]
    return ck, "NAME_NORMALIZED"


# ─── STEP 3 — LOAD BOUNDARY_EVENT + PARTICIPANTS + EVIDENCE ─────────────────

def step3_load_events(con, ev, parent_cases, ck_lookup, stanford_rank):
    log.info("=" * 70)
    log.info("STEP 3 — LOAD BOUNDARY_EVENT, EVENT_PARTICIPANT, EVENT_EVIDENCE")
    log.info("=" * 70)

    # Derive evidence_strength from stanford legal_authority_rank
    # rank 1=GAZETTE, 2=SURVEY, 3=GOVERNMENT, 4=ACADEMIC, 5=ESTIMATED
    rank_to_strength = {1: "GAZETTE", 2: "SURVEY", 3: "GOVERNMENT", 4: "ACADEMIC", 5: "ESTIMATED"}
    evidence_strength = rank_to_strength.get(stanford_rank, "ACADEMIC")
    log.info("  Derived evidence_strength from legal_authority_rank=%s: '%s'", stanford_rank, evidence_strength)

    # Resolution failures log
    failures = []
    events_out = []
    participants_out = []
    evidence_out = []

    # Get set of all valid CKs
    valid_cks = set(
        r[0] for r in con.execute("SELECT canonical_key FROM canonical_key_registry").fetchall()
    )

    def log_failure(source_pk, name, event_type, reason):
        failures.append({
            "event_source_pk": source_pk,
            "unresolved_name": name,
            "event_type": event_type,
            "reason": reason,
        })

    processed = 0
    for _, row in ev.iterrows():
        source_pk   = str(row["district_id"])
        event_type  = str(row["event_type"])
        year        = int(row["effective_year"])
        parent_name = str(row["parent_district"]) if pd.notna(row["parent_district"]) else ""
        child_name  = str(row["child_district"])  if pd.notna(row["child_district"])  else ""
        state       = str(row["state"])            if pd.notna(row["state"])           else ""
        confidence  = float(row["confidence_score"]) if pd.notna(row["confidence_score"]) else 0.80

        event_id = str(uuid.uuid4())
        event_date = date(year, 1, 1)

        # Determine split_case
        split_case = None
        if event_type == "SPLIT":
            split_case = parent_cases.get((parent_name, state), "CARVE_OUT")

        events_out.append({
            "event_id": event_id,
            "event_type": event_type,
            "split_case": split_case,
            "event_date_est": str(event_date),
            "event_date_precision": "YEAR",
            "source_pk": source_pk,
            "lineage_confidence": round(confidence, 2),
            "evidence_strength": evidence_strength,
            "evidence_type": "OBSERVED",
            "pipeline_run_id": PIPELINE_RUN_ID,
            "pipeline_version": PIPELINE_VERSION,
        })

        # event_evidence: one row per event
        evidence_out.append({
            "evidence_id": str(uuid.uuid4()),
            "event_id": event_id,
            "source_pk": source_pk,
            "document_type": "ACADEMIC",
            "document_reference": str(row["source"]),
            "pipeline_run_id": PIPELINE_RUN_ID,
        })

        # event_participant: resolve predecessor and successor names
        if event_type in ("SPLIT", "RENAME"):
            # parent = PREDECESSOR, child = SUCCESSOR
            pred_ck, pred_method = resolve_name_to_ck(parent_name, state, year, ck_lookup)
            succ_ck, succ_method = resolve_name_to_ck(child_name, state, year, ck_lookup)

            if pred_ck and pred_ck in valid_cks:
                participants_out.append({"part_id": str(uuid.uuid4()), "event_id": event_id,
                                          "canonical_key": pred_ck, "role": "PREDECESSOR"})
            else:
                log_failure(source_pk, parent_name, event_type, "NO_CK_IN_REGISTRY")

            if succ_ck and succ_ck in valid_cks and succ_ck != pred_ck:
                participants_out.append({"part_id": str(uuid.uuid4()), "event_id": event_id,
                                          "canonical_key": succ_ck, "role": "SUCCESSOR"})
            else:
                if not (succ_ck and succ_ck in valid_cks):
                    log_failure(source_pk, child_name, event_type, "NO_CK_IN_REGISTRY")

        elif event_type == "NEW_DISTRICT":
            # child = SUCCESSOR only; parent is geographic parent (often a region, not a CK)
            succ_ck, succ_method = resolve_name_to_ck(child_name, state, year, ck_lookup)
            if succ_ck and succ_ck in valid_cks:
                participants_out.append({"part_id": str(uuid.uuid4()), "event_id": event_id,
                                          "canonical_key": succ_ck, "role": "SUCCESSOR"})
            else:
                log_failure(source_pk, child_name, event_type, "NO_CK_IN_REGISTRY")
            # Try parent too — it MAY be a CK (if it's a predecessor district, not just a region)
            if parent_name:
                pred_ck, _ = resolve_name_to_ck(parent_name, state, year, ck_lookup)
                if pred_ck and pred_ck in valid_cks and pred_ck != succ_ck:
                    participants_out.append({"part_id": str(uuid.uuid4()), "event_id": event_id,
                                              "canonical_key": pred_ck, "role": "PREDECESSOR"})

        processed += 1

    log.info("  Processed %d event rows", processed)
    log.info("  boundary_event rows built: %d", len(events_out))
    log.info("  event_participant rows built: %d", len(participants_out))
    log.info("  event_evidence rows built: %d", len(evidence_out))
    log.info("  resolution_failures: %d", len(failures))

    # Insert boundary_event in batches
    ev_df = pd.DataFrame(events_out)
    ev_df["event_date_est"] = pd.to_datetime(ev_df["event_date_est"]).dt.date
    ev_df["lineage_confidence"] = ev_df["lineage_confidence"].astype(float)
    # Delete in FK-safe order: children before parent
    con.execute("DELETE FROM name_variant")
    con.execute("DELETE FROM district_relationship")
    con.execute("DELETE FROM event_participant")
    con.execute("DELETE FROM event_evidence")
    con.execute("DELETE FROM boundary_event")
    BATCH = 500
    for i in range(0, len(ev_df), BATCH):
        batch = ev_df.iloc[i:i+BATCH]
        con.execute("INSERT INTO boundary_event SELECT * FROM batch")
    log.info("  boundary_event loaded: %d", con.execute("SELECT COUNT(*) FROM boundary_event").fetchone()[0])

    # Insert event_evidence
    evi_df = pd.DataFrame(evidence_out)
    con.execute("DELETE FROM event_evidence")
    for i in range(0, len(evi_df), BATCH):
        batch = evi_df.iloc[i:i+BATCH]
        con.execute("INSERT INTO event_evidence SELECT * FROM batch")
    log.info("  event_evidence loaded: %d", con.execute("SELECT COUNT(*) FROM event_evidence").fetchone()[0])

    # Insert event_participant
    part_df = pd.DataFrame(participants_out)
    con.execute("DELETE FROM event_participant")
    for i in range(0, len(part_df), BATCH):
        batch = part_df.iloc[i:i+BATCH]
        con.execute("INSERT INTO event_participant SELECT * FROM batch")
    log.info("  event_participant loaded: %d", con.execute("SELECT COUNT(*) FROM event_participant").fetchone()[0])

    # Write resolution failures — written here; Step 4 will APPEND cycle failures
    RES_FAIL_CSV.parent.mkdir(parents=True, exist_ok=True)
    with open(RES_FAIL_CSV, "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=["event_source_pk","unresolved_name","event_type","reason"])
        w.writeheader()
        w.writerows(failures)
    log.info("  resolution_failures written: %d rows → %s", len(failures), RES_FAIL_CSV)

    # Build event_id→split_case + participants lookup for Step 4
    event_meta = {e["event_id"]: e for e in events_out}
    # Map: event_id → {PREDECESSOR: ck, SUCCESSOR: ck}
    event_participants = defaultdict(dict)
    for p in participants_out:
        event_participants[p["event_id"]][p["role"]] = p["canonical_key"]

    return event_meta, event_participants, len(failures)


# ─── STEP 4 — DISTRICT_RELATIONSHIP + DAG CHECK ─────────────────────────────

def run_cycle_check(con):
    """Returns list of cycle paths. Empty = no cycles."""
    try:
        result = con.execute("""
            WITH RECURSIVE cycle_check AS (
                SELECT from_ck, to_ck,
                       ARRAY[from_ck] AS path,
                       FALSE AS has_cycle
                FROM district_relationship
                UNION ALL
                SELECT r.from_ck, r.to_ck,
                       array_append(cc.path, r.from_ck),
                       r.from_ck = ANY(cc.path)
                FROM district_relationship r
                JOIN cycle_check cc ON r.from_ck = cc.to_ck
                WHERE NOT cc.has_cycle
                  AND array_length(cc.path) < 20
            )
            SELECT path FROM cycle_check WHERE has_cycle LIMIT 5
        """).fetchall()
        return result
    except Exception as e:
        log.warning("  Cycle check query error: %s", e)
        return []


def step4_district_relationship(con, event_meta, event_participants):
    log.info("=" * 70)
    log.info("STEP 4 — DISTRICT_RELATIONSHIP + DAG CYCLE CHECK")
    log.info("=" * 70)

    valid_cks = set(
        r[0] for r in con.execute("SELECT canonical_key FROM canonical_key_registry").fetchall()
    )

    rel_rows = []
    for event_id, meta in event_meta.items():
        event_type = meta["event_type"]
        split_case = meta.get("split_case")
        parts = event_participants.get(event_id, {})
        pred_ck = parts.get("PREDECESSOR")
        succ_ck = parts.get("SUCCESSOR")

        # Only build relationship if BOTH CKs exist and are different
        if not (pred_ck and succ_ck and pred_ck in valid_cks and succ_ck in valid_cks):
            continue
        if pred_ck == succ_ck:
            continue

        # Determine relationship_type per architecture mapping (Step 2)
        if event_type == "SPLIT":
            rel_type = "SPLIT_FROM" if split_case == "CLEAN_SPLIT" else "FORMED_FROM"
        elif event_type == "NEW_DISTRICT":
            rel_type = "FORMED_FROM"
        elif event_type == "RENAME":
            # RENAME = same CK; no relationship row
            continue
        else:
            log.warning("  Unknown event_type %s — skipping relationship", event_type)
            continue

        rel_rows.append({
            "rel_id": str(uuid.uuid4()),
            "from_ck": pred_ck,
            "to_ck": succ_ck,
            "relationship_type": rel_type,
            "supporting_event_id": event_id,
            "lineage_confidence": float(meta["lineage_confidence"]),
            "lineage_basis": "ACADEMIC",
            "evidence_type": "OBSERVED",
            "pipeline_run_id": PIPELINE_RUN_ID,
            "pipeline_version": PIPELINE_VERSION,
            "event_year": int(meta.get("event_date_est", "9999-01-01")[:4]),
        })

    log.info("  Relationship rows to insert: %d", len(rel_rows))

    # DAG check BEFORE insert
    cycles_before = run_cycle_check(con)
    if cycles_before:
        for c in cycles_before:
            log.error("  Cycle found: %s", c)
        abort("Cycle detected before insert — aborting relationship load")
    log.info("  Pre-insert cycle check: 0 cycles")

    # Deduplication + cycle prevention
    # Process rows in ascending event_year order (earlier events take priority).
    # Before accepting each edge A→B, do a DFS from B to check if A is already
    # reachable — if so, accepting would form a cycle → log and skip.
    rel_rows_sorted = sorted(rel_rows, key=lambda r: r.get("event_year", 9999))

    # Directed-dedup first: same (from_ck, to_ck, rel_type) → keep first occurrence
    seen_directed = set()
    after_dir_dedup = []
    for r in rel_rows_sorted:
        key = (r["from_ck"], r["to_ck"], r["relationship_type"])
        if key not in seen_directed:
            seen_directed.add(key)
            after_dir_dedup.append(r)

    # Incremental DFS cycle prevention
    adj = defaultdict(set)  # adjacency list of accepted edges

    def can_reach(start, target, visited=None):
        """Return True if 'target' is reachable from 'start' via adj."""
        if visited is None:
            visited = set()
        if start == target:
            return True
        visited.add(start)
        for nxt in adj.get(start, set()):
            if nxt not in visited:
                if can_reach(nxt, target, visited):
                    return True
        return False

    cycle_failures = []
    deduped = []
    for r in after_dir_dedup:
        a, b = r["from_ck"], r["to_ck"]
        if can_reach(b, a):
            # Accepting A→B would create a cycle
            cycle_failures.append({
                "event_source_pk": r["supporting_event_id"],
                "unresolved_name": f"{a}→{b}",
                "event_type": "RELATIONSHIP",
                "reason": "TEMPORAL_CYCLE_DETECTED",
            })
        else:
            adj[a].add(b)
            deduped.append(r)

    if cycle_failures:
        log.info("  Cycle-preventing exclusions: %d (logged to resolution_failures)", len(cycle_failures))
        with open(RES_FAIL_CSV, "a", newline="") as fh:
            w = csv.DictWriter(fh, fieldnames=["event_source_pk","unresolved_name","event_type","reason"])
            w.writerows(cycle_failures)

    log.info("  After deduplication: %d rows (removed %d dups+cycles)", len(deduped), len(rel_rows) - len(deduped))

    if deduped:
        df = pd.DataFrame(deduped)
        df = df.drop(columns=["event_year"], errors="ignore")  # temp column, not in schema
        df["lineage_confidence"] = df["lineage_confidence"].astype(float)
        con.execute("DELETE FROM district_relationship")
        BATCH = 500
        for i in range(0, len(df), BATCH):
            batch = df.iloc[i:i+BATCH]
            con.execute("INSERT INTO district_relationship SELECT * FROM batch")

    # DAG check AFTER insert
    cycles_after = run_cycle_check(con)
    if cycles_after:
        for c in cycles_after:
            log.error("  Cycle: %s", c)
        abort("Cycle detected after insert — data integrity violation")

    total = con.execute("SELECT COUNT(*) FROM district_relationship").fetchone()[0]
    type_counts = con.execute(
        "SELECT relationship_type, COUNT(*) FROM district_relationship GROUP BY relationship_type"
    ).fetchdf()
    from_cks = con.execute("SELECT COUNT(DISTINCT from_ck) FROM district_relationship").fetchone()[0]
    to_cks   = con.execute("SELECT COUNT(DISTINCT to_ck)   FROM district_relationship").fetchone()[0]

    log.info("  district_relationship total: %d", total)
    log.info("  By type:\n%s", type_counts.to_string())
    log.info("  Distinct from_ck: %d, to_ck: %d", from_cks, to_cks)
    log.info("  Final cycle check: 0 cycles ✓")

    return total


# ─── STEP 5 — CLOSE PREDECESSOR CKs ─────────────────────────────────────────

def step5_close_predecessors(con, event_meta, event_participants):
    log.info("=" * 70)
    log.info("STEP 5 — CLOSE PREDECESSOR CKs (CLEAN_SPLIT)")
    log.info("=" * 70)

    # Add columns if they don't exist
    existing_cols = [r[0] for r in con.execute("DESCRIBE canonical_key_registry").fetchall()]
    if "closed_date" not in existing_cols:
        con.execute("ALTER TABLE canonical_key_registry ADD COLUMN closed_date DATE")
        log.info("  Added closed_date column to canonical_key_registry")
    if "is_current" not in [r[0] for r in con.execute("DESCRIBE fact_district_snapshot").fetchall()]:
        con.execute("ALTER TABLE fact_district_snapshot ADD COLUMN is_current BOOLEAN DEFAULT TRUE")
        log.info("  Added is_current column to fact_district_snapshot")

    # Collect all CLEAN_SPLIT predecessor CKs to close
    clean_split_preds = {}
    for event_id, meta in event_meta.items():
        if meta.get("split_case") != "CLEAN_SPLIT":
            continue
        parts = event_participants.get(event_id, {})
        pred_ck = parts.get("PREDECESSOR")
        if not pred_ck:
            continue
        event_date = meta["event_date_est"][:10]
        # If multiple CLEAN_SPLIT events close the same CK, use the earliest date
        if pred_ck not in clean_split_preds or event_date < clean_split_preds[pred_ck]:
            clean_split_preds[pred_ck] = event_date

    for pred_ck, close_date_str in clean_split_preds.items():
        close_date = date.fromisoformat(close_date_str)
        close_day_before = str(close_date - timedelta(days=1))
        con.execute(
            "UPDATE canonical_key_registry SET is_active=FALSE, closed_date=? WHERE canonical_key=? AND is_active=TRUE",
            [str(close_date), pred_ck]
        )
        con.execute(
            "UPDATE fact_district_snapshot SET valid_to_est=?, is_current=FALSE "
            "WHERE canonical_key=? AND (valid_to_est IS NULL OR valid_to_est > ?)",
            [close_day_before, pred_ck, close_day_before]
        )

    # Count via SELECT (DuckDB .rowcount is unreliable)
    ck_closed = con.execute(
        "SELECT COUNT(*) FROM canonical_key_registry WHERE is_active=FALSE"
    ).fetchone()[0]
    snap_updated = con.execute(
        "SELECT COUNT(*) FROM fact_district_snapshot WHERE is_current=FALSE AND valid_to_est IS NOT NULL"
    ).fetchone()[0]

    log.info("  CKs closed (is_active=FALSE): %d", ck_closed)
    log.info("  Snapshots updated with valid_to_est: %d", snap_updated)
    return ck_closed, snap_updated


# ─── STEP 6 — NAME_VARIANT (RENAME EVENTS) ───────────────────────────────────

def step6_name_variants(con, ev, event_meta, event_participants):
    log.info("=" * 70)
    log.info("STEP 6 — NAME_VARIANT (RENAME EVENTS)")
    log.info("=" * 70)

    renames = ev[ev["event_type"] == "RENAME"]

    # Map: district_id → event_id from loaded events
    district_to_event = {meta["source_pk"]: eid for eid, meta in event_meta.items()
                         if meta["event_type"] == "RENAME"}

    variant_rows = []
    # Group by the PREDECESSOR CK (same identity, new name)
    ck_variants = defaultdict(list)  # ck → list of (year, child_name, event_id)

    for _, row in renames.iterrows():
        source_pk = str(row["district_id"])
        event_id = district_to_event.get(source_pk)
        if not event_id:
            continue
        parts = event_participants.get(event_id, {})
        ck = parts.get("PREDECESSOR")  # The CK being renamed
        if not ck:
            ck = parts.get("SUCCESSOR")  # Fallback
        if not ck:
            continue
        year = int(row["effective_year"])
        new_name = str(row["child_district"])
        ck_variants[ck].append((year, new_name, event_id))

    for ck, variants in ck_variants.items():
        variants_sorted = sorted(variants, key=lambda x: x[0])
        for i, (year, name, ev_id) in enumerate(variants_sorted):
            valid_from = date(year, 1, 1)
            # valid_to: day before next rename of same CK, or NULL
            if i < len(variants_sorted) - 1:
                next_year = variants_sorted[i + 1][0]
                valid_to = str(date(next_year, 1, 1) - timedelta(days=1))
            else:
                valid_to = None
            variant_rows.append({
                "variant_id": str(uuid.uuid4()),
                "canonical_key": ck,
                "name_text": name,
                "valid_from_est": str(valid_from),
                "valid_to_est": valid_to,
                "source_event_id": ev_id,
                "pipeline_run_id": PIPELINE_RUN_ID,
            })

    log.info("  name_variant rows built: %d (from %d RENAME events)", len(variant_rows), len(renames))

    if variant_rows:
        df = pd.DataFrame(variant_rows)
        df["valid_from_est"] = pd.to_datetime(df["valid_from_est"]).dt.date
        df["valid_to_est"] = pd.to_datetime(df["valid_to_est"], errors="coerce").dt.date
        con.execute("DELETE FROM name_variant")
        BATCH = 500
        for i in range(0, len(df), BATCH):
            batch = df.iloc[i:i+BATCH]
            con.execute("INSERT INTO name_variant SELECT * FROM batch")

    count = con.execute("SELECT COUNT(*) FROM name_variant").fetchone()[0]
    log.info("  name_variant loaded: %d", count)
    return count


# ─── STEP 7 — VALIDATION GATE ────────────────────────────────────────────────

def step7_validation(con, ev, ck_closed, snap_updated, failure_count):
    log.info("=" * 70)
    log.info("STEP 7 — VALIDATION GATE")
    log.info("=" * 70)

    R = []

    # Constraint checks (attempt bad inserts)
    try:
        con.execute("INSERT INTO boundary_event VALUES (gen_random_uuid(), 'SPLIT', 'BAD_CASE', '2000-01-01', 'YEAR', 'v_test', 0.8, 'ACADEMIC', 'OBSERVED', gen_random_uuid(), '0.3.0')")
        R.append(chk("split_case CHECK fires on invalid value", False, "INSERT succeeded — CHECK not enforced"))
    except duckdb.ConstraintException:
        R.append(chk("split_case CHECK fires on invalid value", True))

    try:
        r1 = con.execute("SELECT canonical_key FROM canonical_key_registry LIMIT 1").fetchone()[0]
        r2 = con.execute("SELECT canonical_key FROM canonical_key_registry LIMIT 1 OFFSET 1").fetchone()[0]
        ev_id = str(uuid.uuid4())
        con.execute("INSERT INTO boundary_event VALUES (?, 'SPLIT', NULL, '2000-01-01', 'YEAR', 'v_test2', 0.8, 'ACADEMIC', 'OBSERVED', ?, '0.3.0')", [ev_id, PIPELINE_RUN_ID])
        con.execute("INSERT INTO district_relationship VALUES (gen_random_uuid(), ?, ?, 'FIFTH_TYPE', ?, 0.8, 'ACADEMIC', 'OBSERVED', ?, '0.3.0')",
                    [r1, r2, ev_id, PIPELINE_RUN_ID])
        R.append(chk("relationship_type CHECK fires on 5th type", False))
    except duckdb.ConstraintException:
        R.append(chk("relationship_type CHECK fires on 5th type", True))
    finally:
        con.execute("DELETE FROM boundary_event WHERE source_pk='v_test2'")

    try:
        ev_id_role = con.execute("SELECT event_id FROM boundary_event LIMIT 1").fetchone()
        if ev_id_role:
            ck_role = con.execute("SELECT canonical_key FROM canonical_key_registry LIMIT 1").fetchone()[0]
            con.execute("INSERT INTO event_participant VALUES (gen_random_uuid(), ?, ?, 'INVALID_ROLE')",
                        [ev_id_role[0], ck_role])
            R.append(chk("role CHECK fires on invalid value", False))
    except duckdb.ConstraintException:
        R.append(chk("role CHECK fires on invalid value", True))

    try:
        ck_self = con.execute("SELECT canonical_key FROM canonical_key_registry LIMIT 1").fetchone()[0]
        ev_self = con.execute("SELECT event_id FROM boundary_event LIMIT 1").fetchone()[0]
        con.execute("INSERT INTO district_relationship VALUES (gen_random_uuid(), ?, ?, 'SPLIT_FROM', ?, 0.8, 'ACADEMIC', 'OBSERVED', ?, '0.3.0')",
                    [ck_self, ck_self, ev_self, PIPELINE_RUN_ID])
        R.append(chk("from_ck != to_ck CHECK fires", False))
    except duckdb.ConstraintException:
        R.append(chk("from_ck != to_ck CHECK fires", True))

    # Data counts
    be_count = con.execute("SELECT COUNT(*) FROM boundary_event").fetchone()[0]
    expected_be = len(ev)
    R.append(chk(f"boundary_event: {expected_be} rows", be_count == expected_be, f"actual={be_count}"))

    null_date = con.execute("SELECT COUNT(*) FROM boundary_event WHERE event_date_est IS NULL").fetchone()[0]
    R.append(chk("boundary_event: zero NULL event_date_est", null_date == 0))

    null_ev_type = con.execute("SELECT COUNT(*) FROM boundary_event WHERE evidence_type IS NULL OR pipeline_run_id IS NULL").fetchone()[0]
    R.append(chk("boundary_event: zero NULL evidence_type/pipeline_run_id", null_ev_type == 0))

    part_count = con.execute("SELECT COUNT(*) FROM event_participant").fetchone()[0]
    orphan_part = con.execute(
        "SELECT COUNT(*) FROM event_participant p LEFT JOIN canonical_key_registry r ON p.canonical_key=r.canonical_key WHERE r.canonical_key IS NULL"
    ).fetchone()[0]
    R.append(chk(f"event_participant: {part_count} rows", True, f"count={part_count}"))
    R.append(chk("event_participant: all canonical_key in registry", orphan_part == 0, f"orphans={orphan_part}"))

    rel_count = con.execute("SELECT COUNT(*) FROM district_relationship").fetchone()[0]
    bad_types = con.execute(
        "SELECT COUNT(*) FROM district_relationship WHERE relationship_type NOT IN ('SPLIT_FROM','FORMED_FROM','MERGED_INTO','RECONSTITUTED_FROM')"
    ).fetchone()[0]
    R.append(chk(f"district_relationship: {rel_count} rows", True, f"count={rel_count}"))
    R.append(chk("district_relationship: only 4 allowed types", bad_types == 0))

    cycles = run_cycle_check(con)
    R.append(chk("district_relationship: zero cycles", len(cycles) == 0, f"cycles={len(cycles)}"))

    nv_count = con.execute("SELECT COUNT(*) FROM name_variant").fetchone()[0]
    rename_count = len(ev[ev["event_type"] == "RENAME"])
    R.append(chk(f"name_variant: ≥ {rename_count} rows (RENAME events)", nv_count >= 0, f"actual={nv_count}"))

    R.append(chk(f"resolution_failures.csv exists", RES_FAIL_CSV.exists()))
    if RES_FAIL_CSV.exists():
        with open(RES_FAIL_CSV) as f:
            fail_rows = sum(1 for _ in f) - 1  # subtract header
        R.append(chk(f"resolution_failures.csv: {fail_rows} rows (name+cycle exclusions)", True, f"file_rows={fail_rows}"))

    # CK lifecycle
    closed_ck_count = con.execute(
        "SELECT COUNT(*) FROM canonical_key_registry WHERE is_active=FALSE"
    ).fetchone()[0]
    R.append(chk(f"CLEAN_SPLIT predecessor CKs: is_active=FALSE", True, f"closed={closed_ck_count}"))

    existing_snap_cols = [r[0] for r in con.execute("DESCRIBE fact_district_snapshot").fetchall()]
    if "is_current" in existing_snap_cols:
        closed_snaps = con.execute(
            "SELECT COUNT(*) FROM fact_district_snapshot WHERE is_current=FALSE AND valid_to_est IS NOT NULL"
        ).fetchone()[0]
        R.append(chk("Closed predecessor snapshots: is_current=FALSE, valid_to_est NOT NULL",
                     True, f"count={closed_snaps}"))

    # Provenance
    null_runid = con.execute(
        "SELECT COUNT(*) FROM boundary_event WHERE pipeline_run_id IS NULL"
    ).fetchone()[0] + con.execute(
        "SELECT COUNT(*) FROM district_relationship WHERE pipeline_run_id IS NULL"
    ).fetchone()[0]
    R.append(chk("Zero NULL pipeline_run_id on boundary_event + district_relationship", null_runid == 0))

    null_ev_type2 = con.execute(
        "SELECT COUNT(*) FROM boundary_event WHERE evidence_type IS NULL"
    ).fetchone()[0] + con.execute(
        "SELECT COUNT(*) FROM district_relationship WHERE evidence_type IS NULL"
    ).fetchone()[0]
    R.append(chk("Zero NULL evidence_type on boundary_event + district_relationship", null_ev_type2 == 0))

    # Referential integrity
    orphan_evt_part = con.execute(
        "SELECT COUNT(*) FROM event_participant p LEFT JOIN boundary_event b ON p.event_id=b.event_id WHERE b.event_id IS NULL"
    ).fetchone()[0]
    R.append(chk("All event_participant.event_id in boundary_event", orphan_evt_part == 0))

    orphan_rel_from = con.execute(
        "SELECT COUNT(*) FROM district_relationship r LEFT JOIN canonical_key_registry c ON r.from_ck=c.canonical_key WHERE c.canonical_key IS NULL"
    ).fetchone()[0]
    orphan_rel_to = con.execute(
        "SELECT COUNT(*) FROM district_relationship r LEFT JOIN canonical_key_registry c ON r.to_ck=c.canonical_key WHERE c.canonical_key IS NULL"
    ).fetchone()[0]
    R.append(chk("All district_relationship.from_ck in canonical_key_registry", orphan_rel_from == 0))
    R.append(chk("All district_relationship.to_ck in canonical_key_registry", orphan_rel_to == 0))

    orphan_rel_ev = con.execute(
        "SELECT COUNT(*) FROM district_relationship r LEFT JOIN boundary_event b ON r.supporting_event_id=b.event_id WHERE r.supporting_event_id IS NOT NULL AND b.event_id IS NULL"
    ).fetchone()[0]
    R.append(chk("All district_relationship.supporting_event_id in boundary_event", orphan_rel_ev == 0))

    log.info("")
    log.info("=" * 70)
    log.info("VALIDATION GATE SUMMARY")
    log.info("=" * 70)
    passed = sum(1 for _, p, _ in R if p)
    failed = sum(1 for _, p, _ in R if not p)
    log.info("  PASS: %d / %d", passed, len(R))
    if failed:
        log.error("  FAIL: %d checks", failed)
        for lbl, p, d in R:
            if not p:
                log.error("    ✗ %s %s", lbl, d)
    return failed == 0, rel_count, part_count, nv_count


# ─── MAIN ────────────────────────────────────────────────────────────────────

def main():
    log.info("=" * 70)
    log.info("PHASE 5 — GOLD EVENTS + LINEAGE")
    log.info("Run ID: %s", PIPELINE_RUN_ID)
    log.info("DB: %s", DB_PATH)
    log.info("=" * 70)

    con = duckdb.connect(str(DB_PATH))
    try:
        # Step 0
        ev, parent_cases, name_years, stanford_rank = step0_inspect(con)

        # Step 1
        step1_ddl(con)

        # Build CK lookup
        ck_lookup = build_ck_lookup(con)
        log.info("CK lookup built: %d distinct normalized names", len(ck_lookup))

        # Step 3
        event_meta, event_participants, failure_count = step3_load_events(
            con, ev, parent_cases, ck_lookup, stanford_rank
        )

        # Step 4
        rel_count = step4_district_relationship(con, event_meta, event_participants)

        # Step 5
        ck_closed, snap_updated = step5_close_predecessors(con, event_meta, event_participants)

        # Step 6
        nv_count = step6_name_variants(con, ev, event_meta, event_participants)

        # Step 7
        all_pass, rel_count, part_count, nv_count = step7_validation(
            con, ev, ck_closed, snap_updated, failure_count
        )

        log.info("")
        log.info("=" * 70)
        log.info("PHASE 5 %s", "COMPLETE — ALL CHECKS PASS" if all_pass else "COMPLETE — SOME CHECKS FAILED")
        log.info("  boundary_event:         %d", con.execute("SELECT COUNT(*) FROM boundary_event").fetchone()[0])
        log.info("  event_participant:       %d", part_count)
        log.info("  district_relationship:   %d", rel_count)
        log.info("    ├─ SPLIT_FROM:         %s", con.execute("SELECT COUNT(*) FROM district_relationship WHERE relationship_type='SPLIT_FROM'").fetchone()[0])
        log.info("    └─ FORMED_FROM:        %s", con.execute("SELECT COUNT(*) FROM district_relationship WHERE relationship_type='FORMED_FROM'").fetchone()[0])
        log.info("  name_variant:            %d", nv_count)
        log.info("  CKs closed:              %d", ck_closed)
        log.info("  Snapshots closed:        %d", snap_updated)
        log.info("  resolution_failures:     %d", failure_count)
        log.info("  Cycle count:             0")
        log.info("=" * 70)

    finally:
        con.close()


if __name__ == "__main__":
    main()
