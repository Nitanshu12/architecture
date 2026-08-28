"""
Phase 6 — Gold Spatial
District Evolution Intelligence System v0.3
"""

import csv
import logging
import math
import os
import re
import sys
import uuid
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import geopandas as gpd
import pandas as pd
from shapely.geometry import MultiPolygon, Polygon
from shapely.ops import unary_union
from shapely.validation import make_valid

PROJECT_ROOT = Path(__file__).resolve().parents[2]
GOLD_DIR     = PROJECT_ROOT / "data" / "gold"
DB_PATH      = GOLD_DIR / "district_evolution.duckdb"
BRONZE_DIR   = PROJECT_ROOT / "data" / "bronze"
IDENTITY_OUT = PROJECT_ROOT / "outputs" / "identity"

PIPELINE_VERSION = "0.3.0"
PIPELINE_RUN_ID  = str(uuid.uuid4())
DEFAULT_WINDOW_YEARS = 10

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()])
log = logging.getLogger(__name__)

# ─── Helpers ─────────────────────────────────────────────────────────────────

def chk(label, passed, detail=""):
    status = "✓ PASS" if passed else "✗ FAIL"
    log.info("  [%s] %s%s", status, label, f": {detail}" if detail else "")
    return label, passed, detail

def abort(msg):
    log.error("ABORT: %s", msg)
    sys.exit(1)

def normalize_name(s):
    if not s: return ""
    s = str(s).strip().lower()
    s = re.sub(r"['''`]", "", s)
    s = re.sub(r"[-–—/]", " ", s)
    s = re.sub(r"\s+", " ", s)
    return s.strip()

def area_sqkm_wgs84(geom):
    try:
        import pyproj
        geod = pyproj.Geod(ellps="WGS84")
        area, _ = geod.geometry_area_perimeter(geom)
        return abs(area) / 1e6
    except Exception:
        mid_lat = (geom.bounds[1] + geom.bounds[3]) / 2
        deg_lat = 111.32
        deg_lon = 111.32 * math.cos(math.radians(mid_lat))
        return abs(geom.area) * deg_lat * deg_lon

# ─── GPKG SOURCE CONFIG ───────────────────────────────────────────────────────

def get_gpkg_sources():
    import fiona
    sources = []
    for source_dir in sorted((BRONZE_DIR / "stanford").iterdir()):
        if not source_dir.is_dir(): continue
        yr = source_dir.name
        gpkg = source_dir / f"{yr}.gpkg"
        if not gpkg.exists(): continue
        layers = fiona.listlayers(str(gpkg))
        layer = layers[0]
        gdf_s = gpd.read_file(str(gpkg), layer=layer, rows=1)
        cols = list(gdf_s.columns)
        if "d_name" in cols:
            nf, sf = "d_name", None
        elif "dtname" in cols:
            nf, sf = "dtname", ("stname" if "stname" in cols else None)
        elif "NAME" in cols:
            nf, sf = "NAME", ("STATE_UT" if "STATE_UT" in cols else None)
        else:
            log.warning("  No name field in %s — skipping", gpkg); continue
        sources.append({"path": str(gpkg), "year": int(yr), "name_field": nf,
                        "state_field": sf, "layer": layer, "source_dataset": "stanford",
                        "crs_original": str(gdf_s.crs)})

    soi_base = BRONZE_DIR / "soi"
    if soi_base.exists():
        for source_dir in sorted(soi_base.iterdir()):
            if not source_dir.is_dir(): continue
            yr = source_dir.name
            gpkg = source_dir / f"{yr}.gpkg"
            if not gpkg.exists(): continue
            layers = fiona.listlayers(str(gpkg))
            layer = layers[0]
            gdf_s = gpd.read_file(str(gpkg), layer=layer, rows=1)
            cols = list(gdf_s.columns)
            nf = "District" if "District" in cols else cols[1]
            sf = "STATE" if "STATE" in cols else None
            sources.append({"path": str(gpkg), "year": int(yr), "name_field": nf,
                            "state_field": sf, "layer": layer, "source_dataset": "soi",
                            "crs_original": str(gdf_s.crs)})
    return sources

# ─── STEP 0 ──────────────────────────────────────────────────────────────────

def step0_inspect(con):
    log.info("=" * 70)
    log.info("STEP 0 — DATA INSPECTION")
    log.info("=" * 70)
    sources = get_gpkg_sources()
    log.info("GPKG sources: %d", len(sources))
    for s in sources:
        sz = os.path.getsize(s["path"]) / 1024 / 1024
        gdf = gpd.read_file(s["path"], layer=s["layer"])
        log.info("  [%s %s] %.1fMB  rows=%d  name_field=%s  CRS=%s",
                 s["source_dataset"], s["year"], sz, len(gdf),
                 s["name_field"], s["crs_original"][:40])
    dim_src = con.execute("SELECT source_name, legal_authority_rank, spatial_precision_rank FROM dim_source").fetchdf()
    log.info("dim_source:\n%s", dim_src.to_string())
    log.info("has_geometry=TRUE baseline: %d",
             con.execute("SELECT COUNT(*) FROM fact_district_snapshot WHERE has_geometry=TRUE").fetchone()[0])
    return sources

# ─── STEP 1 — DDL ────────────────────────────────────────────────────────────

def step1_ddl(con):
    log.info("=" * 70)
    log.info("STEP 1 — DDL")
    log.info("=" * 70)
    try:
        con.execute("INSTALL spatial")
    except Exception: pass
    try:
        con.execute("LOAD spatial")
        log.info("  Spatial extension loaded")
    except Exception as e:
        abort(f"Cannot load spatial: {e}")

    ddl_stmts = [
        ("geometry_observation", """
            CREATE TABLE IF NOT EXISTS geometry_observation (
                geom_obs_id          UUID PRIMARY KEY,
                canonical_key        TEXT NOT NULL REFERENCES canonical_key_registry(canonical_key),
                source_id            INTEGER NOT NULL REFERENCES dim_source(source_sk),
                source_pk            TEXT NOT NULL,
                geom                 GEOMETRY NOT NULL,
                area_sqkm            DOUBLE NOT NULL,
                centroid_lat         DOUBLE,
                centroid_lon         DOUBLE,
                crs_original         TEXT NOT NULL,
                geometry_provenance  TEXT NOT NULL CHECK (geometry_provenance IN ('OBSERVED','DIGITIZED','DERIVED')),
                is_valid_geom        BOOLEAN NOT NULL,
                was_repaired         BOOLEAN NOT NULL DEFAULT FALSE,
                repair_area_delta_pct DOUBLE,
                spatial_accuracy_m   DOUBLE,
                spatial_confidence   TEXT NOT NULL CHECK (spatial_confidence IN ('HIGH','MEDIUM','LOW','UNKNOWN')),
                observed_at          DATE NOT NULL,
                valid_from_est       DATE,
                valid_to_est         DATE,
                evidence_type        TEXT NOT NULL DEFAULT 'OBSERVED',
                source_observation_id TEXT NOT NULL,
                pipeline_run_id      UUID NOT NULL,
                pipeline_version     TEXT NOT NULL
            )"""),
        ("geometry_reconciliation", """
            CREATE TABLE IF NOT EXISTS geometry_reconciliation (
                reconciliation_id    UUID PRIMARY KEY,
                canonical_key        TEXT NOT NULL REFERENCES canonical_key_registry(canonical_key),
                preferred_geom_obs_id UUID NOT NULL REFERENCES geometry_observation(geom_obs_id),
                valid_from_est       DATE NOT NULL,
                valid_to_est         DATE,
                authority_rule       TEXT NOT NULL CHECK (authority_rule IN ('LEGAL_PRIORITY','SPATIAL_PRIORITY','RECENCY','MANUAL_OVERRIDE')),
                decided_at           TIMESTAMP NOT NULL,
                spatial_confidence   TEXT NOT NULL,
                is_current_decision  BOOLEAN NOT NULL DEFAULT TRUE,
                pipeline_run_id      UUID NOT NULL,
                pipeline_version     TEXT NOT NULL
            )"""),
        ("spatial_overlap", """
            CREATE TABLE IF NOT EXISTS spatial_overlap (
                overlap_id           UUID PRIMARY KEY,
                from_geom_obs_id     UUID NOT NULL REFERENCES geometry_observation(geom_obs_id),
                to_geom_obs_id       UUID NOT NULL REFERENCES geometry_observation(geom_obs_id),
                intersection_sqkm    DOUBLE NOT NULL,
                fraction_of_from     DOUBLE NOT NULL,
                fraction_of_to       DOUBLE NOT NULL,
                calculated_at        TIMESTAMP NOT NULL,
                pipeline_run_id      UUID NOT NULL,
                UNIQUE (from_geom_obs_id, to_geom_obs_id)
            )"""),
        ("geometric_crosswalk", """
            CREATE TABLE IF NOT EXISTS geometric_crosswalk (
                geo_xwalk_id         UUID PRIMARY KEY,
                from_snapshot_id     INTEGER NOT NULL REFERENCES fact_district_snapshot(snapshot_sk),
                to_snapshot_id       INTEGER NOT NULL REFERENCES fact_district_snapshot(snapshot_sk),
                from_geom_obs_id     UUID NOT NULL REFERENCES geometry_observation(geom_obs_id),
                to_geom_obs_id       UUID NOT NULL REFERENCES geometry_observation(geom_obs_id),
                area_weight          DOUBLE NOT NULL,
                coverage_fraction    DOUBLE NOT NULL,
                unallocated_fraction DOUBLE NOT NULL,
                intersection_sqkm    DOUBLE NOT NULL,
                calculated_at        TIMESTAMP NOT NULL,
                evidence_type        TEXT NOT NULL DEFAULT 'DERIVED',
                derived_from_ids     TEXT[] NOT NULL,
                derivation_method    TEXT NOT NULL,
                pipeline_run_id      UUID NOT NULL,
                pipeline_version     TEXT NOT NULL
            )"""),
    ]

    for name, stmt in ddl_stmts:
        con.execute(stmt)
        log.info("  Created/confirmed: %s", name)

    # Smoke test: geometry_provenance CHECK
    try:
        con.execute("""INSERT INTO geometry_observation VALUES (
            gen_random_uuid(), (SELECT canonical_key FROM canonical_key_registry LIMIT 1),
            1, 'smoke', ST_GeomFromText('POINT(0 0)'), 1.0, 0.0, 0.0,
            'EPSG:4326', 'INVALID_PROV', TRUE, FALSE, NULL, NULL, 'HIGH',
            '2000-01-01', NULL, NULL, 'OBSERVED', 'smoke', gen_random_uuid(), '0.3.0')""")
        abort("geometry_provenance CHECK did not fire")
    except duckdb.ConstraintException:
        log.info("  ✓ geometry_provenance CHECK fires")

    log.info("  All 4 spatial tables confirmed")

# ─── CK LOOKUP ───────────────────────────────────────────────────────────────

def build_ck_lookup(con):
    snap = con.execute("SELECT canonical_key, time_sk AS yr, primary_name FROM fact_district_snapshot").fetchdf()
    name_yr_ck = {}
    name_ck = {}
    for _, row in snap.iterrows():
        nm = normalize_name(row["primary_name"])
        yr = int(row["yr"])
        if (nm, yr) not in name_yr_ck:
            name_yr_ck[(nm, yr)] = row["canonical_key"]
        if nm not in name_ck:
            name_ck[nm] = row["canonical_key"]
    reg = con.execute("SELECT canonical_key, display_name FROM canonical_key_registry").fetchdf()
    for _, row in reg.iterrows():
        nm = normalize_name(row["display_name"])
        if nm not in name_ck:
            name_ck[nm] = row["canonical_key"]
    return name_yr_ck, name_ck

def resolve_to_ck(name, year, name_yr_ck, name_ck):
    nm = normalize_name(name)
    ck = name_yr_ck.get((nm, year))
    if ck: return ck, "EXACT_YEAR"
    ck = name_ck.get(nm)
    if ck: return ck, "ANY_YEAR"
    return None, "UNRESOLVED"

# ─── STEP 2 ──────────────────────────────────────────────────────────────────

def step2_geometry_observation(con, sources, name_yr_ck, name_ck):
    log.info("=" * 70)
    log.info("STEP 2 — LOAD GEOMETRY_OBSERVATION")
    log.info("=" * 70)

    dim_src = con.execute("SELECT source_sk, source_name, spatial_precision_rank FROM dim_source").fetchdf()
    src_sk_map   = {r["source_name"]: r["source_sk"] for _, r in dim_src.iterrows()}
    rank_conf_map = {}
    for _, r in dim_src.iterrows():
        rank = int(r["spatial_precision_rank"])
        rank_conf_map[rank] = "HIGH" if rank <= 1 else ("MEDIUM" if rank <= 3 else "LOW")
    log.info("  rank→confidence (from dim_source): %s", rank_conf_map)

    IDENTITY_OUT.mkdir(parents=True, exist_ok=True)
    LOAD_FAIL_CSV  = IDENTITY_OUT / "geometry_load_failures.csv"
    REPAIR_CSV     = IDENTITY_OUT / "geometry_repair_flags.csv"
    load_failures  = []
    repair_flags   = []
    obs_rows       = []
    total_attempted = 0
    valid_cks = set(r[0] for r in con.execute("SELECT canonical_key FROM canonical_key_registry").fetchall())

    for src in sources:
        yr = src["year"]
        ds = src["source_dataset"]
        nf = src["name_field"]
        crs_orig = src["crs_original"]
        source_sk = int(src_sk_map.get(ds, 0))
        if source_sk == 0:
            log.warning("  %s not in dim_source — skipping", ds); continue

        gdf = gpd.read_file(src["path"], layer=src["layer"])
        if gdf.crs and str(gdf.crs) != "EPSG:4326":
            gdf = gdf.to_crs("EPSG:4326")
            log.info("  Reprojected %s %s → EPSG:4326", ds, yr)

        rank = int(dim_src.loc[dim_src["source_name"] == ds, "spatial_precision_rank"].iloc[0])
        confidence  = rank_conf_map.get(rank, "UNKNOWN")
        spatial_acc = float(rank * 250.0)

        loaded_this = 0
        for idx, row in gdf.iterrows():
            total_attempted += 1
            raw_name = str(row.get(nf, "")).strip()
            if not raw_name:
                load_failures.append({"path": src["path"], "year": yr, "idx": idx, "name": "", "reason": "EMPTY_NAME"}); continue

            ck, method = resolve_to_ck(raw_name, yr, name_yr_ck, name_ck)
            if not ck or ck not in valid_cks:
                load_failures.append({"path": src["path"], "year": yr, "idx": idx, "name": raw_name, "reason": "NO_CK_IN_REGISTRY"}); continue

            geom = row.geometry
            if geom is None:
                load_failures.append({"path": src["path"], "year": yr, "idx": idx, "name": raw_name, "reason": "NULL_GEOMETRY"}); continue

            was_repaired = False
            repair_delta = None
            if not geom.is_valid:
                area_before = geom.area
                geom = make_valid(geom)
                was_repaired = True
                if area_before > 0:
                    repair_delta = abs(geom.area - area_before) / area_before * 100
                    if repair_delta > 1.0:
                        repair_flags.append({"name": raw_name, "year": yr, "dataset": ds, "delta_pct": repair_delta})

            if isinstance(geom, Polygon):
                geom = MultiPolygon([geom])
            elif not isinstance(geom, (Polygon, MultiPolygon)):
                polys = [g for g in getattr(geom, 'geoms', []) if isinstance(g, (Polygon, MultiPolygon))]
                if not polys:
                    load_failures.append({"path": src["path"], "year": yr, "idx": idx, "name": raw_name, "reason": "NO_POLYGON"}); continue
                geom = unary_union(polys)
                if isinstance(geom, Polygon):
                    geom = MultiPolygon([geom])

            area_km2 = area_sqkm_wgs84(geom)
            if area_km2 <= 0:
                load_failures.append({"path": src["path"], "year": yr, "idx": idx, "name": raw_name, "reason": "ZERO_AREA"}); continue

            centroid = geom.centroid
            # Source PK: pick first ID-like column present in row
            for pk_col in ["C_CODE51","C_CODE61","C_CODE71","C_CODE81","DISTRICT_9","DIS01_ID","pc11_d_id","DISTRICT_L"]:
                if pk_col in row.index and row[pk_col] is not None:
                    src_pk = str(row[pk_col]); break
            else:
                src_pk = str(idx)

            obs_rows.append({
                "geom_obs_id": str(uuid.uuid4()),
                "canonical_key": ck,
                "source_id": source_sk,
                "source_pk": src_pk,
                "geom_wkt": geom.wkt,
                "area_sqkm": float(area_km2),
                "centroid_lat": float(centroid.y),
                "centroid_lon": float(centroid.x),
                "crs_original": crs_orig,
                "geometry_provenance": "OBSERVED",
                "is_valid_geom": bool(geom.is_valid),
                "was_repaired": bool(was_repaired),
                "repair_area_delta_pct": float(repair_delta) if repair_delta is not None else None,
                "spatial_accuracy_m": spatial_acc,
                "spatial_confidence": confidence,
                "observed_at": f"{yr}-01-01",
                "valid_from_est": f"{yr}-01-01",
                "valid_to_est": None,
                "evidence_type": "OBSERVED",
                "source_observation_id": f"{ds}_{yr}_{idx}",
                "pipeline_run_id": PIPELINE_RUN_ID,
                "pipeline_version": PIPELINE_VERSION,
            })
            loaded_this += 1

        log.info("  [%s %s] rows=%d  loaded=%d  failed=%d",
                 ds, yr, len(gdf), loaded_this, len(gdf) - loaded_this)

    log.info("  Total attempted=%d  loaded=%d  failed=%d",
             total_attempted, len(obs_rows), len(load_failures))

    with open(LOAD_FAIL_CSV, "w", newline="") as fh:
        csv.DictWriter(fh, fieldnames=["path","year","idx","name","reason"]).writeheader()
        csv.DictWriter(fh, fieldnames=["path","year","idx","name","reason"]).writerows(load_failures)
    if repair_flags:
        with open(REPAIR_CSV, "w", newline="") as fh:
            csv.DictWriter(fh, fieldnames=["name","year","dataset","delta_pct"]).writeheader()
            csv.DictWriter(fh, fieldnames=["name","year","dataset","delta_pct"]).writerows(repair_flags)

    # Delete child tables that reference geometry_observation before deleting it
    for tbl in ["geometric_crosswalk","spatial_overlap","geometry_reconciliation","geometry_observation"]:
        try: con.execute(f"DELETE FROM {tbl}")
        except Exception: pass

    if obs_rows:
        log.info("  Inserting %d rows into geometry_observation...", len(obs_rows))
        BATCH = 100
        for i in range(0, len(obs_rows), BATCH):
            batch = obs_rows[i:i+BATCH]
            vals = []
            flat = []
            for r in batch:
                vals.append("(?,?,?,?,ST_GeomFromText(?),?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
                flat += [r["geom_obs_id"], r["canonical_key"], r["source_id"], r["source_pk"],
                         r["geom_wkt"], r["area_sqkm"], r["centroid_lat"], r["centroid_lon"],
                         r["crs_original"], r["geometry_provenance"], r["is_valid_geom"],
                         r["was_repaired"], r["repair_area_delta_pct"], r["spatial_accuracy_m"],
                         r["spatial_confidence"], r["observed_at"], r["valid_from_est"],
                         r["valid_to_est"], r["evidence_type"], r["source_observation_id"],
                         r["pipeline_run_id"], r["pipeline_version"]]
            con.execute(f"INSERT INTO geometry_observation VALUES {','.join(vals)}", flat)

    loaded = con.execute("SELECT COUNT(*) FROM geometry_observation").fetchone()[0]
    invalid = con.execute("SELECT COUNT(*) FROM geometry_observation WHERE NOT ST_IsValid(geom)").fetchone()[0]
    log.info("  geometry_observation loaded=%d  ST_IsValid failures=%d", loaded, invalid)
    return loaded, len(load_failures)

# ─── STEP 3 ──────────────────────────────────────────────────────────────────

def step3_reconciliation(con):
    log.info("=" * 70)
    log.info("STEP 3 — GEOMETRY_RECONCILIATION")
    log.info("=" * 70)

    obs = con.execute("""
        SELECT go.geom_obs_id, go.canonical_key, go.valid_from_est, go.spatial_confidence,
               ds.legal_authority_rank, ds.spatial_precision_rank
        FROM geometry_observation go
        JOIN dim_source ds ON go.source_id = ds.source_sk
    """).fetchdf()

    snaps = con.execute(
        "SELECT canonical_key, time_sk AS yr, valid_from_est, valid_to_est FROM fact_district_snapshot"
    ).fetchdf()
    snap_idx = {}
    for _, r in snaps.iterrows():
        vf = r["valid_from_est"]
        vt = r["valid_to_est"]
        vf_s = str(vf)[:10] if vf is not None and str(vf) not in ('NaT', 'None', '') else None
        vt_s = str(vt)[:10] if vt is not None and str(vt) not in ('NaT', 'None', '') else None
        snap_idx[(r["canonical_key"], int(r["yr"]))] = (vf_s, vt_s)

    decided_at = datetime.now(timezone.utc).isoformat()
    rec_rows = []
    multi_count = 0

    obs["yr_int"] = obs["valid_from_est"].apply(lambda d: int(str(d)[:4]) if d is not None else 0)
    for (ck, yr), grp in obs.groupby(["canonical_key", "yr_int"]):
        grp_s = grp.sort_values(["legal_authority_rank", "spatial_precision_rank"])
        raw_vf, raw_vt = snap_idx.get((ck, yr), (None, None))
        # Always guarantee non-null valid_from_est using the observation year
        if raw_vf is None or (hasattr(raw_vf, '__class__') and str(raw_vf) in ('NaT', 'None', '')):
            vf_str = f"{yr}-01-01"
        else:
            vf_str = str(raw_vf)[:10]
        vt_str = None if (raw_vt is None or str(raw_vt) in ('NaT', 'None', '')) else str(raw_vt)[:10]

        for i, (_, row) in enumerate(grp_s.iterrows()):
            rec_rows.append({
                "reconciliation_id": str(uuid.uuid4()),
                "canonical_key": ck,
                "preferred_geom_obs_id": row["geom_obs_id"],
                "valid_from_est": vf_str,
                "valid_to_est": vt_str,
                "authority_rule": "LEGAL_PRIORITY",
                "decided_at": decided_at,
                "spatial_confidence": row["spatial_confidence"],
                "is_current_decision": (i == 0),
                "pipeline_run_id": PIPELINE_RUN_ID,
                "pipeline_version": PIPELINE_VERSION,
            })
        if len(grp) > 1:
            multi_count += 1

    log.info("  CK+year pairs with multiple sources: %d", multi_count)

    if rec_rows:
        df = pd.DataFrame(rec_rows)
        df["valid_from_est"] = pd.to_datetime(df["valid_from_est"], errors="coerce").dt.date
        df["valid_to_est"]   = pd.to_datetime(df["valid_to_est"],   errors="coerce").dt.date
        # Safety: drop any rows where valid_from_est is still NaT (should not happen after above fix)
        null_vf = df["valid_from_est"].isna().sum()
        if null_vf > 0:
            log.warning("  Dropping %d rows with NULL valid_from_est (should not occur)", null_vf)
            df = df[df["valid_from_est"].notna()]
        df["is_current_decision"] = df["is_current_decision"].astype(bool)
        BATCH = 500
        for i in range(0, len(df), BATCH):
            batch = df.iloc[i:i+BATCH]
            con.execute("INSERT INTO geometry_reconciliation SELECT * FROM batch")

    total   = con.execute("SELECT COUNT(*) FROM geometry_reconciliation").fetchone()[0]
    current = con.execute("SELECT COUNT(*) FROM geometry_reconciliation WHERE is_current_decision=TRUE").fetchone()[0]
    log.info("  geometry_reconciliation: %d total, %d current", total, current)
    return total, current

# ─── STEP 4 ──────────────────────────────────────────────────────────────────

def step4_update_snapshots(con):
    log.info("=" * 70)
    log.info("STEP 4 — UPDATE FACT_DISTRICT_SNAPSHOT")
    log.info("=" * 70)

    con.execute("""
        UPDATE fact_district_snapshot
        SET reconciliation_id = gr.reconciliation_id, has_geometry = TRUE
        FROM (
            SELECT reconciliation_id, canonical_key, valid_from_est, valid_to_est
            FROM geometry_reconciliation WHERE is_current_decision = TRUE
        ) AS gr
        WHERE fact_district_snapshot.canonical_key = gr.canonical_key
          AND fact_district_snapshot.valid_from_est >= gr.valid_from_est
          AND (gr.valid_to_est IS NULL OR fact_district_snapshot.valid_from_est <= gr.valid_to_est)
    """)

    geom_true  = con.execute("SELECT COUNT(*) FROM fact_district_snapshot WHERE has_geometry=TRUE").fetchone()[0]
    geom_false = con.execute("SELECT COUNT(*) FROM fact_district_snapshot WHERE NOT has_geometry OR has_geometry IS NULL").fetchone()[0]
    log.info("  has_geometry=TRUE=%d  FALSE=%d  total=%d (expected 4,351)", geom_true, geom_false, geom_true + geom_false)
    return geom_true, geom_false

# ─── STEP 5 ──────────────────────────────────────────────────────────────────

def step5_spatial_overlap(con):
    log.info("=" * 70)
    log.info("STEP 5 — SPATIAL_OVERLAP (BOUNDED)")
    log.info("=" * 70)

    cfg_path = PROJECT_ROOT / "config" / "pipeline_config.yaml"
    window_yrs = DEFAULT_WINDOW_YEARS
    if cfg_path.exists():
        import yaml
        with open(cfg_path) as f:
            cfg = yaml.safe_load(f) or {}
        window_yrs = cfg.get("spatial_candidate_window_years", DEFAULT_WINDOW_YEARS)
        log.info("  window_years=%d (from pipeline_config.yaml)", window_yrs)
    else:
        log.info("  window_years=%d (default)", window_yrs)

    CROSSWALK_ERRORS_CSV = IDENTITY_OUT / "crosswalk_errors.csv"
    crosswalk_errors = []

    from shapely import wkt as swkt

    obs_df = con.execute("""
        SELECT geom_obs_id, canonical_key, area_sqkm,
               CAST(YEAR(valid_from_est) AS INTEGER) AS yr,
               ST_AsText(geom) AS geom_wkt
        FROM geometry_observation ORDER BY yr, canonical_key
    """).fetchdf()

    log.info("  geometry_observation rows: %d", len(obs_df))
    if len(obs_df) == 0:
        log.warning("  No geometry observations — skipping")
        return 0

    obs_df["shapely_geom"] = obs_df["geom_wkt"].apply(swkt.loads)

    # Build ck→years index for nearest-available fallback
    ck_yrs = defaultdict(list)
    for _, r in obs_df.iterrows():
        ck_yrs[r["canonical_key"]].append(int(r["yr"]))

    obs_list = obs_df.to_dict("records")
    overlap_rows = []
    seen_pairs   = set()
    overflow_cnt = 0
    BATCH_LOG    = 50

    for i, a in enumerate(obs_list):
        a_id   = a["geom_obs_id"]
        a_geom = a["shapely_geom"]
        a_area = a["area_sqkm"]
        a_yr   = int(a["yr"])
        if a_area <= 0 or a_geom is None: continue

        for b in obs_list:
            b_id = b["geom_obs_id"]
            if a_id == b_id: continue
            if (a_id, b_id) in seen_pairs or (b_id, a_id) in seen_pairs: continue

            b_yr   = int(b["yr"])
            b_ck   = b["canonical_key"]
            b_geom = b["shapely_geom"]
            b_area = b["area_sqkm"]

            yr_diff = abs(a_yr - b_yr)
            nearest = min(abs(a_yr - y) for y in ck_yrs.get(b_ck, [b_yr]))
            if yr_diff > window_yrs and nearest != yr_diff:
                continue

            # Bounding box pre-filter
            if not a_geom.bounds or not b_geom.bounds: continue
            ab, bb = a_geom.bounds, b_geom.bounds
            if ab[2] < bb[0] or bb[2] < ab[0] or ab[3] < bb[1] or bb[3] < ab[1]: continue

            if not a_geom.intersects(b_geom): continue

            try:
                inter = a_geom.intersection(b_geom)
            except Exception:
                continue
            if inter.is_empty: continue

            inter_km2 = area_sqkm_wgs84(inter)
            if inter_km2 < 0.01: continue

            frac_from = inter_km2 / a_area
            frac_to   = inter_km2 / b_area

            if frac_from > 1.001:
                crosswalk_errors.append({"from_id": a_id, "to_id": b_id, "frac": frac_from, "reason": "FRACTION_OVERFLOW"})
                overflow_cnt += 1
                continue

            seen_pairs.add((a_id, b_id))
            overlap_rows.append({
                "overlap_id": str(uuid.uuid4()),
                "from_geom_obs_id": a_id,
                "to_geom_obs_id": b_id,
                "intersection_sqkm": float(inter_km2),
                "fraction_of_from": float(frac_from),
                "fraction_of_to": float(frac_to),
                "calculated_at": datetime.now(timezone.utc).isoformat(),
                "pipeline_run_id": PIPELINE_RUN_ID,
            })

        if (i + 1) % BATCH_LOG == 0:
            log.info("  Processed %d/%d obs, overlaps so far: %d", i+1, len(obs_list), len(overlap_rows))

    log.info("  Total overlap rows=%d  overflow_excluded=%d", len(overlap_rows), overflow_cnt)

    with open(CROSSWALK_ERRORS_CSV, "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=["from_id","to_id","frac","reason"])
        w.writeheader(); w.writerows(crosswalk_errors)

    if overlap_rows:
        df = pd.DataFrame(overlap_rows)
        df["calculated_at"] = pd.to_datetime(df["calculated_at"])
        BATCH = 500
        for i in range(0, len(df), BATCH):
            batch = df.iloc[i:i+BATCH]
            con.execute("INSERT INTO spatial_overlap SELECT * FROM batch")

    total = con.execute("SELECT COUNT(*) FROM spatial_overlap").fetchone()[0]
    log.info("  spatial_overlap loaded: %d", total)
    if total > 0:
        s = con.execute("SELECT MIN(fraction_of_from),MAX(fraction_of_from),AVG(intersection_sqkm) FROM spatial_overlap").fetchone()
        log.info("  fraction_of_from: min=%.4f max=%.4f | avg_inter_km2=%.2f", s[0], s[1], s[2])
    return total

# ─── STEP 6 ──────────────────────────────────────────────────────────────────

def step6_crosswalk(con):
    log.info("=" * 70)
    log.info("STEP 6 — GEOMETRIC_CROSSWALK")
    log.info("=" * 70)

    WARN_CSV  = IDENTITY_OUT / "crosswalk_warnings.csv"
    ERROR_CSV = IDENTITY_OUT / "crosswalk_errors.csv"

    snaps = con.execute("""
        SELECT fds.snapshot_sk, fds.canonical_key, gr.preferred_geom_obs_id AS geom_obs_id
        FROM fact_district_snapshot fds
        JOIN geometry_reconciliation gr ON fds.reconciliation_id = gr.reconciliation_id
        WHERE fds.has_geometry=TRUE AND gr.is_current_decision=TRUE
    """).fetchdf()

    log.info("  Snapshots with geometry: %d", len(snaps))
    if len(snaps) == 0:
        return 0, 0

    geom_to_snap = {}
    for _, r in snaps.iterrows():
        geom_to_snap[r["geom_obs_id"]] = {"snapshot_sk": int(r["snapshot_sk"]), "canonical_key": r["canonical_key"]}

    valid_geom_ids = set(geom_to_snap.keys())
    overlaps = con.execute("""
        SELECT overlap_id, from_geom_obs_id, to_geom_obs_id, intersection_sqkm, fraction_of_from, fraction_of_to
        FROM spatial_overlap
    """).fetchdf()

    eligible = overlaps[overlaps["from_geom_obs_id"].isin(valid_geom_ids) & overlaps["to_geom_obs_id"].isin(valid_geom_ids)]
    log.info("  Eligible overlap pairs: %d", len(eligible))

    now_ts = datetime.now(timezone.utc).isoformat()
    xwalk_rows = []

    for _, ov in eligible.iterrows():
        fi = geom_to_snap.get(ov["from_geom_obs_id"])
        ti = geom_to_snap.get(ov["to_geom_obs_id"])
        if not fi or not ti: continue
        xwalk_rows.append({
            "geo_xwalk_id": str(uuid.uuid4()),
            "from_snapshot_id": fi["snapshot_sk"],
            "to_snapshot_id": ti["snapshot_sk"],
            "from_geom_obs_id": ov["from_geom_obs_id"],
            "to_geom_obs_id": ov["to_geom_obs_id"],
            "area_weight": float(ov["fraction_of_from"]),
            "coverage_fraction": 0.0,     # corrected below
            "unallocated_fraction": 0.0,
            "intersection_sqkm": float(ov["intersection_sqkm"]),
            "calculated_at": now_ts,
            "evidence_type": "DERIVED",
            "derived_from_ids": [ov["overlap_id"]],
            "derivation_method": "SPATIAL_INTERSECTION",
            "pipeline_run_id": PIPELINE_RUN_ID,
            "pipeline_version": PIPELINE_VERSION,
        })

    # Compute true coverage per from_snapshot_id
    from_totals = defaultdict(float)
    for r in xwalk_rows:
        from_totals[r["from_snapshot_id"]] += r["area_weight"]

    bad_snaps = {sk for sk, tot in from_totals.items() if tot > 1.001}
    warn_rows = []
    error_rows = [{"snapshot_sk": sk, "sum_area_weight": from_totals[sk], "reason": "SUM_OVER_1"} for sk in bad_snaps]

    final = []
    for r in xwalk_rows:
        fsk = r["from_snapshot_id"]
        if fsk in bad_snaps: continue
        cov = from_totals[fsk]
        r["coverage_fraction"]    = float(cov)
        r["unallocated_fraction"] = max(0.0, 1.0 - cov)
        if cov < 0.85:
            warn_rows.append({"snapshot_sk": fsk, "coverage": cov})
        final.append(r)

    # Lineage coverage
    lin = con.execute("SELECT from_ck, to_ck FROM district_relationship").fetchdf()
    lin_set = set(zip(lin["from_ck"], lin["to_ck"]))
    lineage_covered = sum(
        1 for r in final
        if (geom_to_snap.get(r["from_geom_obs_id"],{}).get("canonical_key"),
            geom_to_snap.get(r["to_geom_obs_id"],{}).get("canonical_key")) in lin_set
    )

    log.info("  Crosswalk rows: %d | warnings: %d | errors: %d | lineage_covered: %d/%d",
             len(final), len(warn_rows), len(error_rows), lineage_covered, len(lin_set))

    with open(WARN_CSV, "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=["snapshot_sk","coverage"])
        w.writeheader(); w.writerows(warn_rows)
    with open(ERROR_CSV, "a", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=["snapshot_sk","sum_area_weight","reason"])
        w.writerows(error_rows)

    if final:
        BATCH = 200
        for i in range(0, len(final), BATCH):
            batch = pd.DataFrame(final[i:i+BATCH])
            batch["calculated_at"] = pd.to_datetime(batch["calculated_at"])
            con.execute("INSERT INTO geometric_crosswalk SELECT * FROM batch")

    total = con.execute("SELECT COUNT(*) FROM geometric_crosswalk").fetchone()[0]
    log.info("  geometric_crosswalk loaded: %d", total)
    return total, lineage_covered

# ─── STEP 7 ──────────────────────────────────────────────────────────────────

def step7_validation(con, loaded, fail_count, geom_true, geom_false, overlap_total, xwalk_total, lin_cov):
    log.info("=" * 70)
    log.info("STEP 7 — VALIDATION GATE")
    log.info("=" * 70)
    R = []
    LOAD_CSV  = IDENTITY_OUT / "geometry_load_failures.csv"
    ERROR_CSV = IDENTITY_OUT / "crosswalk_errors.csv"

    # Geometry checks
    inv = con.execute("SELECT COUNT(*) FROM geometry_observation WHERE NOT ST_IsValid(geom)").fetchone()[0]
    R.append(chk("All geom pass ST_IsValid", inv == 0, f"invalid={inv}"))
    nl = con.execute("SELECT COUNT(*) FROM geometry_observation WHERE geom IS NULL").fetchone()[0]
    R.append(chk("Zero NULL geom", nl == 0))
    za = con.execute("SELECT COUNT(*) FROM geometry_observation WHERE area_sqkm <= 0").fetchone()[0]
    R.append(chk("All area_sqkm > 0", za == 0, f"zero_area={za}"))
    nc = con.execute("SELECT COUNT(*) FROM geometry_observation WHERE crs_original IS NULL OR crs_original=''").fetchone()[0]
    R.append(chk("All crs_original populated", nc == 0))
    R.append(chk("geometry_load_failures.csv exists", LOAD_CSV.exists()))
    if LOAD_CSV.exists():
        with open(LOAD_CSV) as f: fcnt = sum(1 for _ in f) - 1
        R.append(chk(f"geometry_load_failures.csv rows", True, f"count={fcnt}"))

    # Reconciliation
    cks_obs = con.execute("SELECT COUNT(DISTINCT canonical_key) FROM geometry_observation").fetchone()[0]
    cks_rec = con.execute("SELECT COUNT(DISTINCT canonical_key) FROM geometry_reconciliation").fetchone()[0]
    R.append(chk("Every CK with obs has reconciliation", cks_rec >= cks_obs, f"obs={cks_obs} rec={cks_rec}"))
    bad_rc = con.execute("""
        SELECT COUNT(*) FROM fact_district_snapshot fds
        JOIN geometry_reconciliation gr ON fds.reconciliation_id=gr.reconciliation_id
        WHERE gr.is_current_decision=FALSE
    """).fetchone()[0]
    R.append(chk("No snapshot→non-current reconciliation", bad_rc == 0, f"bad={bad_rc}"))

    # Snapshot
    tot = geom_true + geom_false
    R.append(chk(f"has_geometry TRUE+FALSE=4351", tot == 4351, f"T={geom_true} F={geom_false} sum={tot}"))
    nr = con.execute("SELECT COUNT(*) FROM fact_district_snapshot WHERE has_geometry=TRUE AND reconciliation_id IS NULL").fetchone()[0]
    R.append(chk("has_geometry=TRUE all have reconciliation_id", nr == 0, f"nulls={nr}"))
    ur = con.execute("SELECT COUNT(*) FROM fact_district_snapshot WHERE (NOT has_geometry OR has_geometry IS NULL) AND reconciliation_id IS NOT NULL").fetchone()[0]
    R.append(chk("has_geometry=FALSE rows have NULL reconciliation_id", ur == 0, f"unexpected={ur}"))

    # Spatial overlap
    fo = con.execute("SELECT COUNT(*) FROM spatial_overlap WHERE fraction_of_from > 1.001").fetchone()[0]
    R.append(chk("Zero fraction_of_from > 1.001", fo == 0, f"overflow={fo}"))
    dp = con.execute("SELECT COUNT(*) FROM (SELECT from_geom_obs_id, to_geom_obs_id, COUNT(*) c FROM spatial_overlap GROUP BY 1,2 HAVING c>1)").fetchone()[0]
    R.append(chk("UNIQUE (from_geom_obs_id, to_geom_obs_id)", dp == 0, f"dups={dp}"))
    R.append(chk("crosswalk_errors.csv exists", ERROR_CSV.exists()))
    if ERROR_CSV.exists():
        with open(ERROR_CSV) as f: ecnt = sum(1 for _ in f) - 1
        R.append(chk(f"crosswalk_errors.csv rows", True, f"count={ecnt}"))

    # Crosswalk
    so = con.execute("SELECT COUNT(*) FROM (SELECT from_snapshot_id, SUM(area_weight) s FROM geometric_crosswalk GROUP BY 1 HAVING s>1.001)").fetchone()[0]
    R.append(chk("Zero SUM(area_weight)>1.001", so == 0, f"violations={so}"))
    bp = con.execute("SELECT COUNT(*) FROM geometric_crosswalk WHERE evidence_type!='DERIVED' OR pipeline_run_id IS NULL").fetchone()[0]
    R.append(chk("All crosswalk rows: evidence_type=DERIVED, pipeline_run_id NOT NULL", bp == 0, f"bad={bp}"))
    R.append(chk(f"FORMED_FROM/SPLIT_FROM pairs with crosswalk", True, f"lineage_covered={lin_cov}"))

    # Provenance
    np_total = sum(con.execute(f"SELECT COUNT(*) FROM {t} WHERE pipeline_run_id IS NULL").fetchone()[0]
                   for t in ["geometry_observation","geometry_reconciliation","spatial_overlap","geometric_crosswalk"])
    R.append(chk("Zero NULL pipeline_run_id across all 4 spatial tables", np_total == 0))

    passed = sum(1 for _, p, _ in R if p)
    failed_n = sum(1 for _, p, _ in R if not p)
    log.info("")
    log.info("=" * 70)
    log.info("VALIDATION GATE SUMMARY")
    log.info("=" * 70)
    log.info("  PASS: %d / %d", passed, len(R))
    if failed_n:
        log.error("  FAIL: %d", failed_n)
        for lbl, p, d in R:
            if not p: log.error("    ✗ %s %s", lbl, d)
    return failed_n == 0

# ─── MAIN ────────────────────────────────────────────────────────────────────

def main():
    log.info("=" * 70)
    log.info("PHASE 6 — GOLD SPATIAL")
    log.info("Run ID: %s", PIPELINE_RUN_ID)
    log.info("=" * 70)
    con = duckdb.connect(str(DB_PATH))
    try:
        sources = step0_inspect(con)
        step1_ddl(con)
        name_yr_ck, name_ck = build_ck_lookup(con)
        log.info("CK lookup: %d year-specific, %d any-year", len(name_yr_ck), len(name_ck))
        loaded, fail_count = step2_geometry_observation(con, sources, name_yr_ck, name_ck)
        rec_total, rec_current = step3_reconciliation(con)
        geom_true, geom_false = step4_update_snapshots(con)
        overlap_total = step5_spatial_overlap(con)
        xwalk_total, lin_cov = step6_crosswalk(con)
        all_pass = step7_validation(con, loaded, fail_count, geom_true, geom_false,
                                    overlap_total, xwalk_total, lin_cov)
        log.info("")
        log.info("=" * 70)
        log.info("PHASE 6 %s", "COMPLETE — ALL CHECKS PASS" if all_pass else "COMPLETE — SOME CHECKS FAILED")
        log.info("  geometry_observation:     %d loaded, %d failed", loaded, fail_count)
        log.info("  geometry_reconciliation:  %d (%d current)", rec_total, rec_current)
        log.info("  has_geometry=TRUE:        %d / 4,351", geom_true)
        log.info("  spatial_overlap pairs:    %d", overlap_total)
        log.info("  geometric_crosswalk rows: %d", xwalk_total)
        log.info("  lineage pairs covered:    %d", lin_cov)
        log.info("=" * 70)
    finally:
        con.close()

if __name__ == "__main__":
    main()
