import duckdb
import pandas as pd
from pathlib import Path
import hashlib

PROJECT_ROOT = Path('/Users/satyamkumar/Desktop/DistrictEvolution_Final')
DB_PATH = PROJECT_ROOT / "data" / "gold" / "district_evolution.duckdb"
PROD_DIR = PROJECT_ROOT / "data" / "products"
EXCEL_DIR = PROD_DIR / "excel"
OUT_DIR = PROJECT_ROOT / "outputs" / "phase9"

def build_area_views(con):
    print("Building District-Centric Area Analytics Views...")

    # 1. vw_district_area
    con.execute("""
    CREATE OR REPLACE VIEW vw_district_area AS
    SELECT 
        fds.snapshot_sk as snapshot_id,
        fds.canonical_key,
        t.year_sk as year,
        fds.primary_name as district_name,
        ckr.state_at_creation as state,
        go.area_sqkm as geometry_area_km2,
        NULL::DOUBLE as source_area_value,
        NULL::VARCHAR as source_area_unit,
        NULL::VARCHAR as source_area_field,
        'GEOMETRY_DERIVED' as area_source,
        go.was_repaired as geometry_repaired,
        'ST_Area_Spheroid' as area_method,
        fds.pipeline_run_id
    FROM fact_district_snapshot fds
    JOIN dim_time t ON fds.time_sk = t.year_sk
    JOIN canonical_key_registry ckr ON fds.canonical_key = ckr.canonical_key
    LEFT JOIN geometry_reconciliation gr ON fds.reconciliation_id = gr.reconciliation_id AND gr.is_current_decision=TRUE
    LEFT JOIN geometry_observation go ON gr.preferred_geom_obs_id = go.geom_obs_id;
    """)

    # 2. vw_district_area_timeseries
    con.execute("""
    CREATE OR REPLACE VIEW vw_district_area_timeseries AS
    WITH ranked AS (
        SELECT 
            canonical_key,
            year,
            district_name,
            state,
            geometry_area_km2 as area_km2,
            snapshot_id,
            LAG(geometry_area_km2) OVER (PARTITION BY canonical_key ORDER BY year) as previous_area_km2
        FROM vw_district_area
    )
    SELECT 
        *,
        (area_km2 - previous_area_km2) as area_change_km2,
        CASE WHEN previous_area_km2 > 0 THEN 100.0 * (area_km2 - previous_area_km2) / previous_area_km2 ELSE NULL END as area_change_pct,
        CASE 
            WHEN previous_area_km2 IS NULL THEN 'NEW_OBSERVATION'
            WHEN ABS(area_km2 - previous_area_km2) < 0.1 THEN 'AREA_STABLE'
            WHEN area_km2 > previous_area_km2 THEN 'AREA_INCREASE'
            ELSE 'AREA_DECREASE'
        END as area_change_status
    FROM ranked;
    """)

    # Intermediate View for raw geometric intersection between any pairs (to reuse earlier logic)
    con.execute("""
    CREATE OR REPLACE VIEW vw_raw_transfer AS
    SELECT 
        sc.stat_xwalk_id as transfer_id,
        sc.from_snapshot_id,
        sc.to_snapshot_id,
        COALESCE(gc.intersection_sqkm, sc.pre_normalization_weight * f.area_km2) as intersection_area_km2,
        sc.pre_normalization_weight as raw_transfer_weight,
        sc.statistical_weight,
        sc.coverage_score,
        CASE 
            WHEN COUNT(sc.pre_normalization_weight) OVER (PARTITION BY sc.from_snapshot_id) = 0 THEN NULL
            ELSE (SUM(sc.pre_normalization_weight) OVER (PARTITION BY sc.from_snapshot_id)) - MAX(sc.coverage_score) OVER (PARTITION BY sc.from_snapshot_id)
        END as overlap_excess,
        sc.was_normalized,
        CASE WHEN sc.was_normalized THEN 'PROPORTIONAL_APPORTIONMENT' ELSE 'NONE' END as normalization_method,
        sc.distribution_assumption,
        sc.weighting_method as area_measurement_method,
        CASE 
            WHEN sc.weighting_method = 'UNMEASURED' THEN 'UNMEASURED'
            ELSE 'MEASURED'
        END as measurement_status,
        CASE 
            WHEN sc.weighting_method = 'UNMEASURED' THEN 'UNMEASURED'
            WHEN sc.coverage_score < 0.85 THEN 'LOW_COVERAGE'
            WHEN sc.was_normalized THEN 'MEASURED_NORMALIZED'
            ELSE 'MEASURED'
        END as crosswalk_status,
        sc.evidence_type,
        sc.uncertainty_estimate,
        sc.pipeline_run_id
    FROM statistical_crosswalk sc
    JOIN vw_district_area_timeseries f ON sc.from_snapshot_id = f.snapshot_id
    JOIN vw_district_area_timeseries t ON sc.to_snapshot_id = t.snapshot_id
    LEFT JOIN geometric_crosswalk gc ON sc.geo_xwalk_id = gc.geo_xwalk_id
    QUALIFY ROW_NUMBER() OVER (PARTITION BY sc.from_snapshot_id, sc.to_snapshot_id ORDER BY 
        CASE WHEN sc.weighting_method = 'UNMEASURED' THEN 2 ELSE 1 END) = 1;
    """)

    # PRODUCT 1: event_register (Exactly 1550 rows)
    con.execute("""
    CREATE OR REPLACE VIEW vw_event_register AS
    SELECT 
        e.event_id,
        MAX(e.source_pk) as source_event_id,
        MAX(e.event_type) as event_type,
        EXTRACT(YEAR FROM MAX(e.event_date_est)) as effective_year,
        MAX(e.event_date_est) as event_date_est,
        MAX(e.event_date_precision) as event_date_precision,
        MAX(CASE WHEN p.role='SUCCESSOR' THEN c.display_name ELSE NULL END) as district_id,
        MAX(CASE WHEN p.role='SUCCESSOR' THEN c.display_name ELSE NULL END) as district_name,
        MAX(c.state_at_creation) as state,
        STRING_AGG(CASE WHEN p.role='PREDECESSOR' THEN c.display_name ELSE NULL END, ', ') as parent_district,
        STRING_AGG(CASE WHEN p.role='SUCCESSOR' THEN c.display_name ELSE NULL END, ', ') as child_district,
        MAX(ev.source_pk) as source,
        MAX(e.lineage_confidence) as confidence_score,
        'CONFIRMED' as event_status,
        MAX(e.pipeline_run_id) as pipeline_run_id
    FROM boundary_event e
    LEFT JOIN event_participant p ON e.event_id = p.event_id
    LEFT JOIN canonical_key_registry c ON p.canonical_key = c.canonical_key
    LEFT JOIN event_evidence ev ON e.event_id = ev.event_id
    GROUP BY e.event_id;
    """)

    # BASE CTEs for Products 2 and 3
    con.execute("""
    CREATE OR REPLACE VIEW vw_event_rels_base AS
    WITH event_rels AS (
        SELECT 
            e.event_id, e.event_type, e.source_pk, EXTRACT(YEAR FROM e.event_date_est) as effective_year, e.event_date_est,
            dr.from_ck, dr.to_ck, dr.relationship_type, dr.lineage_confidence,
            e.pipeline_run_id
        FROM boundary_event e
        LEFT JOIN district_relationship dr ON e.event_id = dr.supporting_event_id
    ),
    source_years AS (
        SELECT er.event_id, er.from_ck, MAX(p.year) as year
        FROM event_rels er
        LEFT JOIN vw_district_area_timeseries p ON er.from_ck = p.canonical_key AND p.year <= er.effective_year
        GROUP BY er.event_id, er.from_ck
    ),
    target_years AS (
        SELECT er.event_id, er.to_ck, MIN(c.year) as year
        FROM event_rels er
        LEFT JOIN vw_district_area_timeseries c ON er.to_ck = c.canonical_key AND c.year >= er.effective_year
        GROUP BY er.event_id, er.to_ck
    ),
    parent_after AS (
        SELECT 
            e.event_id, dr.from_ck, s.area_km2 as area_after_km2, s.snapshot_id as after_snapshot_id
        FROM boundary_event e
        JOIN district_relationship dr ON e.event_id = dr.supporting_event_id
        JOIN vw_district_area_timeseries s ON dr.from_ck = s.canonical_key
        WHERE s.year = (
            SELECT MIN(year) FROM vw_district_area_timeseries s2 
            WHERE s2.canonical_key = s.canonical_key AND s2.year >= EXTRACT(YEAR FROM e.event_date_est)
        )
        GROUP BY e.event_id, dr.from_ck, s.area_km2, s.snapshot_id
    ),
    target_before AS (
        SELECT 
            e.event_id, dr.to_ck, s.area_km2 as area_before_km2
        FROM boundary_event e
        JOIN district_relationship dr ON e.event_id = dr.supporting_event_id
        JOIN vw_district_area_timeseries s ON dr.to_ck = s.canonical_key
        WHERE s.year = (
            SELECT MAX(year) FROM vw_district_area_timeseries s2 
            WHERE s2.canonical_key = s.canonical_key AND s2.year <= EXTRACT(YEAR FROM e.event_date_est)
        )
        GROUP BY e.event_id, dr.to_ck, s.area_km2
    )
    SELECT 
        er.event_id, er.event_type, er.effective_year,
        er.from_ck, p.district_name as from_district, p.state as from_state, sy.year as from_year,
        er.to_ck, c.district_name as to_district, c.state as to_state, ty.year as to_year,
        er.relationship_type,
        NULL::VARCHAR as evidence_type,
        er.lineage_confidence as confidence,
        p.snapshot_id as source_snapshot_id, c.snapshot_id as target_snapshot_id,
        p.area_km2 as from_area_km2, c.area_km2 as to_area_km2,
        tb.area_before_km2 as target_area_before_km2,
        pa.area_after_km2 as source_area_after_km2,
        er.pipeline_run_id
    FROM event_rels er
    LEFT JOIN source_years sy ON er.event_id = sy.event_id AND er.from_ck = sy.from_ck
    LEFT JOIN vw_district_area_timeseries p ON sy.from_ck = p.canonical_key AND sy.year = p.year
    LEFT JOIN target_years ty ON er.event_id = ty.event_id AND er.to_ck = ty.to_ck
    LEFT JOIN vw_district_area_timeseries c ON ty.to_ck = c.canonical_key AND ty.year = c.year
    LEFT JOIN parent_after pa ON er.event_id = pa.event_id AND er.from_ck = pa.from_ck
    LEFT JOIN target_before tb ON er.event_id = tb.event_id AND er.to_ck = tb.to_ck
    QUALIFY ROW_NUMBER() OVER(PARTITION BY er.event_id, er.from_ck, er.to_ck ORDER BY COALESCE(p.snapshot_id, 0) DESC, COALESCE(c.snapshot_id, 0) DESC) = 1;
    """)

    # PRODUCT 2: event_relationship
    con.execute("""
    CREATE OR REPLACE VIEW vw_event_relationship AS
    SELECT 
        MD5(event_id || COALESCE(from_ck, '') || COALESCE(to_ck, '')) as relationship_id,
        event_id,
        from_ck,
        from_district,
        from_state,
        from_year,
        to_ck,
        to_district,
        to_state,
        to_year,
        relationship_type,
        evidence_type,
        confidence,
        'ACTIVE' as relationship_status
    FROM vw_event_rels_base;
    """)

    # PRODUCT 3: event_area_accounting
    con.execute("""
    CREATE OR REPLACE VIEW vw_event_area_accounting AS
    SELECT 
        MD5(b.event_id || COALESCE(b.from_ck, '') || COALESCE(b.to_ck, '')) as relationship_id,
        b.event_id,
        b.from_ck,
        b.to_ck,
        b.from_year,
        b.to_year,
        b.effective_year,
        
        b.from_area_km2,
        b.to_area_km2,
        
        CASE WHEN b.event_type IN ('RENAME', 'NON_SPATIAL') OR xfer.measurement_status IN ('UNMEASURED', 'NON_SPATIAL') THEN NULL ELSE xfer.intersection_area_km2 END as area_transferred_km2,
        CASE WHEN b.event_type IN ('RENAME', 'NON_SPATIAL') OR xfer.measurement_status IN ('UNMEASURED', 'NON_SPATIAL') THEN NULL ELSE xfer.intersection_area_km2 END as area_received_km2,
        CASE WHEN b.event_type IN ('RENAME', 'NON_SPATIAL') OR xfer.measurement_status IN ('UNMEASURED', 'NON_SPATIAL') THEN NULL ELSE xfer.intersection_area_km2 END as area_relinquished_km2,
        
        CASE WHEN b.event_type IN ('RENAME', 'NON_SPATIAL') OR xfer.measurement_status IN ('UNMEASURED', 'NON_SPATIAL') THEN NULL 
             WHEN b.source_area_after_km2 IS NOT NULL THEN b.source_area_after_km2 ELSE 0 END as area_retained_km2,
             
        CASE WHEN b.event_type IN ('RENAME', 'NON_SPATIAL') OR xfer.measurement_status IN ('UNMEASURED', 'NON_SPATIAL') THEN NULL 
             ELSE (b.from_area_km2 - (CASE WHEN b.source_area_after_km2 IS NOT NULL THEN b.source_area_after_km2 ELSE 0 END + SUM(COALESCE(xfer.intersection_area_km2, 0)) OVER (PARTITION BY b.event_id, b.from_ck))) END as unaccounted_area_km2,
        
        CASE WHEN b.event_type IN ('RENAME', 'NON_SPATIAL') OR xfer.measurement_status IN ('UNMEASURED', 'NON_SPATIAL') THEN NULL ELSE xfer.intersection_area_km2 END as intersection_area_km2,
        CASE WHEN b.event_type IN ('RENAME', 'NON_SPATIAL') OR xfer.measurement_status IN ('UNMEASURED', 'NON_SPATIAL') THEN NULL ELSE (CASE WHEN xfer.overlap_excess > 0 THEN xfer.overlap_excess * b.from_area_km2 ELSE 0 END) END as overlap_area_km2,
        
        CASE WHEN b.event_type IN ('RENAME', 'NON_SPATIAL') OR xfer.measurement_status IN ('UNMEASURED', 'NON_SPATIAL') THEN NULL ELSE xfer.raw_transfer_weight END as raw_area_weight,
        CASE WHEN b.event_type IN ('RENAME', 'NON_SPATIAL') OR xfer.measurement_status IN ('UNMEASURED', 'NON_SPATIAL') THEN NULL ELSE xfer.statistical_weight END as statistical_weight,
        
        CASE WHEN b.event_type IN ('RENAME', 'NON_SPATIAL') THEN NULL ELSE xfer.coverage_score END as coverage_score,
        CASE WHEN b.event_type IN ('RENAME', 'NON_SPATIAL') THEN NULL ELSE xfer.overlap_excess END as overlap_excess,
        
        COALESCE(CASE WHEN b.event_type = 'RENAME' THEN 'NON_SPATIAL' ELSE xfer.measurement_status END, 'UNMEASURED') as measurement_status,
        COALESCE(CASE WHEN b.event_type = 'RENAME' THEN 'NON_SPATIAL' ELSE xfer.crosswalk_status END, 'UNMEASURED') as crosswalk_status,
        
        CASE WHEN b.event_type = 'RENAME' THEN NULL ELSE xfer.area_measurement_method END as area_measurement_method,
        CASE WHEN b.event_type = 'RENAME' THEN NULL ELSE xfer.normalization_method END as normalization_method,
        CASE WHEN b.event_type = 'RENAME' THEN NULL ELSE xfer.distribution_assumption END as distribution_assumption,
        
        b.evidence_type,
        b.confidence
        
    FROM vw_event_rels_base b
    LEFT JOIN vw_raw_transfer xfer ON xfer.from_snapshot_id = b.source_snapshot_id AND xfer.to_snapshot_id = b.target_snapshot_id;
    """)

    # PRODUCT 4: event_area_summary (Exactly 1550 rows)
    con.execute("""
    CREATE OR REPLACE VIEW vw_event_area_summary AS
    SELECT 
        r.event_id,
        r.event_type,
        r.effective_year,
        r.district_name,
        r.state,
        COUNT(a.to_ck) as relationship_count,
        MIN(a.measurement_status) as measurement_status,
        MIN(a.crosswalk_status) as crosswalk_status,
        
        MAX(a.from_area_km2) as area_before_km2,
        SUM(a.area_transferred_km2) as area_transferred_km2,
        SUM(a.area_received_km2) as area_received_km2,
        SUM(a.area_relinquished_km2) as area_relinquished_km2,
        MAX(a.area_retained_km2) as area_retained_km2,
        MAX(a.unaccounted_area_km2) as unaccounted_area_km2,
        
        MAX(a.coverage_score) as coverage_score,
        MAX(a.overlap_excess) as overlap_excess,
        
        MAX(r.confidence_score) as confidence,
        
        CASE 
            WHEN COUNT(a.to_ck) = 0 THEN 'NO_RELATIONSHIP'
            WHEN MIN(a.measurement_status) = 'NON_SPATIAL' THEN 'NON_SPATIAL_EVENT'
            WHEN MAX(a.from_area_km2) IS NULL THEN 'NO_SOURCE_GEOMETRY'
            WHEN MIN(a.measurement_status) = 'UNMEASURED' THEN 'UNKNOWN_UNMEASURED'
            ELSE NULL
        END as reason_unmeasured,
        
        r.pipeline_run_id
    FROM vw_event_register r
    LEFT JOIN vw_event_area_accounting a ON r.event_id = a.event_id
    GROUP BY r.event_id, r.event_type, r.effective_year, r.district_name, r.state, r.pipeline_run_id;
    """)

def export_area_products(con, iteration=1):
    print(f"Exporting District/Event Centric Area Products (Iteration {iteration})...")
    
    views = {
        "district_area": "ORDER BY year, district_name",
        "district_area_timeseries": "ORDER BY canonical_key, year",
        "event_register": "ORDER BY event_id",
        "event_relationship": "ORDER BY event_id, to_ck",
        "event_area_accounting": "ORDER BY event_id, to_ck",
        "event_area_summary": "ORDER BY effective_year, event_id"
    }
    
    hashes = {}
    for v, order_clause in views.items():
        view_name = f"vw_{v}"
        query = f"SELECT * FROM {view_name} {order_clause}"
        
        parquet_path = PROD_DIR / f"{v}.parquet"
        csv_path = PROD_DIR / f"{v}.csv"
        
        if iteration == 1:
            con.execute(f"COPY ({query}) TO '{parquet_path}' (FORMAT PARQUET)")
            con.execute(f"COPY ({query}) TO '{csv_path}' (FORMAT CSV, HEADER)")
            
            # Excel export if < 100k rows
            df = con.execute(query).fetchdf()
            if len(df) < 100000:
                df.to_excel(EXCEL_DIR / f"{v}.xlsx", index=False)
        else:
            tmp_path = PROD_DIR / f"{v}_temp.parquet"
            con.execute(f"COPY ({query}) TO '{tmp_path}' (FORMAT PARQUET)")
            with open(tmp_path, "rb") as f:
                hashes[v] = hashlib.md5(f.read()).hexdigest()
            tmp_path.unlink()
                
    if iteration == 1:
        for v in views.keys():
            with open(PROD_DIR / f"{v}.parquet", "rb") as f:
                hashes[v] = hashlib.md5(f.read()).hexdigest()
                
    return hashes

def main():
    con = duckdb.connect(str(DB_PATH))
    con.execute("INSTALL spatial; LOAD spatial;")
    
    build_area_views(con)
    hashes1 = export_area_products(con, iteration=1)
    hashes2 = export_area_products(con, iteration=2)
    
    rep_report = ["# Phase 9 Reproducibility Report\n\nDataset | Hash 1 | Hash 2 | Status\n---|---|---|---"]
    for k in hashes1.keys():
        h1 = hashes1[k]
        h2 = hashes2[k]
        status = "PASS" if h1 == h2 else "FAIL"
        rep_report.append(f"{k} | {h1} | {h2} | {status}")
        
    with open(OUT_DIR / "reproducibility_report.md", "w") as f:
        f.write("\n".join(rep_report))
            
    con.close()
    print("Phase 9 Area Products Generation Complete.")

if __name__ == "__main__":
    main()
