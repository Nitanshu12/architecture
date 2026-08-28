import duckdb
import pandas as pd
import hashlib
from pathlib import Path
import json

PROJECT_ROOT = Path('/Users/satyamkumar/Desktop/DistrictEvolution_Final')
DB_PATH = PROJECT_ROOT / "data" / "gold" / "district_evolution.duckdb"
PROD_DIR = PROJECT_ROOT / "data" / "products"
EXCEL_DIR = PROD_DIR / "excel"
OUT_DIR = PROJECT_ROOT / "outputs" / "phase8"

def build_views(con):
    print("Building analytical views...")
    # vw_district_identity
    con.execute("""
    CREATE OR REPLACE VIEW vw_district_identity AS
    SELECT 
        ckr.canonical_key,
        ckr.display_name,
        ckr.state_at_creation as state,
        CASE WHEN ckr.is_active THEN 'ACTIVE' ELSE 'CLOSED' END as identity_status,
        EXTRACT(YEAR FROM ckr.established_date) as first_observed_year,
        EXTRACT(YEAR FROM ckr.closed_date) as last_observed_year,
        'HIGH' as identity_confidence,
        (SELECT COUNT(*) FROM source_pk_to_ck_mapping m WHERE m.canonical_key = ckr.canonical_key) as source_count
    FROM canonical_key_registry ckr;
    """)

    # vw_district_snapshot
    con.execute("""
    CREATE OR REPLACE VIEW vw_district_snapshot AS
    SELECT 
        fds.snapshot_sk as snapshot_id,
        fds.canonical_key,
        fds.time_sk,
        t.year_sk as year,
        fds.primary_name as district_name,
        ckr.state_at_creation as state,
        go.geom as geometry,
        ST_Area(go.geom) as area,
        fds.source_dataset,
        fds.source_pk,
        'HIGH' as identity_confidence,
        fds.pipeline_run_id
    FROM fact_district_snapshot fds
    JOIN dim_time t ON fds.time_sk = t.year_sk
    LEFT JOIN geometry_reconciliation gr ON fds.reconciliation_id = gr.reconciliation_id AND gr.is_current_decision=TRUE
    LEFT JOIN geometry_observation go ON gr.preferred_geom_obs_id = go.geom_obs_id
    JOIN canonical_key_registry ckr ON fds.canonical_key = ckr.canonical_key;
    """)

    # vw_district_lineage
    con.execute("""
    CREATE OR REPLACE VIEW vw_district_lineage AS
    SELECT 
        dr.rel_id as relationship_id,
        dr.from_ck as parent_ck,
        dr.to_ck as child_ck,
        dr.relationship_type,
        EXTRACT(YEAR FROM e.event_date_est) as effective_year,
        dr.supporting_event_id as event_id,
        dr.evidence_type,
        dr.lineage_confidence as confidence,
        dr.lineage_basis as measurement_status,
        dr.pipeline_run_id
    FROM district_relationship dr
    LEFT JOIN boundary_event e ON dr.supporting_event_id = e.event_id;
    """)

    # vw_boundary_events
    con.execute("""
    CREATE OR REPLACE VIEW vw_boundary_events AS
    SELECT 
        de.event_id,
        de.event_type,
        EXTRACT(YEAR FROM de.event_date_est) as effective_year,
        de.event_date_est,
        de.event_date_precision,
        de.evidence_type as evidence_status,
        de.lineage_confidence as confidence,
        de.source_pk as source,
        dr.from_ck as parent_ck,
        dr.to_ck as child_ck
    FROM boundary_event de
    LEFT JOIN district_relationship dr ON de.event_id = dr.supporting_event_id;
    """)

    # vw_statistical_crosswalk
    con.execute("""
    CREATE OR REPLACE VIEW vw_statistical_crosswalk AS
    SELECT 
        sc.from_snapshot_id,
        f.canonical_key as from_ck,
        f.year as year_from,
        sc.to_snapshot_id,
        t.canonical_key as to_ck,
        t.year as year_to,
        sc.pre_normalization_weight as raw_area_weight,
        sc.statistical_weight,
        sc.coverage_score,
        ((SUM(COALESCE(sc.pre_normalization_weight, 0)) OVER (PARTITION BY sc.from_snapshot_id)) - sc.coverage_score) as overlap_excess,
        sc.was_normalized,
        CASE 
            WHEN sc.was_normalized THEN 'PROPORTIONAL_APPORTIONMENT' 
            ELSE 'NONE' 
        END as normalization_method,
        sc.distribution_assumption,
        sc.weighting_method as measurement_status,
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
    JOIN vw_district_snapshot f ON sc.from_snapshot_id = f.snapshot_id
    JOIN vw_district_snapshot t ON sc.to_snapshot_id = t.snapshot_id;
    """)

    # vw_usable_crosswalk
    con.execute("""
    CREATE OR REPLACE VIEW vw_usable_crosswalk AS
    SELECT * FROM vw_statistical_crosswalk
    WHERE crosswalk_status IN ('MEASURED', 'MEASURED_NORMALIZED');
    """)

    # vw_crosswalk_quality
    con.execute("""
    CREATE OR REPLACE VIEW vw_crosswalk_quality AS
    SELECT 
        from_snapshot_id as source_snapshot_id,
        from_ck as canonical_key,
        year_from as year,
        COUNT(to_snapshot_id) as target_count,
        SUM(raw_area_weight) as raw_weight_sum,
        SUM(statistical_weight) as normalized_weight_sum,
        MAX(coverage_score) as coverage_score,
        MAX(overlap_excess) as overlap_excess,
        (1.0 - MAX(coverage_score)) as uncovered_fraction,
        MAX(CAST(was_normalized AS INTEGER)) as was_normalized,
        MAX(measurement_status) as measurement_status,
        MAX(crosswalk_status) as crosswalk_status,
        CASE 
            WHEN MAX(overlap_excess) > 0.1 THEN 'HIGH_OVERLAP'
            WHEN MAX(coverage_score) < 0.85 THEN 'LOW_COVERAGE'
            ELSE 'STANDARD'
        END as quality_flags,
        CASE 
            WHEN MAX(overlap_excess) > 0.1 THEN 'YELLOW'
            WHEN MAX(coverage_score) < 0.85 THEN 'ORANGE'
            WHEN MAX(measurement_status) = 'UNMEASURED' THEN 'RED'
            ELSE 'GREEN'
        END as scientific_risk
    FROM vw_statistical_crosswalk
    GROUP BY from_snapshot_id, from_ck, year_from;
    """)

    # vw_validation_summary
    con.execute("""
    CREATE OR REPLACE VIEW vw_validation_summary AS
    SELECT 
        r.run_id as validation_run_id,
        res.rule_id,
        ru.rule_code as rule_name,
        ru.severity,
        r.entities_checked as total_checked,
        r.entities_checked - (r.errors_found + r.warnings_found) as passed,
        r.errors_found + r.warnings_found as failed,
        r.warnings_found as warning_count,
        r.errors_found as error_count,
        COUNT(CASE WHEN res.is_resolved THEN 1 END) as resolution_status,
        r.run_timestamp as generated_at
    FROM validation_run r
    JOIN validation_result res ON r.run_id = res.run_id
    JOIN validation_rule ru ON res.rule_id = ru.rule_id
    GROUP BY r.run_id, res.rule_id, ru.rule_code, ru.severity, r.entities_checked, r.errors_found, r.warnings_found, r.run_timestamp;
    """)

def export_products(con, iteration=1):
    print(f"Exporting products (Iteration {iteration})...")
    # For geometry, we output as GeoParquet for spatial capabilities, but for standard parquets we export without ST_AsWKB
    # For district_snapshot we'll just cast geom to WKT for CSV and WKB for parquet
    
    views = [
        "district_identity", "district_snapshot", "district_lineage", 
        "boundary_events", "statistical_crosswalk", "usable_crosswalk",
        "crosswalk_quality", "validation_summary"
    ]
    
    hashes = {}
    for v in views:
        view_name = f"vw_{v}"
        
        if v == "district_snapshot":
            # For geometries
            query = f"SELECT * EXCLUDE(geometry), ST_AsWKB(geometry) as geometry FROM {view_name} ORDER BY snapshot_id"
        elif v == "statistical_crosswalk" or v == "usable_crosswalk":
            query = f"SELECT * FROM {view_name} ORDER BY from_snapshot_id, to_snapshot_id"
        elif v == "boundary_events":
            query = f"SELECT * FROM {view_name} ORDER BY event_id, child_ck"
        else:
            # Get primary column to order by
            first_col = con.execute(f"DESCRIBE {view_name}").fetchone()[0]
            query = f"SELECT * FROM {view_name} ORDER BY {first_col}"
            
        parquet_path = PROD_DIR / f"{v}.parquet"
        csv_path = PROD_DIR / f"{v}.csv"
        
        # We only output CSV/Excel on iteration 1
        if iteration == 1:
            con.execute(f"COPY ({query}) TO '{parquet_path}' (FORMAT PARQUET)")
            
            # CSV export
            if v == "district_snapshot":
                csv_query = f"SELECT * EXCLUDE(geometry), ST_AsText(geometry) as geometry FROM {view_name} ORDER BY snapshot_id"
            else:
                csv_query = query
            con.execute(f"COPY ({csv_query}) TO '{csv_path}' (FORMAT CSV, HEADER)")
            
            # Also write to excel if small enough
            df = con.execute(csv_query).fetchdf()
            if len(df) < 100000 and v not in ['statistical_crosswalk', 'district_snapshot']: # Excel limit
                df.to_excel(EXCEL_DIR / f"{v}.xlsx", index=False)
                
        else:
            # Iteration 2: write to temporary path for hash comparison
            tmp_path = PROD_DIR / f"{v}_temp.parquet"
            con.execute(f"COPY ({query}) TO '{tmp_path}' (FORMAT PARQUET)")
            with open(tmp_path, "rb") as f:
                h = hashlib.md5(f.read()).hexdigest()
            hashes[v] = h
            tmp_path.unlink()
            
    if iteration == 1:
        # Generate hashes for iteration 1
        for v in views:
            parquet_path = PROD_DIR / f"{v}.parquet"
            with open(parquet_path, "rb") as f:
                hashes[v] = hashlib.md5(f.read()).hexdigest()
                
    return hashes

def generate_validation_reports(con):
    print("Generating validation reports...")
    report = ["# Product Validation Report\n"]
    
    row_counts = []
    
    views = [
        ("vw_district_identity", "canonical_key_registry"),
        ("vw_district_snapshot", "fact_district_snapshot"),
        ("vw_district_lineage", "district_relationship"),
        ("vw_boundary_events", "boundary_event"),
        ("vw_statistical_crosswalk", "statistical_crosswalk")
    ]
    
    for view, base in views:
        v_count = con.execute(f"SELECT COUNT(*) FROM {view}").fetchone()[0]
        b_count = con.execute(f"SELECT COUNT(*) FROM {base}").fetchone()[0]
        diff = b_count - v_count
        
        row_counts.append({
            "Dataset": view.replace('vw_', ''),
            "ProductRows": v_count,
            "SourceRows": b_count,
            "Difference": diff,
            "Status": "PASS" if diff == 0 else "FAIL"
        })
        
    usable_count = con.execute("SELECT COUNT(*) FROM vw_usable_crosswalk").fetchone()[0]
    total_cross = con.execute("SELECT COUNT(*) FROM vw_statistical_crosswalk").fetchone()[0]
    excluded = total_cross - usable_count
    
    row_counts.append({
        "Dataset": "usable_crosswalk",
        "ProductRows": usable_count,
        "SourceRows": total_cross,
        "Difference": excluded,
        "Status": "FILTERED"
    })
    
    df_rc = pd.DataFrame(row_counts)
    df_rc.to_csv(OUT_DIR / "row_counts.csv", index=False)
    
    report.append("## Reconciliation\n")
    report.append(df_rc.to_markdown(index=False) + "\n\n")
    
    # Validation queries
    report.append("## Referential Integrity Checks\n")
    
    ck_miss = con.execute("SELECT COUNT(*) FROM vw_district_snapshot WHERE canonical_key NOT IN (SELECT canonical_key FROM vw_district_identity)").fetchone()[0]
    report.append(f"- Snapshots missing canonical_key in identity table: {ck_miss}\n")
    
    parent_miss = con.execute("SELECT COUNT(*) FROM vw_district_lineage WHERE parent_ck NOT IN (SELECT canonical_key FROM vw_district_identity)").fetchone()[0]
    report.append(f"- Lineage parent_ck missing in identity table: {parent_miss}\n")

    unmeasured_cnt = con.execute("SELECT COUNT(*) FROM vw_statistical_crosswalk WHERE crosswalk_status = 'UNMEASURED'").fetchone()[0]
    low_cov_cnt = con.execute("SELECT COUNT(*) FROM vw_statistical_crosswalk WHERE crosswalk_status = 'LOW_COVERAGE'").fetchone()[0]
    norm_cnt = con.execute("SELECT COUNT(*) FROM vw_statistical_crosswalk WHERE crosswalk_status = 'MEASURED_NORMALIZED'").fetchone()[0]

    report.append("\n## Policy Exclusions\n")
    report.append(f"- Total UNMEASURED records preserved: {unmeasured_cnt}\n")
    report.append(f"- Total LOW_COVERAGE records preserved: {low_cov_cnt}\n")
    report.append(f"- Total MEASURED_NORMALIZED records preserved: {norm_cnt}\n")
    report.append(f"- Total records excluded in usable dataset: {excluded}\n")

    with open(OUT_DIR / "product_validation_report.md", "w") as f:
        f.write("\n".join(report))

def main():
    con = duckdb.connect(str(DB_PATH))
    con.execute("INSTALL spatial; LOAD spatial;")
    
    build_views(con)
    hashes1 = export_products(con, iteration=1)
    hashes2 = export_products(con, iteration=2)
    
    # Compare reproducibility
    rep_report = ["# Reproducibility Report\n\nDataset | Hash 1 | Hash 2 | Status\n---|---|---|---"]
    for k in hashes1.keys():
        h1 = hashes1[k]
        h2 = hashes2[k]
        status = "PASS" if h1 == h2 else "FAIL"
        rep_report.append(f"{k} | {h1} | {h2} | {status}")
        
    with open(OUT_DIR / "reproducibility_report.md", "w") as f:
        f.write("\n".join(rep_report))
        
    generate_validation_reports(con)
    
    con.close()
    print("Phase 8 generation complete.")

if __name__ == "__main__":
    main()
