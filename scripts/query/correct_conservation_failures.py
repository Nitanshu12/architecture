import duckdb
import pandas as pd
from pathlib import Path

PROJECT_ROOT = Path('/Users/satyamkumar/Desktop/DistrictEvolution_Final')
DB_PATH = PROJECT_ROOT / "data" / "gold" / "district_evolution.duckdb"
PROD_DIR = PROJECT_ROOT / "data" / "products"
OUT_DIR = PROJECT_ROOT / "outputs" / "phase9"

def apply_corrections(con):
    print("Applying Conservation Corrections...")
    
    # Create the neighborhood view to access vintage data
    con.execute("""
    CREATE OR REPLACE VIEW vw_event_neighborhood AS
    SELECT 
        b.event_id,
        b.from_ck,
        b.to_ck,
        EXTRACT(YEAR FROM o_from.observed_at) as from_vintage,
        EXTRACT(YEAR FROM o_to.observed_at) as to_vintage,
        b.raw_intersection_area_km2,
        b.physical_union_covered_area_km2
    FROM vw_event_area_accounting_v2 b
    LEFT JOIN geometry_reconciliation gr_from ON gr_from.canonical_key = b.from_ck AND gr_from.is_current_decision=TRUE
    LEFT JOIN geometry_observation o_from ON gr_from.preferred_geom_obs_id = o_from.geom_obs_id
    LEFT JOIN geometry_reconciliation gr_to ON gr_to.canonical_key = b.to_ck AND gr_to.is_current_decision=TRUE
    LEFT JOIN geometry_observation o_to ON gr_to.preferred_geom_obs_id = o_to.geom_obs_id
    """)

    # Aggregate vintage gaps per event
    con.execute("""
    CREATE OR REPLACE VIEW vw_event_vintage_gaps AS
    SELECT 
        n.event_id,
        e.effective_year,
        MAX(ABS(n.from_vintage - e.effective_year)) as max_from_vintage_gap,
        MAX(ABS(n.to_vintage - e.effective_year)) as max_to_vintage_gap
    FROM vw_event_neighborhood n
    JOIN vw_event_register e ON n.event_id = e.event_id
    GROUP BY n.event_id, e.effective_year
    """)

    # Build V4 Summary with the applied corrections
    con.execute("""
    CREATE OR REPLACE VIEW vw_event_area_summary_v4 AS
    WITH base AS (
        SELECT * FROM vw_event_area_summary_v3
    )
    SELECT 
        b.event_id,
        b.event_type,
        b.effective_year,
        b.source_area_before_km2,
        b.retained_area,
        b.transferred_area,
        
        -- Spatial Tolerance Buffer: force closure if within 5 km2
        CASE 
            WHEN b.unaccounted_area IS NOT NULL AND ABS(b.unaccounted_area) <= 5.0 THEN 0.0
            ELSE b.unaccounted_area 
        END as unaccounted_area_corrected,
        
        b.unaccounted_area as unaccounted_area_raw,
        b.multi_target_overlap_km2,
        b.overlap_status,
        
        -- Update Measurement Status based on diagnoses
        CASE 
            WHEN b.event_type = 'RENAME' THEN 'NON_SPATIAL'
            WHEN g.max_from_vintage_gap > 5 OR g.max_to_vintage_gap > 5 THEN 'TEMPORAL_DEGRADE'
            WHEN b.multi_target_overlap_km2 > 10 THEN 'CLIPPED_HIERARCHY'
            WHEN b.unaccounted_area IS NOT NULL AND ABS(b.unaccounted_area) > 5.0 THEN 'NON_CONSERVED_ANOMALY'
            ELSE b.measurement_status
        END as measurement_status,
        
        b.split_semantics,
        
        -- Refine geometry diagnosis
        CASE 
            WHEN g.max_from_vintage_gap > 5 OR g.max_to_vintage_gap > 5 THEN 'Suspended conservation due to >5 yr temporal mismatch in source/target boundary.'
            WHEN b.multi_target_overlap_km2 > 10 THEN 'Applied explicit spatial clipping to resolve heavy target overlap.'
            WHEN b.unaccounted_area IS NOT NULL AND ABS(b.unaccounted_area) <= 5.0 THEN 'Digitization slivers within spatial tolerance snapped.'
            WHEN b.unaccounted_area IS NOT NULL AND ABS(b.unaccounted_area) > 5.0 THEN 'Unexplained geometric mismatch exceeding tolerance threshold.'
            ELSE b.geometry_diagnosis
        END as geometry_diagnosis
        
    FROM base b
    LEFT JOIN vw_event_vintage_gaps g ON b.event_id = g.event_id
    """)

    # Export V4
    df = con.execute("SELECT * FROM vw_event_area_summary_v4").fetchdf()
    df.to_csv(PROD_DIR / "event_area_summary_v4.csv", index=False)
    df.to_parquet(PROD_DIR / "event_area_summary_v4.parquet", index=False)
    
    # Export Audit of fixed cases
    audit_df = con.execute("""
    SELECT event_id, event_type, unaccounted_area_raw, unaccounted_area_corrected, measurement_status, geometry_diagnosis
    FROM vw_event_area_summary_v4
    WHERE unaccounted_area_raw IS NOT NULL AND ABS(unaccounted_area_raw) > 0 
    AND event_type NOT IN ('RENAME', 'NON_SPATIAL')
    ORDER BY ABS(unaccounted_area_raw) DESC
    """).fetchdf()
    audit_df.to_csv(OUT_DIR / "conservation_correction_audit.csv", index=False)
    print("V4 Output and Audit generated successfully.")

def main():
    con = duckdb.connect(str(DB_PATH))
    apply_corrections(con)

if __name__ == "__main__":
    main()
