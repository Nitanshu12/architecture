import duckdb
import pandas as pd
from pathlib import Path

PROJECT_ROOT = Path('/Users/satyamkumar/Desktop/DistrictEvolution_Final')
DB_PATH = PROJECT_ROOT / "data" / "gold" / "district_evolution.duckdb"
PROD_DIR = PROJECT_ROOT / "data" / "products"

def build_v3(con):
    print("Building Event Area Summary V3...")
    
    con.execute("""
    CREATE OR REPLACE VIEW vw_event_area_summary_v3 AS
    WITH rels AS (
        SELECT supporting_event_id as event_id, relationship_type
        FROM district_relationship
    ),
    event_semantics AS (
        SELECT 
            event_id, 
            MAX(relationship_type) as relationship_type
        FROM rels GROUP BY event_id
    ),
    retained_agg AS (
        SELECT event_id, from_ck, SUM(raw_intersection_area_km2) as retained_area
        FROM vw_event_area_accounting_v2 
        WHERE from_ck = to_ck
        GROUP BY event_id, from_ck
    ),
    transferred_agg AS (
        SELECT event_id, from_ck, SUM(raw_intersection_area_km2) as raw_transferred_area
        FROM vw_event_area_accounting_v2 
        WHERE from_ck != to_ck
        GROUP BY event_id, from_ck
    ),
    base AS (
        SELECT 
            e.event_id::VARCHAR as event_id,
            MAX(e.event_type) as event_type,
            MAX(e.effective_year) as effective_year,
            MAX(a.from_area_km2) as source_area_before_km2,
            
            MAX(COALESCE(r.retained_area, 0)) as retained_area,
            
            -- transferred_area: total union covered minus retained, capped at 0 just in case
            GREATEST(MAX(a.physical_union_covered_area_km2) - MAX(COALESCE(r.retained_area, 0)), 0) as transferred_area,
            
            -- unaccounted_area validated against actual geometry gap
            MAX(a.from_area_km2) - MAX(a.physical_union_covered_area_km2) as unaccounted_area,
            
            MAX(a.overlap_area_km2) as multi_target_overlap_km2,
            MAX(a.overlap_status) as overlap_status,
            MAX(a.measurement_status) as measurement_status,
            
            MAX(es.relationship_type) as relationship_type,
            
            -- Explicit geometry diagnosis
            MAX(CASE 
                WHEN e.event_type = 'RENAME' THEN 'RENAME events are nominal identity changes without spatial transfers.'
                WHEN a.overlap_status = 'INVALID_GEOMETRY' AND a.from_area_km2 IS NULL AND a.to_area_km2 IS NULL THEN 'Missing source and target geometry observations.'
                WHEN a.overlap_status = 'INVALID_GEOMETRY' AND a.from_area_km2 IS NULL THEN 'Missing source geometry observation.'
                WHEN a.overlap_status = 'INVALID_GEOMETRY' AND a.to_area_km2 IS NULL THEN 'Missing target geometry observation.'
                ELSE 'VALID'
            END) as geometry_diagnosis
            
        FROM vw_event_register e
        LEFT JOIN vw_event_area_accounting_v2 a ON e.event_id = a.event_id
        LEFT JOIN retained_agg r ON a.event_id = r.event_id AND a.from_ck = r.from_ck
        LEFT JOIN transferred_agg t ON a.event_id = t.event_id AND a.from_ck = t.from_ck
        LEFT JOIN event_semantics es ON e.event_id = es.event_id
        GROUP BY e.event_id
    )
    SELECT 
        event_id,
        event_type,
        effective_year,
        
        CASE WHEN event_type = 'RENAME' THEN NULL ELSE source_area_before_km2 END as source_area_before_km2,
        CASE WHEN event_type = 'RENAME' THEN NULL ELSE retained_area END as retained_area,
        CASE WHEN event_type = 'RENAME' THEN NULL ELSE transferred_area END as transferred_area,
        CASE WHEN event_type = 'RENAME' THEN NULL ELSE unaccounted_area END as unaccounted_area,
        CASE WHEN event_type = 'RENAME' THEN NULL ELSE multi_target_overlap_km2 END as multi_target_overlap_km2,
        
        overlap_status,
        
        CASE WHEN event_type = 'RENAME' THEN 'NON_SPATIAL' ELSE measurement_status END as measurement_status,
        
        CASE 
            WHEN relationship_type = 'SPLIT_FROM' THEN 'CLEAN_SPLIT'
            WHEN relationship_type = 'FORMED_FROM' THEN 'CARVE_OUT'
            ELSE 'UNKNOWN'
        END as split_semantics,
        
        geometry_diagnosis
        
    FROM base
    """)
    
    df = con.execute("SELECT * FROM vw_event_area_summary_v3").fetchdf()
    df.to_csv(PROD_DIR / "event_area_summary_v3.csv", index=False)
    df.to_parquet(PROD_DIR / "event_area_summary_v3.parquet", index=False)
    print("V3 Export Complete.")

def main():
    con = duckdb.connect(str(DB_PATH))
    con.execute("INSTALL spatial; LOAD spatial;")
    build_v3(con)

if __name__ == "__main__":
    main()
