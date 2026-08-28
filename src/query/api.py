import duckdb
import pandas as pd
from typing import List, Dict, Any, Optional

class DistrictQueryAPI:
    def __init__(self, db_path: str = "data/gold/district_evolution.duckdb"):
        self.con = duckdb.connect(db_path, read_only=True)
        # Ensure views exist (they will be created by the build script)

    def get_district_history(self, canonical_key: str) -> pd.DataFrame:
        query = \"\"\"
        SELECT * FROM vw_district_snapshot 
        WHERE canonical_key = ?
        ORDER BY year ASC
        \"\"\"
        return self.con.execute(query, [canonical_key]).fetchdf()

    def get_district_lineage(self, canonical_key: str) -> pd.DataFrame:
        query = \"\"\"
        SELECT * FROM vw_district_lineage 
        WHERE parent_ck = ? OR child_ck = ?
        ORDER BY effective_year ASC
        \"\"\"
        return self.con.execute(query, [canonical_key, canonical_key]).fetchdf()

    def get_boundary_changes(self, year_from: int, year_to: int) -> pd.DataFrame:
        query = \"\"\"
        SELECT * FROM vw_boundary_events
        WHERE effective_year >= ? AND effective_year <= ?
        ORDER BY effective_year ASC
        \"\"\"
        return self.con.execute(query, [year_from, year_to]).fetchdf()

    def get_crosswalk(self, source_snapshot_id: int, target_year: int) -> pd.DataFrame:
        query = \"\"\"
        SELECT * FROM vw_statistical_crosswalk
        WHERE from_snapshot_id = ? AND to_year = ?
        \"\"\"
        return self.con.execute(query, [source_snapshot_id, target_year]).fetchdf()

    def get_usable_crosswalk(
        self,
        minimum_coverage: float = 0.85,
        allow_normalized: bool = True,
        allow_unmeasured: bool = False,
        allow_questionable: bool = False
    ) -> pd.DataFrame:
        conditions = ["coverage_score >= ?"]
        params = [minimum_coverage]
        
        status_in = ["'MEASURED'"]
        if allow_normalized:
            status_in.append("'MEASURED_NORMALIZED'")
        if allow_unmeasured:
            status_in.append("'UNMEASURED'")
        if allow_questionable:
            status_in.append("'QUESTIONABLE'")
            
        conditions.append(f"crosswalk_status IN ({','.join(status_in)})")
        
        where_clause = " AND ".join(conditions)
        query = f"SELECT * FROM vw_usable_crosswalk WHERE {where_clause}"
        return self.con.execute(query, params).fetchdf()

    def get_validation_status(self, canonical_key: str) -> pd.DataFrame:
        query = \"\"\"
        SELECT 
            rule_id, severity, message, is_resolved 
        FROM validation_result 
        WHERE canonical_key = ?
        \"\"\"
        return self.con.execute(query, [canonical_key]).fetchdf()

    def get_district_area(self, canonical_key: str, year: int) -> pd.DataFrame:
        query = """
        SELECT * FROM vw_district_area_timeseries 
        WHERE canonical_key = ? AND year = ?
        """
        return self.con.execute(query, [canonical_key, year]).fetchdf()

    def get_district_area_history(self, canonical_key: str) -> pd.DataFrame:
        query = """
        SELECT * FROM vw_district_area_timeseries 
        WHERE canonical_key = ?
        ORDER BY year ASC
        """
        return self.con.execute(query, [canonical_key]).fetchdf()

    def get_area_change(self, canonical_key: str) -> pd.DataFrame:
        query = """
        SELECT * FROM vw_district_area_change 
        WHERE canonical_key = ?
        ORDER BY year_to ASC
        """
        return self.con.execute(query, [canonical_key]).fetchdf()

    def get_area_transfer(self, source_ck: str, target_year: int) -> pd.DataFrame:
        query = """
        SELECT * FROM vw_district_area_transfer 
        WHERE from_ck = ? AND to_year = ?
        ORDER BY area_transferred_km2 DESC
        """
        return self.con.execute(query, [source_ck, target_year]).fetchdf()

    def get_event_area_accounting(self, event_id: str) -> pd.DataFrame:
        query = """
        SELECT * FROM vw_event_area_accounting 
        WHERE event_id = ?
        ORDER BY target_district_name ASC
        """
        return self.con.execute(query, [event_id]).fetchdf()

    def get_district_area_ledger(self, canonical_key: str) -> pd.DataFrame:
        query = """
        SELECT * FROM vw_district_area_ledger 
        WHERE canonical_key = ?
        ORDER BY effective_year ASC
        """
        return self.con.execute(query, [canonical_key]).fetchdf()

    def get_area_conservation(self, event_id: str) -> pd.DataFrame:
        query = """
        SELECT 
            event_id,
            total_area_before_km2,
            total_area_transferred_km2,
            total_area_retained_km2,
            total_unaccounted_area_km2,
            conservation_error_km2,
            conservation_error_pct
        FROM vw_area_transfer_summary
        WHERE event_id = ?
        """
        return self.con.execute(query, [event_id]).fetchdf()

    def load_event_register(self) -> pd.DataFrame:
        """Loads the full 1550 event register."""
        path = os.path.join(self.products_dir, "event_register.parquet")
        return pd.read_parquet(path)
        
    def load_event_unmeasured(self) -> pd.DataFrame:
        """Loads diagnostic unmeasured events."""
        path = os.path.join(self.products_dir, "event_unmeasured.parquet")
        return pd.read_parquet(path)
        
    def load_event_coverage_summary(self) -> pd.DataFrame:
        """Loads coverage statistics."""
        path = os.path.join(self.products_dir, "event_coverage_summary.parquet")
        return pd.read_parquet(path)
        
    def load_area_transfer_matrix(self, long_form: bool = True) -> pd.DataFrame:
        """
        Loads the long-form area transfer matrix connecting source and target districts.
        """
        path = os.path.join(self.products_dir, "district_area_transfer.parquet")
        return pd.read_parquet(path)

    def get_area_transfer_matrix(self, year_from: int, year_to: int) -> pd.DataFrame:
        # Returns the long-form matrix as requested
        query = """
        SELECT from_ck, to_ck, area_transferred_km2 
        FROM vw_district_area_transfer 
        WHERE from_year = ? AND to_year = ?
        """
        return self.con.execute(query, [year_from, year_to]).fetchdf()

    def close(self):
        self.con.close()
