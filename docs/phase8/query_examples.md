# Phase 8 Query Examples

The following executable queries leverage the DuckDB query layer to interrogate the District Evolution Intelligence System.

### 1. Find the history of a district
```sql
SELECT year, district_name, state, area 
FROM vw_district_snapshot 
WHERE canonical_key = 'IND-000156'
ORDER BY year ASC;
```

### 2. Find all districts created between two years
```sql
SELECT canonical_key, display_name, first_observed_year 
FROM vw_district_identity 
WHERE first_observed_year BETWEEN 1991 AND 2001;
```

### 3. Find parents of a district
```sql
SELECT parent_ck, relationship_type, effective_year 
FROM vw_district_lineage 
WHERE child_ck = 'IND-003996';
```

### 4. Find children of a district
```sql
SELECT child_ck, relationship_type, effective_year 
FROM vw_district_lineage 
WHERE parent_ck = 'IND-000156';
```

### 5. Calculate district area change
```sql
SELECT 
    s1.year as previous_year, s2.year as current_year,
    s1.area as previous_area, s2.area as current_area,
    (s2.area - s1.area) as area_diff
FROM vw_district_snapshot s1
JOIN vw_district_snapshot s2 ON s1.canonical_key = s2.canonical_key 
WHERE s1.canonical_key = 'IND-000156' AND s1.year < s2.year;
```

### 6. Find districts affected by splits
```sql
SELECT parent_ck, child_ck, effective_year 
FROM vw_district_lineage 
WHERE relationship_type = 'SPLIT_FROM';
```

### 7. Find districts affected by mergers
```sql
SELECT parent_ck, child_ck, effective_year 
FROM vw_district_lineage 
WHERE relationship_type = 'MERGED_INTO';
```

### 8. Retrieve a statistical crosswalk
```sql
SELECT from_snapshot_id, to_snapshot_id, raw_area_weight, statistical_weight, crosswalk_status 
FROM vw_statistical_crosswalk 
WHERE from_ck = 'IND-000156';
```

### 9. Retrieve only scientifically usable crosswalks
```sql
SELECT from_snapshot_id, to_snapshot_id, statistical_weight 
FROM vw_usable_crosswalk 
WHERE crosswalk_status IN ('MEASURED', 'MEASURED_NORMALIZED');
```

### 10. Identify low-coverage relationships
```sql
SELECT from_ck, to_ck, coverage_score 
FROM vw_statistical_crosswalk 
WHERE crosswalk_status = 'LOW_COVERAGE';
```

### 11. Identify normalized relationships
```sql
SELECT from_ck, to_ck, raw_area_weight, statistical_weight, overlap_excess 
FROM vw_statistical_crosswalk 
WHERE was_normalized = TRUE;
```

### 12. Identify UNMEASURED relationships
```sql
SELECT from_ck, to_ck, measurement_status 
FROM vw_statistical_crosswalk 
WHERE crosswalk_status = 'UNMEASURED';
```

### 13. Trace a harmonized value back to its evidence
```sql
SELECT 
    sc.from_ck, sc.to_ck, sc.evidence_type, 
    sc.overlap_excess, sc.distribution_assumption
FROM vw_statistical_crosswalk sc
WHERE sc.from_ck = 'IND-003907';
```
