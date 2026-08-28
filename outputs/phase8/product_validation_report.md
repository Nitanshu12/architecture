# Product Validation Report

## Reconciliation

| Dataset               |   ProductRows |   SourceRows |   Difference | Status   |
|:----------------------|--------------:|-------------:|-------------:|:---------|
| district_identity     |           964 |          964 |            0 | PASS     |
| district_snapshot     |          4351 |         4351 |            0 | PASS     |
| district_lineage      |           551 |          551 |            0 | PASS     |
| boundary_events       |          1550 |         1550 |            0 | PASS     |
| statistical_crosswalk |         46388 |        46388 |            0 | PASS     |
| usable_crosswalk      |         38287 |        46388 |         8101 | FILTERED |


## Referential Integrity Checks

- Snapshots missing canonical_key in identity table: 0

- Lineage parent_ck missing in identity table: 0


## Policy Exclusions

- Total UNMEASURED records preserved: 6696

- Total LOW_COVERAGE records preserved: 1405

- Total MEASURED_NORMALIZED records preserved: 28289

- Total records excluded in usable dataset: 8101
