# Architecture <-> Data Compatibility

| REQUIREMENT | DATA SUPPORT | DATA GAP | RISK | RECOMMENDATION |
|---|---|---|---|---|
| Temporal DATE fields (No NULLs) | Events have `effective_year` | Exact dates missing, only years available | High | Map `YYYY` to `YYYY-01-01` and use `valid_from_precision = 'YEAR'`. |
| Unique Source PK | Stanford datasets have varied PKs | No uniform PK across all 9 datasets | Medium | Extract year-specific PKs (e.g., `DIS01_ID`, `JID`) and map carefully in bronze layer. |
| Split Semantics (CLEAN_SPLIT vs CARVE_OUT) | Events CSV has `event_type` and parents/children | Does not explicitly declare split case | High | Must infer split case based on parent continuation, or quarantine for manual review. |
| Source Immutability | Datasets are available in raw GPKG/CSV | None | Low | Read raw formats in pipeline, never overwrite. |
| Geometry Validations | GPKGs contain valid multipolygons mostly | SOI has mixed Polygon/MultiPolygon and LCC CRS | Medium | Implement standard `ST_MakeValid` and CRS transformation pipeline. |
