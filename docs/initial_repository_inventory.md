# Initial Repository Inventory

| PATH | TYPE | PURPOSE | STATUS | RELEVANCE TO v0.3 | KEEP / ARCHIVE / REPLACE / UNKNOWN |
|---|---|---|---|---|---|
| ARCHITECTURE.md | Doc | Unknown | Active | Medium | KEEP |
| LICENSE | Config | Project configuration/Metadata | Active | Medium | KEEP |
| Makefile | Config | Project configuration/Metadata | Active | Medium | KEEP |
| README.md | Doc | Project configuration/Metadata | Active | Medium | KEEP |
| config/matching.yaml | Config | Configuration | Active | Medium | KEEP |
| config/pipeline.yaml | Config | Configuration | Active | Medium | KEEP |
| config/sources.yaml | Config | Configuration | Active | Medium | KEEP |
| config/spatial.yaml | Config | Configuration | Active | Medium | KEEP |
| config/validation.yaml | Config | Configuration | Active | Medium | KEEP |
| data/bronze/events/district_evolution_master.csv | Data | Source Data | Active | High | KEEP |
| data/bronze/soi/2025/2025.gpkg | Data | Source Data | Active | High | KEEP |
| data/bronze/stanford/1951/1951.gpkg | Data | Source Data | Active | High | KEEP |
| data/bronze/stanford/1961/1961.gpkg | Data | Source Data | Active | High | KEEP |
| data/bronze/stanford/1971/1971.gpkg | Data | Source Data | Active | High | KEEP |
| data/bronze/stanford/1981/1981.gpkg | Data | Source Data | Active | High | KEEP |
| data/bronze/stanford/1991/1991.gpkg | Data | Source Data | Active | High | KEEP |
| data/bronze/stanford/2001/2001.gpkg | Data | Source Data | Active | High | KEEP |
| data/bronze/stanford/2011/2011.gpkg | Data | Source Data | Active | High | KEEP |
| data/bronze/stanford/2021/2021.gpkg | Data | Source Data | Active | High | KEEP |
| docs/architecture/ARCHITECTURE.md | Doc | Architecture/Documentation | Active | High | KEEP |
| docs/architecture/architecture_decisions.md | Doc | Architecture/Documentation | Active | High | KEEP |
| docs/architecture/changelog.md | Doc | Architecture/Documentation | Active | High | KEEP |
| docs/architecture/open_decisions.md | Doc | Architecture/Documentation | Active | High | KEEP |
| docs/domain/domain_definitions.md | Doc | Architecture/Documentation | Active | High | KEEP |
| docs/domain/event_rules.md | Doc | Architecture/Documentation | Active | High | KEEP |
| docs/domain/identity_rules.md | Doc | Architecture/Documentation | Active | High | KEEP |
| docs/domain/lineage_rules.md | Doc | Architecture/Documentation | Active | High | KEEP |
| docs/domain/temporal_rules.md | Doc | Architecture/Documentation | Active | High | KEEP |
| docs/harmonization/statistical_harmonization.md | Doc | Architecture/Documentation | Active | High | KEEP |
| docs/spatial/crosswalk_rules.md | Doc | Architecture/Documentation | Active | High | KEEP |
| docs/spatial/geometry_rules.md | Doc | Architecture/Documentation | Active | High | KEEP |
| docs/spatial/reconciliation_rules.md | Doc | Architecture/Documentation | Active | High | KEEP |
| docs/validation/invariants.md | Doc | Architecture/Documentation | Active | High | KEEP |
| docs/validation/validation_framework.md | Doc | Architecture/Documentation | Active | High | KEEP |
| env.example | Config | Project configuration/Metadata | Active | Medium | KEEP |
| gitignore | Config | Project configuration/Metadata | Active | Medium | KEEP |
| pyproject.toml | Config | Project configuration/Metadata | Active | Medium | KEEP |
| sql/schema/bronze.sql | Config | Database Schema | Active | Medium | KEEP |
| sql/schema/gold_core.sql | Config | Database Schema | Active | Medium | KEEP |
| sql/schema/gold_events.sql | Config | Database Schema | Active | Medium | KEEP |
| sql/schema/gold_harmonization.sql | Config | Database Schema | Active | Medium | KEEP |
| sql/schema/gold_spatial.sql | Config | Database Schema | Active | Medium | KEEP |
| sql/schema/silver.sql | Config | Database Schema | Active | Medium | KEEP |
| sql/schema/validation.sql | Config | Database Schema | Active | Medium | KEEP |
| src/bronze/immutability.py | Code | Pipeline code | Active | High | KEEP |
| src/bronze/loader.py | Code | Pipeline code | Active | High | KEEP |
| src/bronze/manifest.py | Code | Pipeline code | Active | High | KEEP |
| src/core/administrative_units.py | Code | Pipeline code | Active | High | KEEP |
| src/core/districts.py | Code | Pipeline code | Active | High | KEEP |
| src/core/snapshots.py | Code | Pipeline code | Active | High | KEEP |
| src/core/time.py | Code | Pipeline code | Active | High | KEEP |
| src/database/connection.py | Code | Pipeline code | Active | High | KEEP |
| src/events/classifier.py | Code | Pipeline code | Active | High | KEEP |
| src/events/event_builder.py | Code | Pipeline code | Active | High | KEEP |
| src/events/evidence.py | Code | Pipeline code | Active | High | KEEP |
| src/events/participants.py | Code | Pipeline code | Active | High | KEEP |
| src/harmonization/allocation.py | Code | Pipeline code | Active | High | KEEP |
| src/harmonization/crosswalk.py | Code | Pipeline code | Active | High | KEEP |
| src/harmonization/methods/area_weighted.py | Code | Pipeline code | Active | High | KEEP |
| src/harmonization/methods/cropland_weighted.py | Code | Pipeline code | Active | High | KEEP |
| src/harmonization/methods/irrigation_weighted.py | Code | Pipeline code | Active | High | KEEP |
| src/harmonization/methods/population_weighted.py | Code | Pipeline code | Active | High | KEEP |
| src/harmonization/observations.py | Code | Pipeline code | Active | High | KEEP |
| src/identity/canonical.py | Code | Pipeline code | Active | High | KEEP |
| src/identity/ck_registry.py | Code | Pipeline code | Active | High | KEEP |
| src/identity/mapping.py | Code | Pipeline code | Active | High | KEEP |
| src/identity/matching/matcher.py | Code | Pipeline code | Active | High | KEEP |
| src/identity/matching/quarantine.py | Code | Pipeline code | Active | High | KEEP |
| src/identity/matching/scoring.py | Code | Pipeline code | Active | High | KEEP |
| src/lineage/cycle_detection.py | Code | Pipeline code | Active | High | KEEP |
| src/lineage/graph.py | Code | Pipeline code | Active | High | KEEP |
| src/lineage/relationships.py | Code | Pipeline code | Active | High | KEEP |
| src/lineage/traversal.py | Code | Pipeline code | Active | High | KEEP |
| src/pipeline/orchestrator.py | Code | Pipeline code | Active | High | KEEP |
| src/pipeline/run_context.py | Code | Pipeline code | Active | High | KEEP |
| src/pipeline/stages.py | Code | Pipeline code | Active | High | KEEP |
| src/provenance/corrections.py | Code | Pipeline code | Active | High | KEEP |
| src/provenance/derivation.py | Code | Pipeline code | Active | High | KEEP |
| src/provenance/evidence.py | Code | Pipeline code | Active | High | KEEP |
| src/provenance/runs.py | Code | Pipeline code | Active | High | KEEP |
| src/silver/geometry/precision.py | Code | Pipeline code | Active | High | KEEP |
| src/silver/geometry/repair.py | Code | Pipeline code | Active | High | KEEP |
| src/silver/geometry/transform.py | Code | Pipeline code | Active | High | KEEP |
| src/silver/geometry/validation.py | Code | Pipeline code | Active | High | KEEP |
| src/silver/provenance/transformation_log.py | Code | Pipeline code | Active | High | KEEP |
| src/silver/reconciliation/candidates.py | Code | Pipeline code | Active | High | KEEP |
| src/silver/standardization/attributes.py | Code | Pipeline code | Active | High | KEEP |
| src/silver/standardization/dates.py | Code | Pipeline code | Active | High | KEEP |
| src/silver/standardization/names.py | Code | Pipeline code | Active | High | KEEP |
| src/spatial/area.py | Code | Pipeline code | Active | High | KEEP |
| src/spatial/crosswalk.py | Code | Pipeline code | Active | High | KEEP |
| src/spatial/overlap.py | Code | Pipeline code | Active | High | KEEP |
| src/spatial/reconciliation.py | Code | Pipeline code | Active | High | KEEP |
| src/validation/engine.py | Code | Pipeline code | Active | High | KEEP |
| src/validation/invariants.py | Code | Pipeline code | Active | High | KEEP |
| src/validation/promotion.py | Code | Pipeline code | Active | High | KEEP |
| src/validation/rules/harmonization.py | Code | Pipeline code | Active | High | KEEP |
| src/validation/rules/identity.py | Code | Pipeline code | Active | High | KEEP |
| src/validation/rules/lineage.py | Code | Pipeline code | Active | High | KEEP |
| src/validation/rules/provenance.py | Code | Pipeline code | Active | High | KEEP |
| src/validation/rules/spatial.py | Code | Pipeline code | Active | High | KEEP |
| src/validation/rules/temporal.py | Code | Pipeline code | Active | High | KEEP |
