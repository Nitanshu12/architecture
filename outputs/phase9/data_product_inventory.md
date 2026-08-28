# Phase 9 Final Data Product Inventory

### `event_register`
- **Grain**: One Source Administrative Event
- **Row Count**: 1550
- **Purpose**: "What happened?" Describes the real-world historical event.
- **Primary Key**: `event_id`
- **NULL Semantics**: Spatial fields are inherently excluded.

### `event_relationship`
- **Grain**: Event × Source District × Target District
- **Row Count**: 1566
- **Purpose**: Defines the topological mapping between old and new identities.
- **Primary Key**: `relationship_id`
- **Foreign Keys**: `event_id`, `from_ck`, `to_ck`
- **NULL Semantics**: Missing relationships are absent.

### `event_area_accounting`
- **Grain**: Event × District Relationship
- **Row Count**: 1566
- **Purpose**: Core area transfer accounting and topological measurement.
- **Primary Key**: `relationship_id`
- **Foreign Keys**: `event_id`, `from_ck`, `to_ck`
- **Area Units**: Equal-Area projected Square Kilometers (`km²`)
- **NULL Semantics**: NULL means "not measurable / not applicable" (e.g. unmeasured geometries yield NULL, not 0.0).

### `event_area_summary`
- **Grain**: Source Administrative Event
- **Row Count**: 1550
- **Purpose**: Primary researcher-facing event-level area conservation rollups.
- **Primary Key**: `event_id`
- **Area Units**: `km²`
- **NULL Semantics**: Sums of NULLs yield NULL where spatial measurement cannot be completed.
