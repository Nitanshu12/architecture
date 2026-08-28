# Area Accounting Definition

## Overview
Area Accounting is the formal, event-centric methodology for quantifying territorial changes across administrative boundaries. It ensures that every square kilometer of a district is tracked during an administrative event.

## Core Concepts

1. **Area Before (`area_before_km2`)**:
   Area of the source district before the administrative change.

2. **Area After (`area_after_km2`)**:
   Area of the relevant district after the administrative change.

3. **Area Retained (`area_retained_km2`)**:
   Area of the source district that remains associated with the same district identity after the event.

4. **Area Relinquished (`area_relinquished_km2`)**:
   Area formerly associated with the source district that is no longer associated with that district after the event.

5. **Area Received (`area_received_km2`)**:
   Area newly associated with the target district that originated from another district.

6. **Area Transferred (`area_transferred_km2`)**:
   Measured area that moves from a source district to a target district.

7. **Area Shared / Overlapping (`overlap_area_km2`)**:
   Area where multiple target geometries overlap spatially and therefore cannot be interpreted as unique territorial transfer without an explicit allocation assumption.

8. **Area Unaccounted (`unaccounted_area_km2`)**:
   Source area that cannot be explained by the available spatial evidence.

9. **Net Area Change (`net_area_change_km2`)**:
   `area_after_km2 - area_before_km2`

10. **Area Conservation Error (`conservation_error_km2`)**:
    `abs(area_before_km2 - (area_retained_km2 + SUM(area_transferred_km2)))`

## Grain
The primary analytical grain is **ONE ROW = ONE ADMINISTRATIVE EVENT × ONE SOURCE/TARGET DISTRICT RELATIONSHIP**.

## The District Area Ledger
The `district_area_ledger` aggregates the event accounting to a district level. It answers: *During this event, what happened to this district's territory?*
