# Area Transfer Definition

## Definition
An **Area Transfer** is defined as the measurable spatial allocation of a source district's territory to one or more target districts between two distinct temporal observations (boundary snapshots). 

## Formalism
Let $S$ be the source district at time $t_1$.
Let $T$ be a target district at time $t_2$.

1. **Intersection Area (km²)**:
   The physical territory shared between the two snapshots:
   $Intersection(S, T) = Area(Geometry(S) \cap Geometry(T))$

2. **Raw Transfer Weight**:
   The fraction of the source district's territory allocated to the target:
   $W_{raw} = \frac{Intersection(S, T)}{Area(S)}$

3. **Coverage Score**:
   The total fraction of the source district's territory accounted for by all identified target districts at time $t_2$:
   $Coverage(S) = \frac{Area(\bigcup (Geometry(S) \cap Geometry(T_i)))}{Area(S)}$

4. **Overlap Excess**:
   The difference between the simple sum of raw transfer weights and the true geometric union coverage score. A non-zero overlap excess ($> 0$) indicates that target boundaries overlap with each other over the source territory.
   $Excess(S) = \sum W_{raw} - Coverage(S)$

## Key Distinctions
- **`raw_transfer_weight`**: Represents spatial geometry facts.
- **`statistical_weight`**: Represents an epistemological claim (e.g., population distribution). While often identical to the raw transfer weight (Area Weighting assumption), it can diverge significantly if ancillary data (e.g., gridded population rasters) are used for apportionment.
- **`was_normalized`**: Indicates if the statistical weight was adjusted proportionally because target geometries overlapped and their raw weights summed to $> 1.0$.

## Lineage Independence
Area Transfers exist as spatial facts. They do NOT inherently dictate administrative lineage. A transfer of 200 km² does not definitively prove a `CARVE_OUT` versus a `CLEAN_SPLIT`. Lineage events are derived from Gazette text; Area Transfers merely quantify the territorial consequences.
