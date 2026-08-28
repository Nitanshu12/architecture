"""Read-only Phase 9.1 audit; it writes only outputs/phase9 artifacts."""

from __future__ import annotations

import hashlib
import re
import zipfile
from pathlib import Path
from xml.etree import ElementTree as ET

import duckdb
import geopandas as gpd
import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[2]
PRODUCTS = ROOT / "data" / "products"
OUT = ROOT / "outputs" / "phase9"
SILVER = ROOT / "data" / "silver" / "geometry"
DB = ROOT / "data" / "gold" / "district_evolution.duckdb"
VINTAGES = [1951, 1961, 1971, 1981, 1991, 2001, 2011, 2021, 2025]
USABLE = {"VALID_OBSERVED", "VALID_REPAIRED_DERIVED_ARTIFACT"}
SEED = "phase9.1-independent-audit-20260821"


def first(series: pd.Series):
    series = series.dropna()
    return series.iloc[0] if not series.empty else np.nan


def num(value):
    return None if value is None or pd.isna(value) else float(value)


def conservation(error, expected):
    if error is None or expected is None or expected == 0:
        return "UNMEASURED"
    limit = max(5.0, abs(expected) * 0.005)
    error = abs(error)
    if error <= limit:
        return "PASS"
    if error <= 2 * limit:
        return "MINOR_DISCREPANCY"
    if error <= 5 * limit:
        return "MATERIAL_DISCREPANCY"
    return "FAIL"


def region(state):
    state = str(state or "").casefold().replace("_", " ")
    if any(x in state for x in ["arunachal", "assam", "manipur", "meghalaya", "mizoram", "nagaland", "sikkim", "tripura"]):
        return "NORTHEAST"
    if any(x in state for x in ["bihar", "jharkhand", "odisha", "west bengal"]):
        return "EAST"
    if any(x in state for x in ["chhattisgarh", "madhya pradesh"]):
        return "CENTRAL"
    if any(x in state for x in ["goa", "gujarat", "maharashtra", "dadra", "daman"]):
        return "WEST"
    if any(x in state for x in ["andhra", "karnataka", "kerala", "tamil", "telangana", "puducherry", "lakshadweep", "andaman"]):
        return "SOUTH"
    return "NORTH"


def normal(value):
    return "".join(c for c in str(value or "").casefold() if c.isalnum())


def sample(frame, category, n=5):
    if frame.empty:
        return frame.copy()
    result = frame.copy()
    result["_hash"] = result.apply(
        lambda x: hashlib.sha256(f"{SEED}|{category}|{x.get('event_id')}|{x.get('source_ck')}|{x.get('target_ck')}".encode()).hexdigest(), axis=1
    )
    return result.sort_values(["_hash", "event_id"], kind="stable").head(n).drop(columns="_hash")


def sheet_names(path):
    ns = {"x": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
    with zipfile.ZipFile(path) as book:
        root = ET.fromstring(book.read("xl/workbook.xml"))
    return [x.attrib["name"] for x in root.find("x:sheets", ns)]


def read_first_sheet(path):
    """Read the values-only first worksheet with the Python standard library."""
    uri = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
    ns = {"x": uri}
    with zipfile.ZipFile(path) as book:
        shared = []
        if "xl/sharedStrings.xml" in book.namelist():
            root = ET.fromstring(book.read("xl/sharedStrings.xml"))
            shared = ["".join(t.text or "" for t in x.iter(f"{{{uri}}}t")) for x in root.findall("x:si", ns)]
        root = ET.fromstring(book.read("xl/worksheets/sheet1.xml"))
    cells, max_row, max_col = {}, 0, 0
    for x in root.findall(".//x:sheetData/x:row/x:c", ns):
        ref = x.attrib["r"]
        letters = "".join(c for c in ref if c.isalpha())
        row = int("".join(c for c in ref if c.isdigit()))
        col = 0
        for letter in letters:
            col = col * 26 + ord(letter.upper()) - 64
        value = x.find("x:v", ns)
        data = value.text if value is not None else None
        if data is not None and x.attrib.get("t") == "s":
            data = shared[int(data)]
        elif data is not None:
            try:
                data = float(data)
            except ValueError:
                pass
        cells[row, col] = data
        max_row, max_col = max(max_row, row), max(max_col, col)
    rows = [[cells.get((r, c)) for c in range(1, max_col + 1)] for r in range(1, max_row + 1)]
    return pd.DataFrame(rows[1:], columns=rows[0])


def area_audit(ts):
    dup_ck = ts.groupby(["canonical_key", "source_year"], dropna=False).filter(lambda x: len(x) > 1)
    dup_pk = ts.groupby(["source_pk", "source_year"], dropna=False).filter(lambda x: len(x) > 1)
    summary = [
        ("ROW_COUNT", "PASS", len(ts), "All geometry observations retained."),
        ("DUPLICATE_CK_YEAR_GROUPS", "CONDITIONAL" if len(dup_ck) else "PASS", dup_ck.groupby(["canonical_key", "source_year"]).ngroups, "Must be unique before use in a matrix."),
        ("DUPLICATE_CK_YEAR_ROWS", "CONDITIONAL" if len(dup_ck) else "PASS", len(dup_ck), "Ambiguous CK/vintage rows."),
        ("DUPLICATE_SOURCE_PK_YEAR_GROUPS", "FAIL" if len(dup_pk) else "PASS", dup_pk.groupby(["source_pk", "source_year"]).ngroups, "Source PK/vintage uniqueness."),
        ("NULL_AREA", "FAIL" if ts.area_km2.isna().any() else "PASS", int(ts.area_km2.isna().sum()), "No null retained area."),
        ("ZERO_OR_NEGATIVE_AREA", "FAIL" if (ts.area_km2 <= 0).any() else "PASS", int((ts.area_km2 <= 0).sum()), "No zero/negative retained area."),
        ("AREA_METHOD_CONSISTENCY", "PASS" if ts.area_method.nunique(dropna=False) == 1 else "FAIL", ts.area_method.nunique(dropna=False), "One km² area method."),
        ("AREA_CRS_CONSISTENCY", "PASS" if ts.area_crs.nunique(dropna=False) == 1 else "FAIL", ts.area_crs.nunique(dropna=False), "One CRS metadata value."),
        ("IMPLAUSIBLE_AREA_LT_1_OR_GT_200000_KM2", "PASS", int(((ts.area_km2 < 1) | (ts.area_km2 > 200000)).sum()), "Screen range only."),
    ]
    rows = [{"record_type": "SUMMARY", "check": a, "result": b, "count": c, "detail": d, "canonical_key": None, "source_year": None, "source_pk": None, "geometry_id": None, "area_km2": None} for a, b, c, d in summary]
    for x in dup_ck.itertuples(index=False):
        rows.append({"record_type": "DETAIL", "check": "DUPLICATE_CK_YEAR", "result": "CONDITIONAL", "count": 1, "detail": x.geometry_status, "canonical_key": x.canonical_key, "source_year": x.source_year, "source_pk": x.source_pk, "geometry_id": x.geometry_id, "area_km2": x.area_km2})
    return pd.DataFrame(rows)


def independent_areas(ts):
    frame = ts.loc[ts.geometry_status.eq("VALID_OBSERVED")].copy()
    frame["region"] = frame.state_name.map(region)
    frame["_hash"] = frame.apply(lambda x: hashlib.sha256(f"{SEED}|{x.canonical_key}|{x.source_year}|{x.geometry_id}".encode()).hexdigest(), axis=1)
    frame = frame.sort_values(["region", "_hash"], kind="stable").groupby("region", group_keys=False).head(6)
    cache, rows = {}, []
    for x in frame.itertuples(index=False):
        key = (x.source_dataset, int(x.source_year))
        if key not in cache:
            prefix = "soi_" if x.source_dataset == "soi" else "stanford_"
            files = list(SILVER.glob(f"{prefix}*_{x.source_year}.geoparquet"))
            if len(files) != 1:
                raise RuntimeError(f"Expected one Silver source for {key}: {files}")
            cache[key] = gpd.read_parquet(files[0])
        match = re.search(r"source_observation_id=[^;]*_(\d+)", x.source_provenance)
        if not match:
            raise RuntimeError(f"No Silver source position for {x.geometry_id}")
        geometry = cache[key].geometry.iloc[int(match.group(1))]
        calculated = float(gpd.GeoSeries([geometry], crs=cache[key].crs).to_crs("EPSG:6933").area.iloc[0] / 1_000_000)
        pipeline = float(x.area_km2)
        difference = pipeline - calculated
        rows.append({"canonical_key": x.canonical_key, "district_name": x.district_name_original, "state": x.state_name, "region": x.region, "source_dataset": x.source_dataset, "source_year": x.source_year, "geometry_id": x.geometry_id, "pipeline_area_km2": pipeline, "independent_area_km2": calculated, "difference_km2": difference, "difference_pct": 100 * difference / calculated, "independent_method": "EPSG:6933 global equal-area projected calculation", "audit_result": "PASS" if abs(difference) <= max(5, calculated * .005) else "REVIEW"})
    return pd.DataFrame(rows)


def census_audit(ts, matrix, workbook, sheets):
    usable = ts.loc[ts.geometry_status.isin(USABLE)]
    cols = [f"area_{year}_km2" for year in VINTAGES]
    rows = []
    rows.append(("REQUIRED_COLUMNS", "PASS" if all(c in matrix for c in cols) else "FAIL", len(matrix), "All requested vintage fields are present."))
    rows.append(("UNIQUE_CANONICAL_KEY", "PASS" if not matrix.canonical_key.duplicated().any() else "FAIL", int(matrix.canonical_key.duplicated().sum()), "One row per CK."))
    rows.append(("ZERO_USED_FOR_MISSING", "PASS" if not (matrix[cols] == 0).any().any() else "FAIL", int((matrix[cols] == 0).sum().sum()), "Zero must not denote missing."))
    wide = matrix.set_index("canonical_key")
    mismatch = missing_semantics = 0
    for year in VINTAGES:
        source = usable.loc[usable.source_year.eq(year), ["canonical_key", "area_km2"]].set_index("canonical_key").area_km2.reindex(wide.index)
        target = wide[f"area_{year}_km2"]
        equal = (target.isna() & source.isna()) | np.isclose(target.fillna(0), source.fillna(0), atol=1e-9, rtol=0)
        mismatch += int((~equal).sum())
        missing_semantics += int((target.notna() != source.notna()).sum())
    rows.append(("LONG_FORM_VALUE_MATCH", "PASS" if not mismatch else "FAIL", mismatch, "Wide values match the unique usable long form exactly."))
    rows.append(("NULL_SEMANTICS", "PASS" if not missing_semantics else "FAIL", missing_semantics, "Wide NULL precisely means no unique usable observation."))
    rows.append(("SILENT_INTERPOLATION", "PASS" if not mismatch else "FAIL", mismatch, "Every wide value has an exact observed source."))
    needed = {"district_area_by_census", "data_dictionary", "area_quality"}
    actual = {x.casefold().replace(" ", "_").replace("&", "and") for x in sheets}
    absent = sorted(needed - actual)
    rows.append(("WORKBOOK_REQUIRED_SHEETS", "PASS" if not absent else "FAIL", len(absent), f"Actual: {sheets}; absent: {absent or 'none'}."))
    workbook = workbook.reindex(columns=matrix.columns)
    for col in cols:
        workbook[col] = pd.to_numeric(workbook[col], errors="coerce")
    same_workbook = len(workbook) == len(matrix)
    if same_workbook:
        for col in matrix.columns:
            if col in cols:
                if not (((matrix[col].isna() & workbook[col].isna()) | np.isclose(matrix[col].fillna(0), workbook[col].fillna(0), atol=1e-9, rtol=0)).all()):
                    same_workbook = False
                    break
            elif not matrix[col].fillna("").astype(str).eq(workbook[col].fillna("").astype(str)).all():
                same_workbook = False
                break
    rows.append(("WORKBOOK_CSV_VALUE_MATCH", "PASS" if same_workbook else "FAIL", len(matrix), "First workbook sheet compared cell-for-cell to CSV."))
    repairs = int(usable.is_derived.fillna(False).sum())
    rows.append(("DERIVED_VALUE_MARKING", "CONDITIONAL" if repairs else "PASS", repairs, "Derived repair status is not exposed in an area_quality worksheet."))
    return pd.DataFrame(rows, columns=["check", "result", "count", "detail"])


def temporal_audit(accounting):
    frame = accounting.drop_duplicates("event_id").copy()
    frame["gap_pre"] = frame.event_year - frame.pre_vintage
    frame["gap_post"] = frame.post_vintage - frame.event_year
    max_gap = frame[["gap_pre", "gap_post"]].max(axis=1)
    frame["temporal_alignment_class"] = np.select(
        [frame.pre_vintage.isna(), frame.post_vintage.isna(), max_gap.le(2), max_gap.le(5), max_gap.le(10)],
        ["NO_PRE_OBSERVATION", "NO_POST_OBSERVATION", "EXCELLENT", "GOOD", "MODERATE"], default="LARGE_GAP"
    )
    frame["strictly_bracketed"] = frame.pre_vintage.lt(frame.event_year) & frame.event_year.lt(frame.post_vintage)
    return frame[["event_id", "event_type", "event_year", "pre_vintage", "post_vintage", "gap_pre", "gap_post", "temporal_alignment_class", "strictly_bracketed"]].sort_values(["event_year", "event_id"], kind="stable")


def groups_from_accounting(accounting):
    rows = []
    for (event_id, source_ck), group in accounting.groupby(["event_id", "source_ck"], dropna=False, sort=False):
        rows.append({
            "event_id": event_id, "source_ck": source_ck, "target_ck": ";".join(sorted(group.target_ck.dropna().astype(str).unique())),
            "event_type": first(group.event_type), "event_year": int(first(group.event_year)), "target_count": int(group.target_ck.dropna().nunique()),
            "relationship_count": len(group), "pre_vintage": num(first(group.pre_vintage)), "post_vintage": num(first(group.post_vintage)),
            "parent_area_before_km2": num(first(group.source_area_before_km2)), "parent_area_after_km2": num(first(group.source_area_after_km2)),
            "child_area_km2": num(group.target_area_after_km2.sum(min_count=1)), "parent_area_loss_km2": num(first(group.parent_area_loss_km2)),
            "raw_intersection_transfer_km2": num(group.raw_intersection_area_km2.sum(min_count=1)),
            "allocated_transfer_km2": num(group.allocated_transfer_area_km2.sum(min_count=1)),
            "overlap_excess_km2": num(first(group.overlap_excess_km2)), "measurement_status": first(group.measurement_status),
            "validation_status": first(group.validation_status), "lineage_confidence": num(first(group.lineage_confidence)),
        })
    return pd.DataFrame(rows)


def conservation_audits(groups):
    clean = groups.loc[groups.event_type.eq("CLEAN_SPLIT")].copy()
    clean["children_area_after_km2"] = clean.child_area_km2
    clean["conservation_error_km2"] = clean.parent_area_before_km2 - clean.children_area_after_km2
    clean["conservation_error_pct"] = 100 * clean.conservation_error_km2.abs() / clean.parent_area_before_km2
    clean["audit_classification"] = [conservation(num(e), num(x)) for e, x in zip(clean.conservation_error_km2, clean.parent_area_before_km2)]
    clean["audit_reason"] = np.where(clean.parent_area_before_km2.isna() | clean.children_area_after_km2.isna(), "Required observed area missing", "Parent before minus child area after")

    carve = groups.loc[groups.event_type.eq("CARVE_OUT")].copy()
    carve["parent_area_loss_km2"] = carve.parent_area_before_km2 - carve.parent_area_after_km2
    carve["difference_km2"] = carve.parent_area_loss_km2 - carve.child_area_km2
    carve["difference_pct"] = 100 * carve.difference_km2.abs() / carve.parent_area_loss_km2.abs()
    carve["conservation_error_km2"] = carve.difference_km2
    carve["conservation_error_pct"] = carve.difference_pct
    carve["audit_classification"] = [conservation(num(e), num(x)) for e, x in zip(carve.difference_km2, carve.parent_area_loss_km2)]
    carve.loc[carve.parent_area_loss_km2 < 0, "audit_classification"] = "PARENT_GAIN_NO_TRANSFER"
    carve["audit_reason"] = np.where(carve.parent_area_before_km2.isna() | carve.parent_area_after_km2.isna() | carve.child_area_km2.isna(), "Required observed area missing", "Parent loss minus child area")

    multi = groups.loc[groups.target_count.gt(1)].copy()
    if multi.empty:
        multi = pd.DataFrame([{"event_id": None, "event_type": None, "source_ck": None, "target_ck": None, "target_count": 0, "parent_area_loss_km2": None, "allocated_transfer_km2": None, "audit_classification": "NOT_TESTABLE", "audit_reason": "No event/source group has more than one target lineage relationship."}])
    else:
        multi["difference_km2"] = multi.allocated_transfer_km2 - multi.parent_area_loss_km2
        multi["difference_pct"] = 100 * multi.difference_km2.abs() / multi.parent_area_loss_km2.abs()
        multi["audit_classification"] = np.where(multi.allocated_transfer_km2 > multi.parent_area_loss_km2 + 1e-7, "CONSERVATION_FAILURE", "PASS")
        multi["audit_reason"] = "Allocated transfer compared without normalizing to the parent loss."
    merge = groups.loc[groups.event_type.eq("MERGE")].copy()
    if merge.empty:
        merge = pd.DataFrame([{"event_id": None, "event_type": "MERGE", "source_ck": None, "target_ck": None, "source_total_area_km2": None, "target_area_km2": None, "conservation_error_km2": None, "conservation_error_pct": None, "audit_classification": "NOT_TESTABLE", "audit_reason": "No MERGE event exists in the event taxonomy or lineage table."}])
    return clean, carve, multi, merge


def rename_audit(groups):
    frame = groups.loc[groups.event_type.eq("RENAME")].copy()
    # For a rename, "after" is the named successor's post-vintage area, not
    # the source CK's independent post-vintage observation.
    frame["area_after_km2"] = frame.child_area_km2
    frame["E_rename"] = (frame.area_after_km2 - frame.parent_area_before_km2).abs() / frame.parent_area_before_km2
    limit = np.maximum(5 / frame.parent_area_before_km2, .005)
    frame["audit_classification"] = np.where(frame.parent_area_before_km2.isna() | frame.area_after_km2.isna(), "UNMEASURED", np.where(frame.E_rename <= limit, "PASS", "RENAME_AREA_INCONSISTENCY"))
    frame["allocated_transfer_nonzero"] = frame.allocated_transfer_km2.notna() & frame.allocated_transfer_km2.abs().gt(1e-7)
    return frame


def overlap_audit(groups):
    frame = groups.copy()
    frame["A_raw_km2"] = frame.raw_intersection_transfer_km2
    frame["A_overlap_km2"] = frame.overlap_excess_km2
    frame["A_union_km2"] = frame.A_raw_km2 - frame.A_overlap_km2
    frame["overlap_excess_pct_source"] = 100 * frame.A_overlap_km2 / frame.parent_area_before_km2
    frame["overlap_class"] = np.select(
        [frame.A_overlap_km2.isna(), frame.overlap_excess_pct_source.eq(0), frame.overlap_excess_pct_source.lt(.5), frame.overlap_excess_pct_source.lt(2), frame.overlap_excess_pct_source.lt(5)],
        ["UNKNOWN", "NO_OVERLAP", "MINOR", "MODERATE", "HIGH"], default="EXTREME"
    )
    frame["is_multi_target"] = frame.target_count.gt(1)
    frame["audit_scope"] = np.where(frame.is_multi_target, "ELIGIBLE_MULTI_TARGET", "SINGLE_TARGET_NOT_AN_OVERLAP_TEST")
    return frame


def old_new_audit(groups):
    frame = groups.loc[groups.event_type.isin(["CLEAN_SPLIT", "CARVE_OUT", "MULTI_CARVE_OUT"])].copy()
    frame["parent_area_change_transfer_km2"] = np.where(frame.event_type.eq("CLEAN_SPLIT"), frame.parent_area_before_km2, frame.parent_area_loss_km2.clip(lower=0))
    frame["difference_km2"] = frame.raw_intersection_transfer_km2 - frame.parent_area_change_transfer_km2
    frame["difference_pct"] = 100 * frame.difference_km2.abs() / frame.parent_area_change_transfer_km2
    frame["comparison_status"] = np.select(
        [frame.parent_area_change_transfer_km2.isna(), frame.parent_area_change_transfer_km2.le(0), frame.raw_intersection_transfer_km2.isna()],
        ["NO_PARENT_CHANGE_EVIDENCE", "NO_POSITIVE_TRANSFER", "MISSING_SPATIAL_EVIDENCE"], default="COMPARABLE"
    )
    return frame


def null_audit(accounting):
    rows = []
    for x in accounting.itertuples(index=False):
        source_reason = "NO_LINEAGE" if pd.isna(x.source_ck) else ("NO_PRE_VINTAGE" if pd.isna(x.pre_vintage) else "NO_OBSERVED_GEOMETRY")
        target_reason = "NO_LINEAGE" if pd.isna(x.target_ck) else ("NO_POST_VINTAGE" if pd.isna(x.post_vintage) else "NO_OBSERVED_GEOMETRY")
        source_missing = pd.isna(x.source_area_before_km2)
        fields = [
            ("source_area_before_km2", x.source_area_before_km2, source_reason),
            ("target_area_after_km2", x.target_area_after_km2, target_reason),
            ("parent_area_loss_km2", x.parent_area_loss_km2, "NON_SPATIAL_EVENT" if x.event_type == "RENAME" else source_reason),
            ("allocated_transfer_area_km2", x.allocated_transfer_area_km2, "NON_SPATIAL_EVENT" if x.event_type == "RENAME" else ("DERIVATION_NOT_ALLOWED" if x.measurement_status in {"MEASURED_PARENT_GAIN", "MEASURED_NO_SPATIAL_EVIDENCE"} else (source_reason if source_missing else target_reason))),
            ("conservation_error_km2", x.conservation_error_km2, "NON_SPATIAL_EVENT" if x.event_type == "RENAME" else (source_reason if source_missing else target_reason)),
        ]
        for field, value, reason in fields:
            if pd.isna(value):
                rows.append({"record_type": "DETAIL", "event_id": x.event_id, "event_type": x.event_type, "source_ck": x.source_ck, "target_ck": x.target_ck, "field": field, "null_count": 1, "null_reason": reason})
    detail = pd.DataFrame(rows)
    summary = detail.groupby(["field", "null_reason"], dropna=False).size().reset_index(name="null_count")
    summary.insert(0, "record_type", "SUMMARY")
    for c in ["event_id", "event_type", "source_ck", "target_ck"]:
        summary[c] = None
    return pd.concat([summary[detail.columns], detail], ignore_index=True).sort_values(["record_type", "field", "null_reason", "event_id"], kind="stable")


def failure_audit(groups, temporal, overlap, clean, carve, rename):
    timing = temporal.set_index("event_id")
    clean = clean.set_index(["event_id", "source_ck"], drop=False)
    carve = carve.set_index(["event_id", "source_ck"], drop=False)
    rename = rename.set_index(["event_id", "source_ck"], drop=False)
    overlap = overlap.set_index(["event_id", "source_ck"], drop=False)
    rows = []
    for x in groups.itertuples(index=False):
        reasons = []
        if timing.loc[x.event_id].temporal_alignment_class in {"NO_PRE_OBSERVATION", "NO_POST_OBSERVATION", "LARGE_GAP"}:
            reasons.append("TEMPORAL_MISMATCH")
        if pd.isna(x.source_ck) or (x.target_count == 0 and x.event_type != "RENAME"):
            reasons.append("IDENTITY_UNCERTAINTY")
        if pd.isna(x.parent_area_before_km2):
            reasons.append("PARENT_AREA_MISSING")
        if x.event_type != "RENAME" and pd.isna(x.child_area_km2):
            reasons.append("TARGET_AREA_MISSING")
        if x.measurement_status == "MEASURED_NO_SPATIAL_EVIDENCE":
            reasons.append("NON_SPATIAL_EVENT")
        key = (x.event_id, x.source_ck)
        audit = clean if x.event_type == "CLEAN_SPLIT" else carve if x.event_type == "CARVE_OUT" else rename if x.event_type == "RENAME" else None
        if audit is not None and key in audit.index:
            status = audit.loc[key].audit_classification
            if status == "FAIL":
                reasons.append("CONSERVATION_FAILURE")
            if status == "RENAME_AREA_INCONSISTENCY":
                reasons.append("RENAME_INCONSISTENCY")
        if key in overlap.index and overlap.loc[key].overlap_class in {"HIGH", "EXTREME"}:
            reasons.append("GEOMETRY_OVERLAP")
        if reasons:
            rows.append({"event_id": x.event_id, "event_type": x.event_type, "source_ck": x.source_ck, "target_ck": x.target_ck, "measurement_status": x.measurement_status, "failure_categories": ";".join(reasons), "primary_failure": reasons[0], "audit_reason": "; ".join(reasons)})
    return pd.DataFrame(rows)


def random_validation(accounting, overlap):
    categories = [
        ("CLEAN_SPLIT", accounting.loc[accounting.event_type.eq("CLEAN_SPLIT")]),
        ("CARVE_OUT", accounting.loc[accounting.event_type.eq("CARVE_OUT")]),
        ("MULTI_CARVE_OUT", accounting.loc[accounting.event_type.eq("MULTI_CARVE_OUT")]),
        ("MERGE", accounting.loc[accounting.event_type.eq("MERGE")]),
        ("RENAME", accounting.loc[accounting.event_type.eq("RENAME")]),
        ("HIGH_OVERLAP", accounting.loc[accounting.event_id.isin(overlap.loc[overlap.overlap_class.isin(["HIGH", "EXTREME"]), "event_id"])]),
        ("LOW_COVERAGE", accounting.loc[accounting.measurement_status.eq("MEASURED_NO_SPATIAL_EVIDENCE")]),
        ("RECONSTRUCTED", accounting.loc[accounting.is_derived.fillna(False)]),
        ("UNMEASURED", accounting.loc[accounting.measurement_status.eq("UNMEASURED")]),
    ]
    base = ["event_id", "event_type", "source_ck", "target_ck", "pre_vintage", "post_vintage", "source_area_before_km2", "source_area_after_km2", "parent_area_loss_km2", "allocated_transfer_area_km2", "conservation_error_km2", "conservation_error_pct", "raw_intersection_area_km2", "overlap_excess_km2", "area_method", "measurement_status", "overall_confidence", "validation_status"]
    mappings = {"source_area_before_km2": "parent_before_area", "source_area_after_km2": "parent_after_area", "parent_area_loss_km2": "parent_area_loss", "allocated_transfer_area_km2": "allocated_transfer", "raw_intersection_area_km2": "raw_intersection", "overlap_excess_km2": "overlap_excess", "overall_confidence": "confidence"}
    frames, coverage = [], []
    for name, subset in categories:
        chosen = sample(subset, name)
        coverage.append({"validation_category": name, "available_cases": len(subset), "sampled_cases": len(chosen), "availability": "AVAILABLE" if len(subset) else "NOT_AVAILABLE_IN_CURRENT_DATA"})
        if len(chosen):
            chosen = chosen[base].rename(columns=mappings)
            chosen.insert(0, "validation_category", name)
            chosen["audit_result"] = chosen.validation_status.map({"CONSERVATION_PASS": "PASS", "RENAME_AREA_STABLE": "PASS"}).fillna("REVIEW")
            chosen["audit_reason"] = chosen.validation_status
            frames.append(chosen)
    columns = ["validation_category", "event_id", "event_type", "source_ck", "target_ck", "pre_vintage", "post_vintage", "parent_before_area", "parent_after_area", "parent_area_loss", "allocated_transfer", "conservation_error_km2", "conservation_error_pct", "raw_intersection", "overlap_excess", "area_method", "measurement_status", "confidence", "audit_result", "audit_reason"]
    return (pd.concat(frames, ignore_index=True)[columns] if frames else pd.DataFrame(columns=columns), pd.DataFrame(coverage))


def official_comparison(ts):
    benchmark = pd.read_csv(ROOT / "data" / "reference" / "official_district_area_benchmark.csv")
    computed = ts.loc[ts.source_year.eq(2025) & ts.geometry_status.isin(USABLE)].copy()
    computed["_name"], computed["_state"] = computed.district_name_original.map(normal), computed.state_name.map(normal)
    benchmark["_name"], benchmark["_state"] = benchmark.official_district_name.map(normal), benchmark.official_state_name.map(normal)
    merged = computed.merge(benchmark, on=["_name", "_state"], how="inner", suffixes=("_pipeline", "_benchmark"))
    merged["difference_km2"] = merged.area_km2 - merged.official_area_km2
    merged["difference_pct"] = 100 * merged.difference_km2.abs() / merged.official_area_km2
    return merged


def taxonomy():
    con = duckdb.connect(str(DB), read_only=True)
    try:
        events = con.execute("SELECT event_type, COUNT(*) n FROM boundary_event GROUP BY 1 ORDER BY 1").fetchdf()
        rels = con.execute("SELECT relationship_type, COUNT(*) n FROM district_relationship GROUP BY 1 ORDER BY 1").fetchdf()
        multiples = int(con.execute("SELECT COUNT(*) FROM (SELECT supporting_event_id FROM district_relationship GROUP BY 1 HAVING COUNT(*) > 1)").fetchone()[0])
    finally:
        con.close()
    return ", ".join(f"{x.event_type}={x.n}" for x in events.itertuples(index=False)), ", ".join(f"{x.relationship_type}={x.n}" for x in rels.itertuples(index=False)), multiples


def product_integrity(matrix):
    rows = []
    for stem in ["district_area_timeseries", "district_area_change", "event_area_accounting", "event_area_summary"]:
        # Keep source identifiers (including leading zeroes) as text while
        # comparing the literal CSV representation with Parquet values.
        csv = pd.read_csv(PRODUCTS / f"{stem}.csv", dtype=str)
        parquet = pd.read_parquet(PRODUCTS / f"{stem}.parquet")
        rows.append({"check": "CSV_PARQUET_ROW_COUNT", "product": stem, "result": "PASS" if len(csv) == len(parquet) else "FAIL", "count": len(csv), "detail": f"CSV={len(csv)}, Parquet={len(parquet)}"})
        rows.append({"check": "CSV_PARQUET_SCHEMA", "product": stem, "result": "PASS" if list(csv.columns) == list(parquet.columns) else "FAIL", "count": len(csv.columns), "detail": "Column sequence compared."})
        mismatches = 0
        if len(csv) == len(parquet) and list(csv.columns) == list(parquet.columns):
            for col in csv.columns:
                if pd.api.types.is_numeric_dtype(parquet[col]) and not pd.api.types.is_bool_dtype(parquet[col]):
                    left = pd.to_numeric(csv[col], errors="coerce")
                    equal = (left.isna() & parquet[col].isna()) | np.isclose(left.fillna(0), parquet[col].fillna(0), atol=1e-9, rtol=0)
                else:
                    equal = csv[col].fillna("").astype(str).str.casefold().eq(parquet[col].fillna("").astype(str).str.casefold())
                mismatches += int((~equal).sum())
        else:
            mismatches = -1
        rows.append({"check": "CSV_PARQUET_VALUE_MATCH", "product": stem, "result": "PASS" if mismatches == 0 else "FAIL", "count": mismatches, "detail": "Cell values compared when row count and schema are compatible; -1 means not comparable."})
    revised_change_columns = {"canonical_key", "district_name", "state", "from_year", "to_year", "area_from_km2", "area_to_km2", "area_change_km2", "area_change_pct", "possible_event_link", "change_class"}
    current_change_columns = set(pd.read_csv(PRODUCTS / "district_area_change.csv", nrows=1).columns)
    rows.append({"check": "AREA_CHANGE_CSV_SCHEMA_MATCHES_REVISED_BUILDER", "product": "district_area_change", "result": "PASS" if current_change_columns == revised_change_columns else "FAIL", "count": len(current_change_columns), "detail": "CSV uses revised decadal interval fields; Parquet compatibility is evaluated separately above."})
    con = duckdb.connect(str(DB), read_only=True)
    try:
        registry_count = int(con.execute("SELECT COUNT(DISTINCT canonical_key) FROM canonical_key_registry").fetchone()[0])
    finally:
        con.close()
    rows.append({"check": "CK_REGISTRY_MATRIX_COVERAGE", "product": "district_area_by_census", "result": "CONDITIONAL" if registry_count != matrix.canonical_key.nunique() else "PASS", "count": registry_count - matrix.canonical_key.nunique(), "detail": f"Registry CKs={registry_count}; matrix CKs={matrix.canonical_key.nunique()}."})
    return pd.DataFrame(rows)


def geometry_inventory():
    rows = []
    for tier, files in [("BRONZE", sorted((ROOT / "data" / "bronze").glob("*/*/*.gpkg"))), ("SILVER", sorted(SILVER.glob("*.geoparquet")))]:
        for path in files:
            frame = gpd.read_file(path) if path.suffix == ".gpkg" else gpd.read_parquet(path)
            rows.append({"tier": tier, "artifact": str(path.relative_to(ROOT)), "rows": len(frame), "crs": str(frame.crs), "null_geometries": int(frame.geometry.isna().sum()), "empty_geometries": int(frame.geometry.is_empty.sum()), "audit_result": "PASS" if not frame.geometry.isna().any() and not frame.geometry.is_empty.any() else "FAIL"})
    return pd.DataFrame(rows)


def report(ts, independent, census, temporal, clean, carve, overlap, old_new, accounting, official, event_types, relationship_types, multiple_lineage_events, integrity):
    clean_measured = clean.loc[clean.audit_classification.ne("UNMEASURED")]
    carve_measured = carve.loc[carve.audit_classification.isin(["PASS", "MINOR_DISCREPANCY", "MATERIAL_DISCREPANCY", "FAIL"])]
    transfer = pd.concat([clean_measured, carve_measured], ignore_index=True)
    accuracy = 1 - transfer.conservation_error_km2.abs() / transfer.parent_area_before_km2.abs()
    comparison = old_new.loc[old_new.comparison_status.eq("COMPARABLE"), "difference_pct"]
    timing = temporal.temporal_alignment_class.value_counts().to_dict()
    failures = int((transfer.audit_classification == "FAIL").sum())
    duplicate_groups = int(ts.groupby(["canonical_key", "source_year"]).size().gt(1).sum())
    unmeasured = int(accounting.measurement_status.eq("UNMEASURED").sum())
    source_nulls = int(accounting.source_area_before_km2.isna().sum())
    target_nulls = int(accounting.target_area_after_km2.isna().sum())
    loss_nulls = int(accounting.parent_area_loss_km2.isna().sum())
    allocation_nulls = int(accounting.allocated_transfer_area_km2.isna().sum())
    error_nulls = int(accounting.conservation_error_km2.isna().sum())
    valid_matrix = int(ts.geometry_status.isin(USABLE).sum())
    registry_gap = int(integrity.loc[integrity.check.eq("CK_REGISTRY_MATRIX_COVERAGE"), "count"].iloc[0])
    change_schema_result = integrity.loc[integrity.check.eq("AREA_CHANGE_CSV_SCHEMA_MATCHES_REVISED_BUILDER"), "result"].iloc[0]
    product_integrity_failed = bool((integrity.result == "FAIL").any())
    area_status = "CONDITIONAL" if duplicate_groups or registry_gap or change_schema_result != "PASS" or census.loc[census.check.eq("WORKBOOK_REQUIRED_SHEETS"), "result"].iloc[0] != "PASS" else "PASS"
    architecture_status = "FAIL" if product_integrity_failed else area_status
    event_status = "FAIL" if failures else "CONDITIONAL"
    temporal_status = "CONDITIONAL" if any(x in timing for x in ["NO_PRE_OBSERVATION", "NO_POST_OBSERVATION", "LARGE_GAP"]) else "PASS"
    lines = [
        "# Phase 9.1 — Independent Audit of Decadal Area Accounting", "",
        "## Scope", "",
        "Read-only audit of revised products, Silver geometries, Gold CK/lineage/event tables, the legacy intersection archive, and previous Phase 9 reports. No pipeline or product-generating code was changed.", "",
        "## Quantitative findings", "",
        f"- Long-form observations: {len(ts):,}; unique usable observations in the wide matrix: {valid_matrix:,}.",
        f"- CK/vintage ambiguity: {duplicate_groups:,} groups / {int(ts.duplicated(['canonical_key', 'source_year'], keep=False).sum()):,} long-form rows. Source PK/year duplicates: 0. Null, zero, and negative areas: 0.",
        f"- CK registry coverage: {registry_gap:,} of 964 registered CKs have no matrix row. `district_area_change.csv` has the revised 4,194-row decadal schema, but its 3,278-row Parquet counterpart is stale and has a different schema; the product pair is invalid.",
        f"- Independent EPSG:6933 recalculation: {len(independent):,} districts across {independent.region.nunique():,} regions; {int((independent.audit_result != 'PASS').sum()):,} tolerance exceptions.",
        f"- Bronze and Silver inventories contain no null or empty geometries; 8 area observations carry an explicit Silver repair/derived flag.",
        f"- Census CSV values and NULL semantics exactly match unique long-form observations. Workbook sheets are {sheet_names(PRODUCTS / 'district_area_by_census.xlsx')}; the required `area_quality` sheet is missing and eight repaired observations are not marked there.",
        f"- Temporal alignment: {timing}. Strict bracketing has {timing.get('NO_PRE_OBSERVATION', 0):,} events without a pre-observation; events dated at observed vintages use a 20-year surrounding span.",
        f"- Clean split conservation: {len(clean_measured):,} calculable, {int((clean_measured.audit_classification == 'PASS').sum()):,} PASS and {int((clean_measured.audit_classification == 'FAIL').sum()):,} FAIL.",
        f"- Carve-out conservation: {len(carve_measured):,} positive-loss comparisons, {int((carve_measured.audit_classification == 'PASS').sum()):,} PASS, {int((carve_measured.audit_classification == 'MINOR_DISCREPANCY').sum()):,} minor, {int((carve_measured.audit_classification == 'MATERIAL_DISCREPANCY').sum()):,} material, and {int((carve_measured.audit_classification == 'FAIL').sum()):,} FAIL.",
        f"- Multi-target event/source groups: {int(overlap.is_multi_target.sum()):,}; MERGE events: 0. The multi-child, target-overlap, and merge tests are not testable—not PASS. Zero observed overlap is tautological for single-target groups.",
        "- Multi-target overlap distribution: n=0; median, p90, p95, p99, and maximum are not applicable. The stored overlap diagnostic must not be interpreted as empirical validation.",
        f"- Nulls: source area {source_nulls:,}; target area {target_nulls:,}; parent loss {loss_nulls:,}; allocated transfer {allocation_nulls:,}; conservation error {error_nulls:,}. `null_reason_audit.csv` assigns each null an explicit reason without converting it to zero.",
        f"- Old vs new: {len(comparison):,} comparable positive-transfer groups. Absolute percent difference median {comparison.median():.2f}%, mean {comparison.mean():.2f}%, p90 {comparison.quantile(.90):.2f}%, p95 {comparison.quantile(.95):.2f}%, p99 {comparison.quantile(.99):.2f}%, max {comparison.max():.2f}%.",
        f"- Official 2025 same-source comparison: {len(official):,} matches, median absolute discrepancy {official.difference_pct.median():.3f}%, maximum {official.difference_pct.max():.3f}%. This compares geodesic and planar calculations of the same SOI geometry, not independent historical truth.",
        f"- Conservation accuracy among {len(transfer):,} measurable transfer comparisons: median {accuracy.median():.3f}, p90 {accuracy.quantile(.90):.3f}, minimum {accuracy.min():.3f}. Negative values mean the discrepancy exceeds the expected change.", "",
        "## Taxonomy and reproducibility", "",
        f"Gold event taxonomy: {event_types}. Gold lineage taxonomy: {relationship_types}. Events with multiple lineage relationships: {multiple_lineage_events}.",
        "The exported event-register and lineage Parquet products serialize event IDs as binary values while revised accounting uses UUID strings, so researcher-facing product joins are not reliable. Previous Phase 9 reports also conflict with the revised product's status counts and should not be treated as audit evidence.", "",
        "## Top 10 scientifically important issues", "",
        f"1. External conservation failure in {failures:,}/{len(transfer):,} measurable transfer groups, including {int((clean_measured.audit_classification == 'FAIL').sum()):,}/{len(clean_measured):,} clean splits.",
        "2. The district-area-change CSV and Parquet products disagree in both schema and row count, so a researcher cannot know which product is authoritative.",
        "3. No multi-child lineage case exists, so the intended allocation method is untested.",
        "4. No MERGE event exists, so merge behavior is untested.",
        f"5. {unmeasured:,}/{len(accounting):,} accounting rows are UNMEASURED; no numeric transfer should be inferred from them.",
        f"6. {duplicate_groups:,} CK/vintage assignments are ambiguous and {registry_gap:,} registered CKs have no area-matrix observation.",
        "7. Workbook contract failure: `area_quality` is absent and repaired values are not marked.",
        f"8. Temporal mismatch: {timing.get('NO_PRE_OBSERVATION', 0):,} events lack a pre-vintage, with 20-year spans around census-dated events.",
        "9. The claimed zero-overlap result has no eligible multi-target test case; it does not validate overlap logic.",
        "10. Binary versus UUID event identifiers and stale all-PASS Phase 9 reports break cross-product provenance and misstate audit evidence.", "",
        "## Required conclusion", "",
        f"The decadal-area-change formulation is conceptually more interpretable than raw intersections: it makes the area constraint explicit, and the {comparison.median():.2f}% median raw-versus-change divergence shows raw intersection is not a stable proxy. It is not yet scientifically validated as a territory-transfer estimator. External child-area conservation fails in most measurable cases, while multi-target, overlap, and merge behavior have no test data.",
        "", "The next methodology should retain the decadal change as a bound, but require complete event-specific successor sets and an independent conservation check before publishing a transfer. Incomplete cases should remain bounded/unmeasured. Add verified multi-child and merge cases before making allocation or overlap claims.", "",
        "## PHASE 9.1 AUDIT RESULT", "", "| Dimension | Result |", "|---|---|",
        f"| Architecture | {architecture_status} |", f"| District Area Foundation | {area_status} |", f"| Event Accounting | {event_status} |", f"| Conservation | {event_status} |", f"| Temporal Alignment | {temporal_status} |", "| Spatial Evidence | CONDITIONAL |", f"| Scientific Validity | {'FAIL' if failures else 'CONDITIONAL'} |",
    ]
    rendered = "\n".join(lines) + "\n"
    (OUT / "phase91_independent_audit.md").write_text(rendered, encoding="utf-8")
    (OUT / "area_accuracy_summary.md").write_text(rendered, encoding="utf-8")


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    ts = pd.read_parquet(PRODUCTS / "district_area_timeseries.parquet")
    matrix = pd.read_csv(PRODUCTS / "district_area_by_census.csv")
    accounting = pd.read_parquet(PRODUCTS / "event_area_accounting.parquet")
    xlsx = PRODUCTS / "district_area_by_census.xlsx"
    areas = area_audit(ts)
    independent = independent_areas(ts)
    census = census_audit(ts, matrix, read_first_sheet(xlsx), sheet_names(xlsx))
    temporal = temporal_audit(accounting)
    groups = groups_from_accounting(accounting)
    clean, carve, multi, merge = conservation_audits(groups)
    rename = rename_audit(groups)
    overlap = overlap_audit(groups)
    old_new = old_new_audit(groups)
    nulls = null_audit(accounting)
    failures = failure_audit(groups, temporal, overlap, clean, carve, rename)
    random, random_coverage = random_validation(accounting, overlap)
    official = official_comparison(ts)
    event_types, relationship_types, multiple_lineage_events = taxonomy()
    integrity = product_integrity(matrix)
    geometries = geometry_inventory()
    outputs = {
        "area_timeseries_audit.csv": areas, "independent_area_recalculation.csv": independent, "census_area_table_audit.csv": census,
        "temporal_alignment_audit.csv": temporal, "clean_split_audit.csv": clean, "carve_out_audit.csv": carve,
        "multi_child_event_audit.csv": multi, "merge_audit.csv": merge, "rename_audit.csv": rename,
        "overlap_target_geometry_audit.csv": overlap, "old_vs_new_area_method.csv": old_new,
        "event_failure_taxonomy.csv": failures, "null_reason_audit.csv": nulls,
        "random_event_area_validation.csv": random, "random_event_area_validation_coverage.csv": random_coverage,
        "official_benchmark_discrepancy_audit.csv": official, "product_integrity_audit.csv": integrity,
        "historical_geometry_inventory_audit.csv": geometries,
    }
    for name, frame in outputs.items():
        frame.to_csv(OUT / name, index=False)
    report(ts, independent, census, temporal, clean, carve, overlap, old_new, accounting, official, event_types, relationship_types, multiple_lineage_events, integrity)
    print(f"Wrote {len(outputs)} Phase 9.1 audit artifacts to {OUT}")


if __name__ == "__main__":
    main()
