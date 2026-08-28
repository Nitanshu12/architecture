import fs from "node:fs/promises";
import { SpreadsheetFile, Workbook } from "@oai/artifact-tool";

const projectRoot = "/Users/satyamkumar/Desktop/DistrictEvolution_Final";
const inputPath = `${projectRoot}/data/products/district_area_by_census.csv`;
const outputPath = `${projectRoot}/data/products/district_area_by_census.xlsx`;
const previewDir = "/private/tmp/phase9_area_matrix_preview";

const csvText = await fs.readFile(inputPath, "utf8");
const workbook = await Workbook.fromCSV(csvText, { sheetName: "District Area by Census" });
const matrix = workbook.worksheets.getItem("District Area by Census");
const metadata = workbook.worksheets.add("Metadata & Definitions");

matrix.showGridLines = false;
matrix.freezePanes.freezeRows(1);
matrix.freezePanes.freezeColumns(3);
const matrixRange = matrix.getUsedRange();
const matrixRows = matrixRange.values.length;
const matrixColumns = matrixRange.values[0].length;
const lastColumn = "L";
matrix.getRange(`A1:${lastColumn}1`).format = {
  fill: "#16324F",
  font: { bold: true, color: "#FFFFFF" },
  horizontalAlignment: "center",
  verticalAlignment: "center",
  wrapText: true,
  borders: { preset: "outside", style: "thin", color: "#0F2538" },
};
matrix.getRange(`A1:${lastColumn}1`).format.rowHeight = 31;
matrix.getRange(`A2:C${matrixRows}`).format = { verticalAlignment: "center" };
matrix.getRange(`D2:${lastColumn}${matrixRows}`).format = {
  numberFormat: "#,##0.00",
  horizontalAlignment: "right",
};
matrix.getRange(`A1:A${matrixRows}`).format.columnWidth = 17;
matrix.getRange(`B1:B${matrixRows}`).format.columnWidth = 27;
matrix.getRange(`C1:C${matrixRows}`).format.columnWidth = 22;
matrix.getRange(`D1:${lastColumn}${matrixRows}`).format.columnWidth = 16;
matrix.tables.add(`A1:${lastColumn}${matrixRows}`, true, "DistrictAreaByCensus");

metadata.showGridLines = false;
metadata.getRange("A1:D1").merge();
metadata.getRange("A1").values = [["District Area by Census — Metadata & Definitions"]];
metadata.getRange("A1:D1").format = {
  fill: "#16324F",
  font: { bold: true, color: "#FFFFFF" },
  horizontalAlignment: "left",
  verticalAlignment: "center",
};
metadata.getRange("A1:D1").format.rowHeight = 30;
metadata.getRange("A3:B9").values = [
  ["Field", "Definition"],
  ["Dataset", "District area observations arranged for human inspection by census/source vintage."],
  ["Geographic unit", "Canonical district identity (canonical_key), with display name and state retained for readability."],
  ["Vintages", "1951, 1961, 1971, 1981, 1991, 2001, 2011, 2021 and 2025."],
  ["Area unit", "Square kilometres (km²). Degree-squared areas are not used."],
  ["Area method", "WGS84 ellipsoidal geodesic area calculated with pyproj.Geod.geometry_area_perimeter."],
  ["Source layer", "data/products/district_area_timeseries.csv is the long-form, observation-level source."],
];
metadata.getRange("A11:B16").values = [
  ["Value / status", "Meaning"],
  ["Blank (NULL)", "No unique valid observed district geometry is available for that canonical district and vintage. It does not mean zero area."],
  ["0", "Would mean an actual measured zero area. This is intentionally not substituted for missing geometry and should be exceptional."],
  ["Observed", "A district geometry observed in the indicated census/source vintage."],
  ["Derived repair flag", "A documented Silver geometry-repair artifact. It remains separately flagged in the long-form time series."],
  ["Temporal caution", "Vintages are observations, not exact administrative-event dates. Event calculations use surrounding pre/post evidence."],
];
metadata.getRange("A3:B3").format = {
  fill: "#2E6F95",
  font: { bold: true, color: "#FFFFFF" },
  horizontalAlignment: "center",
};
metadata.getRange("A11:B11").format = {
  fill: "#2E6F95",
  font: { bold: true, color: "#FFFFFF" },
  horizontalAlignment: "center",
};
metadata.getRange("A4:A9").format = { fill: "#E8F1F5", font: { bold: true }, verticalAlignment: "top" };
metadata.getRange("A12:A16").format = { fill: "#E8F1F5", font: { bold: true }, verticalAlignment: "top" };
metadata.getRange("A3:B9").format.borders = { preset: "outside", style: "thin", color: "#AAB7C4" };
metadata.getRange("A11:B16").format.borders = { preset: "outside", style: "thin", color: "#AAB7C4" };
metadata.getRange("A3:B16").format.wrapText = true;
metadata.getRange("A1:A16").format.columnWidth = 24;
metadata.getRange("B1:B16").format.columnWidth = 92;
metadata.getRange("A4:B9").format.rowHeight = 31;
metadata.getRange("A12:B16").format.rowHeight = 42;
metadata.freezePanes.freezeRows(3);

const matrixCheck = await workbook.inspect({
  kind: "table",
  range: "District Area by Census!A1:L8",
  include: "values,formulas",
  tableMaxRows: 8,
  tableMaxCols: 12,
});
const metadataCheck = await workbook.inspect({
  kind: "table",
  range: "Metadata & Definitions!A1:B16",
  include: "values,formulas",
  tableMaxRows: 16,
  tableMaxCols: 2,
});
const formulaErrors = await workbook.inspect({
  kind: "match",
  searchTerm: "#REF!|#DIV/0!|#VALUE!|#NAME\\?|#N/A",
  options: { useRegex: true, maxResults: 100 },
  summary: "formula error scan",
});
console.log(matrixCheck.ndjson);
console.log(metadataCheck.ndjson);
console.log(formulaErrors.ndjson);

await fs.mkdir(previewDir, { recursive: true });
const matrixPreview = await workbook.render({
  sheetName: "District Area by Census",
  range: "A1:L18",
  scale: 1.5,
  format: "png",
});
await fs.writeFile(`${previewDir}/matrix.png`, new Uint8Array(await matrixPreview.arrayBuffer()));
const metadataPreview = await workbook.render({
  sheetName: "Metadata & Definitions",
  range: "A1:B16",
  scale: 1.5,
  format: "png",
});
await fs.writeFile(`${previewDir}/metadata.png`, new Uint8Array(await metadataPreview.arrayBuffer()));

const output = await SpreadsheetFile.exportXlsx(workbook);
await output.save(outputPath);
console.log(`Saved ${outputPath}`);
