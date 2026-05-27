/**
 * WorldMap — country choropleth widget renderer.
 *
 * Reads `(region, value)` rows from a dataset; colors each country by
 * the metric using a sequential blues scale. Region codes can be ISO
 * 3166-1 alpha-2 (`US`, `DE`) or alpha-3 (`USA`, `DEU`) — the format
 * is auto-detected from the first non-empty row. Codes the standard
 * doesn't know (`XX`) are silently dropped with a console.debug so a
 * single bad row doesn't break the chart.
 *
 * Topology: `world-atlas` countries-50m TopoJSON, embedded in the
 * bundle (~150KB gz). No tile server, no Mapbox token — the map ships
 * with the binary. Higher resolution (10m) exists but oversamples at
 * dashboard sizes and doubles the bundle weight.
 */

import { useMemo } from "react";
import { scaleSequential } from "d3-scale";
import { interpolateBlues } from "d3-scale-chromatic";
import {
  ComposableMap,
  Geographies,
  Geography,
} from "react-simple-maps";
import iso3166 from "iso-3166-1";
import worldAtlas from "world-atlas/countries-50m.json";

import type { DashboardQueryResult } from "@/lib/queries";

interface WorldMapProps {
  data: DashboardQueryResult;
  regionField: string;
  valueField: string;
  tooltipField?: string;
}

export function WorldMap({
  data,
  regionField,
  valueField,
  tooltipField,
}: WorldMapProps) {
  // Build the (numeric M49 ID → value) map by translating each row's
  // region code through iso-3166-1. world-atlas's feature ids ARE M49
  // numerics, so the lookup is direct after translation. Unknown codes
  // get a debug log and skip; one bad row shouldn't blank the map.
  const { valuesById, tooltipsById, min, max } = useMemo(() => {
    const regionIdx = data.columns.findIndex((c) => c.name === regionField);
    const valueIdx = data.columns.findIndex((c) => c.name === valueField);
    const tooltipIdx =
      tooltipField && tooltipField !== ""
        ? data.columns.findIndex((c) => c.name === tooltipField)
        : -1;
    const valuesById = new Map<string, number>();
    const tooltipsById = new Map<string, string>();
    let min = Infinity;
    let max = -Infinity;
    if (regionIdx < 0 || valueIdx < 0) {
      return { valuesById, tooltipsById, min: 0, max: 0 };
    }
    for (const row of data.rows) {
      const code = (row[regionIdx] ?? "").trim();
      const raw = row[valueIdx];
      if (!code || raw === "" || raw == null) continue;
      const numId = toM49(code);
      if (!numId) {
        // eslint-disable-next-line no-console
        console.debug(`world_map: skipping unknown region code "${code}"`);
        continue;
      }
      const v = Number(raw);
      if (!Number.isFinite(v)) continue;
      valuesById.set(numId, v);
      if (tooltipIdx >= 0) {
        tooltipsById.set(numId, row[tooltipIdx] ?? "");
      }
      if (v < min) min = v;
      if (v > max) max = v;
    }
    if (!Number.isFinite(min)) min = 0;
    if (!Number.isFinite(max)) max = 0;
    return { valuesById, tooltipsById, min, max };
  }, [data, regionField, valueField, tooltipField]);

  // Sequential scale; identical min/max collapse to a single value so the
  // scale's range stays sane. d3-scale-chromatic interpolators return
  // colour strings; clamp protects against rounding-edge values.
  const color = useMemo(() => {
    if (min === max) {
      return () => interpolateBlues(0.6);
    }
    return scaleSequential(interpolateBlues).domain([min, max]).clamp(true);
  }, [min, max]);

  return (
    <div className="h-full w-full">
      <ComposableMap
        projectionConfig={{ scale: 140 }}
        style={{ width: "100%", height: "100%" }}
      >
        <Geographies geography={worldAtlas as object}>
          {({ geographies }) =>
            geographies.map((geo) => {
              const id = String(geo.id ?? "");
              const v = valuesById.get(id);
              const fill = v !== undefined ? color(v) : "#e5e7eb";
              const tooltip =
                v !== undefined
                  ? `${geo.properties?.name ?? id}: ${
                      tooltipsById.get(id) || formatValue(v)
                    }`
                  : `${geo.properties?.name ?? id}: no data`;
              return (
                <Geography
                  key={geo.rsmKey}
                  geography={geo}
                  fill={fill as string}
                  stroke="#9ca3af"
                  strokeWidth={0.3}
                  style={{
                    default: { outline: "none" },
                    hover: { outline: "none", fill: "#3b82f6" },
                    pressed: { outline: "none" },
                  }}
                >
                  <title>{tooltip}</title>
                </Geography>
              );
            })
          }
        </Geographies>
      </ComposableMap>
    </div>
  );
}

/**
 * toM49 — translate an ISO 3166-1 alpha-2 / alpha-3 code into the M49
 * numeric string the world-atlas TopoJSON uses as a feature id. Empty
 * or unknown codes return null so the caller can debug-log and skip.
 */
function toM49(code: string): string | null {
  const c = code.trim().toUpperCase();
  if (c.length === 2) {
    const rec = iso3166.whereAlpha2(c);
    return rec?.numeric ?? null;
  }
  if (c.length === 3) {
    const rec = iso3166.whereAlpha3(c);
    return rec?.numeric ?? null;
  }
  // Already a numeric M49 (some workloads emit them directly).
  if (/^\d{1,3}$/.test(c)) {
    return c.padStart(3, "0");
  }
  return null;
}

/**
 * formatValue — short numeric formatter for the title tooltip. Picks
 * compact notation for thousands+ so big numbers don't overflow a
 * small SVG hover; keeps small values precise.
 */
function formatValue(v: number): string {
  if (Math.abs(v) >= 1000) {
    return new Intl.NumberFormat(undefined, {
      notation: "compact",
      maximumFractionDigits: 1,
    }).format(v);
  }
  return new Intl.NumberFormat().format(v);
}
