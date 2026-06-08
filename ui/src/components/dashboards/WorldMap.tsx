/**
 * WorldMap — country choropleth widget renderer.
 *
 * Reads `(region, value)` rows from a dataset; colors each country by
 * the metric. Fill is the theme's `--primary` accent (same blue as the
 * bar/pie charts) at an opacity ramped by value (dim = low, solid =
 * high); countries with no data take the muted "land" token. Compositing
 * the accent over the card means it reads correctly in both light and
 * dark themes and never bottoms out at white. Region codes can be ISO
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

  // Value → fill opacity. A floor of 0.25 keeps the lowest bucket visibly
  // tinted (not indistinguishable from no-data land); identical min/max
  // collapse to a single mid opacity. Compositing the accent at <1 alpha
  // over the card is what gives the dim→solid ramp in either theme.
  const opacityFor = useMemo(() => {
    if (min === max) return () => 0.7;
    const span = max - min;
    return (v: number) => 0.25 + 0.75 * Math.min(1, Math.max(0, (v - min) / span));
  }, [min, max]);

  // Resolve theme tokens to literal colours: recharts/SVG `fill` won't
  // resolve a CSS `var()`, so we read the computed `--primary` (accent),
  // `--muted` (no-data land) and `--border` (country outlines) once, the
  // same way the bar/pie charts resolve `--primary`.
  const theme = useMemo(() => {
    const fallback = { primary: "#2563eb", muted: "#1e293b", border: "#334155" };
    if (typeof document === "undefined") return fallback;
    const cs = getComputedStyle(document.documentElement);
    const get = (name: string, f: string) => cs.getPropertyValue(name).trim() || f;
    return {
      primary: get("--primary", fallback.primary),
      muted: get("--muted", fallback.muted),
      border: get("--border", fallback.border),
    };
  }, []);

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
              const has = v !== undefined;
              const tooltip = has
                ? `${geo.properties?.name ?? id}: ${
                    tooltipsById.get(id) || formatValue(v)
                  }`
                : `${geo.properties?.name ?? id}: no data`;
              return (
                <Geography
                  key={geo.rsmKey}
                  geography={geo}
                  fill={has ? theme.primary : theme.muted}
                  fillOpacity={has ? opacityFor(v) : 1}
                  stroke={theme.border}
                  strokeWidth={0.3}
                  style={{
                    default: { outline: "none" },
                    hover: { outline: "none", fill: theme.primary, fillOpacity: 1 },
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
