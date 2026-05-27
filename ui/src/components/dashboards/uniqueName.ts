/**
 * uniqueName — return base, base2, base3, … so a freshly-allocated
 * identifier never collides with an existing one. Used for dataset
 * names, widget ids, control names — anywhere the editor materialises
 * a value that must round-trip through the saved spec without a 400.
 */
export function uniqueName(base: string, taken: string[]): string {
  if (!taken.includes(base)) return base;
  for (let n = 2; ; n++) {
    const candidate = `${base}${n}`;
    if (!taken.includes(candidate)) return candidate;
  }
}
