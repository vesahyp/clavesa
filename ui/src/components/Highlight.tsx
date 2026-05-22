/**
 * Highlight — wraps the substrings of `text` that match `query` in a
 * <mark>, leaving the rest untouched. Used by the list pages (Catalog,
 * Pipelines, Sources) to show *why* a row survived the type-to-filter
 * search. Matching is case-insensitive and marks every occurrence.
 */

import type { ReactNode } from "react";

export function Highlight({
  text,
  query,
}: {
  text: string;
  query: string;
}): ReactNode {
  const q = query.trim();
  if (!q) return text;

  const haystack = text.toLowerCase();
  const needle = q.toLowerCase();
  const parts: ReactNode[] = [];
  let cursor = 0;

  for (;;) {
    const hit = haystack.indexOf(needle, cursor);
    if (hit < 0) {
      parts.push(text.slice(cursor));
      break;
    }
    if (hit > cursor) parts.push(text.slice(cursor, hit));
    parts.push(
      <mark
        key={hit}
        className="rounded-[2px] bg-amber-400/15 text-amber-700 dark:text-amber-300"
      >
        {text.slice(hit, hit + needle.length)}
      </mark>,
    );
    cursor = hit + needle.length;
  }

  return <>{parts}</>;
}
