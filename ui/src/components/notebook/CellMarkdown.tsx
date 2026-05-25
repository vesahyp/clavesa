/**
 * CellMarkdown — markdown-cell renderer via react-markdown + remark-gfm.
 *
 * Raw HTML is NOT rendered — react-markdown's default skipHtml behavior
 * (we don't pass rehype-raw). GFM extensions enable tables, strikethrough,
 * autolinks, task lists — the conventions users get in GitHub renders of
 * the same .ipynb file.
 *
 * Tailwind's typography plugin isn't wired into this project, so we style
 * the immediate children of the wrapper directly. Keeps the surface area
 * tight to what nbformat markdown cells actually contain in practice
 * (headings, paragraphs, code, lists, tables).
 */

import ReactMarkdown from "react-markdown";
import remarkGfm from "remark-gfm";

export function CellMarkdown({ source }: { source: string }) {
  return (
    <div className="markdown-cell rounded border bg-background p-4 text-sm leading-relaxed text-foreground [&>:first-child]:mt-0 [&>:last-child]:mb-0 [&_a]:text-primary [&_a]:underline [&_code]:rounded [&_code]:bg-muted [&_code]:px-1 [&_code]:py-0.5 [&_code]:font-mono [&_code]:text-xs [&_h1]:mb-3 [&_h1]:mt-4 [&_h1]:text-xl [&_h1]:font-semibold [&_h2]:mb-2 [&_h2]:mt-3 [&_h2]:text-lg [&_h2]:font-semibold [&_h3]:mb-2 [&_h3]:mt-3 [&_h3]:font-semibold [&_li]:my-0.5 [&_ol]:my-2 [&_ol]:list-decimal [&_ol]:pl-6 [&_p]:my-2 [&_pre]:my-2 [&_pre]:overflow-auto [&_pre]:rounded [&_pre]:bg-muted [&_pre]:p-3 [&_pre_code]:bg-transparent [&_pre_code]:p-0 [&_table]:my-2 [&_table]:border-collapse [&_td]:border [&_td]:px-2 [&_td]:py-1 [&_th]:border [&_th]:bg-muted [&_th]:px-2 [&_th]:py-1 [&_th]:text-left [&_ul]:my-2 [&_ul]:list-disc [&_ul]:pl-6">
      <ReactMarkdown remarkPlugins={[remarkGfm]}>{source}</ReactMarkdown>
    </div>
  );
}
