# web-tracker

A small, dependency-free, **cookieless** web-analytics tracker. Drop it on your
site, tag the elements you care about, and it writes everything to a 1×1 pixel
that your CDN already logs — no backend, no cookies, no third-party tag. Pair it
with the [cloudfront-web-analytics cookbook recipe](../docs/cookbook/cloudfront-web-analytics.md)
to turn those logs into sessions, funnels, and click-through rates with clavesa.

This is the **exact tracker that runs on clavesa.dev** — not a sample. It's
tested end to end (the site's own analytics dashboard is built from its output),
so what you drop in is what we run.

## Use it

1. **Host `tracker.js` yourself** (copy it into your site's static assets — don't
   hot-link ours; the point is that nothing loads from a third party).
2. **Add one script tag:**

   ```html
   <script src="/tracker.js" defer></script>
   ```

3. **Tag what you want to measure** with `data-track`:

   ```html
   <a href="/signup" data-track="signup">Start free</a>
   <a href="/docs"   data-track="docs">Read the docs</a>
   <button data-track="see-pricing">See pricing</button>
   ```

That's the whole integration. From there, point clavesa at your CloudFront logs
(the cookbook recipe) to get the tables and a dashboard.

## What it captures

Every event is a GET/`sendBeacon` to `/t.gif?…` with the data in the query
string. The only state it keeps is a random id in `localStorage` (a 30-minute
sliding session id in `sessionStorage`) — no cookies, no fingerprinting.

- `session_start` — first hit of a 30-minute session, with referrer + viewport.
- `view` / `displayed` — a `[data-track]` element entered the viewport
  (`view`) and was seen for ≥3s (`displayed`). Once per session per element.
- `click` — a click on a `[data-track]` element. A click also back-fills
  `view` + `displayed`, so **clicks ⊆ displayed ⊆ view** and click-through rate
  is always ≤ 100%.
- `scroll` — 25/50/75/100% depth milestones, once each per session.
- `lcp` / `cls` — Largest Contentful Paint and Cumulative Layout Shift.
- `session_end` — max scroll depth + duration, flushed on page hide.
- `error` — uncaught JS errors and unhandled promise rejections.

Only elements you mark with `data-track` are tracked for interaction; unmarked
clicks fire nothing. The `data-track` name is the shared key across `view`,
`displayed`, and `click`, so per-element click-through rate and conversion
funnels fall out for free.

## Configure (optional)

Set `window.TRACKER_CONFIG` before the script loads:

```html
<script>
  window.TRACKER_CONFIG = {
    endpoint: "/t.gif",              // where beacons go (must be CDN-logged)
    sessionTimeout: 30 * 60 * 1000,  // sliding session window, ms
    debug: false                     // console.log every event
  };
</script>
<script src="/tracker.js" defer></script>
```

You also need a **1×1 object served at `/t.gif`** (any transparent GIF works —
the response body is irrelevant; the request just needs to be logged) and
**CloudFront standard access logging** turned on. The
[cookbook recipe](../docs/cookbook/cloudfront-web-analytics.md) walks both, plus
the clavesa pipeline that reads the logs.

## "No PII" is a goal, not a guarantee

The tracker sends no names, emails, or cookies, and the pipeline stores only a
2-letter country code rather than the IP. But CloudFront access logs retain
client IPs for their configured window, so treat this as *privacy-light*, not
*anonymous*. Set a log retention/TTL you're comfortable with.

MIT-licensed, same as clavesa.
