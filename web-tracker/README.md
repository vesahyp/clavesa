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
  Answers "where did the visit begin", and only that.
- `pageview` — every page load. The path rides on every beacon, so this
  carries no fields of its own. Use it for "which pages were read";
  `session_start` is the landing page and the two are different questions.
- `view` / `displayed` — a `[data-track]` element entered the viewport
  (`view`) and was seen for ≥3s (`displayed`). Once per session per element.
- `click` — a click on a `[data-track]` element. A click also back-fills
  `view` + `displayed`, so **clicks ⊆ displayed ⊆ view** and click-through rate
  is always ≤ 100%. Set `clicks: "all"` to report every click with where it
  landed: see [Click maps](#click-maps).
- `scroll` — 25/50/75/100% depth milestones, once each per session.
- `lcp` / `cls` / `inp` — Largest Contentful Paint, Cumulative Layout Shift,
  and Interaction to Next Paint (the worst interaction of the visit, in ms).
  A visit that never interacted sends no `inp`.
- `session_end` — max scroll depth + duration, flushed on page hide. A page
  with nothing to scroll reports 100, not 0: it was read in full.
- `error` — uncaught JS errors and unhandled promise rejections.
- `auth` — the visit signed in. See [Signed-in visits](#signed-in-visits).
- `email_click` — the visit arrived from a campaign link. See
  [Campaign links](#campaign-links).

Only elements you mark with `data-track` are tracked for interaction; unmarked
clicks fire nothing. The `data-track` name is the shared key across `view`,
`displayed`, and `click`, so per-element click-through rate and conversion
funnels fall out for free.

## Configure (optional)

Set `window.TRACKER_CONFIG` before the script loads:

```html
<script>
  window.TRACKER_CONFIG = {
    enabled: true,                   // false = measure nothing, see below
    endpoint: "/t.gif",              // where beacons go (must be CDN-logged)
    sessionTimeout: 30 * 60 * 1000,  // sliding session window, ms
    sanitize: null,                  // function(value) -> value, see below
    clicks: "tagged",                // or "all", see Click maps below
    clickData: null,                 // function(element) -> object, see below
    debug: false                     // console.log every event
  };
</script>
<script src="/tracker.js" defer></script>
```

### `enabled`

`enabled: false` turns the tracker off completely: no beacon, no listener, no
`localStorage`. The API stays published, so `__clvtracker.setAuthId()` and
`__clvtracker.track()` are safe to call and do nothing.

Use it when the site loads itself in a headless browser. A nightly prerender, a
screenshot script or an end-to-end suite walks every page and otherwise reports
one full session per page, from a residential IP with an ordinary user agent,
which no crawler test can catch. ecarbrowser's prerender sets
`window.__PRERENDER__` before any page script runs, so the site configures:

```js
window.TRACKER_CONFIG = { enabled: !window.__PRERENDER__ };
```

### `sanitize`

Runs over every field the site controls: the path and each per-event value,
including the text of a caught error. Use it when the page holds something that
must never leave it, and the plainest form is a regex that blanks the shape of
that something:

```js
window.TRACKER_CONFIG = {
  // Any number with 3+ decimals or a 5+ digit run becomes "#", so a
  // coordinate cannot ride out inside an error message.
  sanitize: function (v) {
    return v.replace(/\d+\.\d{3,}/g, "#").replace(/\d{5,}/g, "#");
  }
};
```

The visitor id, session id, and timestamp skip it, so a scrubber aimed at
digits is safe to write: it will never see a UUID.

## Click maps

`clicks: "all"` reports every click, not only the ones on a `[data-track]`
element, and adds where the click landed:

| field | what it is |
|-------|-----------|
| `sel` | the `data-track` name when the click was inside one, else `#id`, else `tag.class` |
| `txt` | the element's text, cut at 30 characters |
| `x`, `y` | pixels from the top left of the viewport |
| `px`, `py` | the same as percentages, so one visitor's 1440px screen lays over another's phone |
| `vw`, `vh` | the viewport it was measured in |

Tagged elements read the same in both modes, so click-through rate still joins
to their impressions. An untagged click gets no `view` or `displayed`, because
nothing measured it entering the viewport, and inventing one would put
elements in the CTR denominator that were never counted.

`clickData` adds fields of your own to a click. It receives the clicked
element, so your markup stays your business:

```js
window.TRACKER_CONFIG = {
  clicks: "all",
  clickData: function (el) {
    var card = el.closest("[data-product]");
    return card ? { product: card.dataset.product } : null;
  }
};
```

It runs in both modes, and a `null` return adds nothing.

## Campaign links

Link into the site with `?em=<campaign>`, and optionally `?emc=<which link>`:

```
https://example.com/pricing?em=september-digest&emc=row-3
```

The visit sends one `email_click` carrying both, at init, before an app has a
chance to strip its own query params. The landing path is on the beacon
already, so `em` says which send, `emc` says which link in it, and the path
says where it led. Both values are cut at 30 characters.

## Signed-in visits

A site with accounts can tell the tracker who is signed in:

```js
// wherever your auth state lives
window.__clvtracker.setAuthId(user.id);   // signed in
window.__clvtracker.setAuthId(null);      // signed out
```

Every beacon after that carries `aid=<id>`, and one `auth` event marks the
transition, so a session counts as signed in without scanning its beacons for
an id. Sign-out clears the id and sends nothing: the absence of `aid` on later
beacons is the signal.

`uid` is untouched by this. It answers "same browser" and `aid` answers "same
person", so the pair is what joins one person's phone and laptop. Pass an
opaque id you already have, such as the `sub` of an OIDC token. The tracker
sends it as given: it is an identity field, so it skips `sanitize`, and a
username or an email address would land in the CDN logs verbatim.

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
