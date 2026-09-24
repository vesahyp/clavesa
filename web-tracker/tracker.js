/* Clavesa — lightweight, dependency-free analytics.
 *
 * Sends events as GET beacons to a 1x1 pixel (/t.gif); the data rides in the
 * query string and is captured by CloudFront access logs. No cookies, no
 * backend endpoint, no PII. Mirrors the ecarbrowser tracker, trimmed for a
 * static marketing site. Configure by setting window.TRACKER_CONFIG before
 * this script loads.
 */
(function () {
  "use strict";

  var config = {
    // false turns the tracker off completely: no beacon leaves the page, no
    // listener is attached, and nothing is written to localStorage. A site that
    // renders itself with a headless browser (a prerender, a screenshot run, an
    // end-to-end test) sets this from whatever marks that run, and its own
    // traffic stops arriving as visits.
    enabled: true,
    endpoint: "/t.gif",
    sessionTimeout: 30 * 60 * 1000, // 30 min sliding session
    maxStringLength: 200,
    // Optional function(value) -> value, applied to every field the site
    // itself controls. Default null: no site needs it until one does.
    sanitize: null,
    // "tagged": only [data-track] elements report a click, which is what a
    // marketing site wants. "all": every click reports, with a selector and
    // where on the screen it landed, which is what a click map needs.
    clicks: "tagged",
    // Optional function(element) -> object, merged into a click event. The
    // element is the one clicked, so a site reads its own markup here (a card's
    // id, a row's product) without this file knowing anything about it.
    clickData: null,
    debug: false
  };
  if (window.TRACKER_CONFIG) {
    for (var k in window.TRACKER_CONFIG) config[k] = window.TRACKER_CONFIG[k];
  }

  // The signed-in identity, when the site hands one over (see setAuthId). Null
  // for every visit that never signs in, which is most of them.
  var authId = null;

  var UID_KEY = "clv_uid";
  var SID_KEY = "clv_sid";
  var DISP_KEY = "clv_disp";
  var VIEW_KEY = "clv_view";

  function log() { if (config.debug) console.log.apply(console, ["[clv]"].concat([].slice.call(arguments))); }

  function uuid() {
    try { if (crypto && crypto.randomUUID) return crypto.randomUUID(); } catch (e) {}
    return "xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx".replace(/[xy]/g, function (c) {
      var r = (Math.random() * 16) | 0;
      return (c === "x" ? r : (r & 0x3) | 0x8).toString(16);
    });
  }

  function trunc(s, n) {
    if (s == null) return "";
    s = String(s);
    return s.length > n ? s.slice(0, n) : s;
  }

  // Persistent visitor id (survives sessions).
  function getUserId() {
    try {
      var id = localStorage.getItem(UID_KEY);
      if (!id) { id = uuid(); localStorage.setItem(UID_KEY, id); }
      return id;
    } catch (e) {
      track("tracker_error", { src: "uid", msg: e.message });
      return "anon";
    }
  }

  // 30-minute sliding session id.
  function getSession() {
    var now = Date.now();
    try {
      var raw = sessionStorage.getItem(SID_KEY);
      var s = raw ? JSON.parse(raw) : null;
      if (!s || !s.id || now - s.ts > config.sessionTimeout) {
        s = { id: uuid(), ts: now, isNew: true };
      } else {
        s.isNew = false;
        s.ts = now;
      }
      sessionStorage.setItem(SID_KEY, JSON.stringify({ id: s.id, ts: s.ts }));
      return s;
    } catch (e) {
      return { id: uuid(), ts: now, isNew: true };
    }
  }

  // Site-supplied last line of defence over the values the site controls: the
  // path and the per-event fields. Error text is the one that carries whatever
  // the page happened to be holding, so a site with something it must never
  // emit (a coordinate, an order number) installs a scrubber here rather than
  // forking this file. Identity fields and the timestamp skip it on purpose —
  // a scrubber written for digits would mangle a UUID.
  function scrub(v) {
    if (!config.sanitize) return v;
    try { return config.sanitize(String(v)); } catch (e) { return ""; }
  }

  function track(event, data) {
    if (!config.enabled) return;
    try {
      var session = getSession();
      var params = new URLSearchParams();
      params.set("e", event);
      params.set("uid", getUserId());
      params.set("sid", session.id);
      params.set("p", scrub(location.pathname));
      params.set("t", String(Date.now()));
      // An identity field, so it skips scrub() for the same reason uid and sid
      // do: a scrubber written for digits would mangle it.
      if (authId) params.set("aid", authId);
      if (data) for (var key in data) if (data[key] != null) params.set(key, scrub(data[key]));

      var url = config.endpoint + "?" + params.toString();
      log("track", event, data || {});
      // sendBeacon flushes reliably even as the page navigates or unloads: the
      // browser guarantees delivery after the page is gone, so a click on a link
      // no longer loses its beacon to the navigation (the GET-image path could be
      // cancelled mid-flight, surfacing as clicks without a matching impression).
      // /t.gif answers POST with 200 and the pipeline keys off the query string,
      // not the method/status. A GET image is the fallback where sendBeacon is
      // absent.
      if (navigator.sendBeacon) {
        navigator.sendBeacon(url);
      } else {
        var img = new Image();
        img.src = url;
      }
    } catch (e) {
      // Last-ditch: never let tracking throw into the page.
      if (config.debug) console.warn("[clv] track failed", e);
    }
  }

  // Set or clear the signed-in identity. The site calls this from wherever its
  // auth state lives, and every beacon after it carries aid, so the pipeline can
  // join one person's sessions across their devices. uid stays what it was: it
  // answers "same browser", which is a different question.
  //
  // One auth event marks the transition, so a session can be counted as signed
  // in without scanning every beacon in it for an aid. Sign-out clears the id
  // and sends nothing: the absence of aid on later beacons is the signal, and an
  // event there would be a second way to say the same thing.
  function setAuthId(id) {
    var next = id ? String(id) : null;
    if (next === authId) return;
    authId = next;
    if (authId) track("auth", {});
  }

  // What to call the thing that was clicked, in "all" mode. A [data-track] name
  // when the click landed inside one, so a tagged element reads the same in both
  // modes and CTR still joins to its impressions. Otherwise an id, or tag plus
  // first class, which is enough to tell one region of a page from another
  // without becoming a brittle full-path selector.
  function selectorFor(el) {
    try {
      if (el && el.nodeType === 3) el = el.parentElement; // a text node
      if (!el) return "";
      var tagged = el.closest && el.closest("[data-track]");
      if (tagged) return tagged.getAttribute("data-track");
      if (el.id) return "#" + el.id;
      var tag = el.tagName ? el.tagName.toLowerCase() : "unknown";
      // className is an SVGAnimatedString on SVG elements, not a string.
      if (el.className && typeof el.className === "string" && el.className.trim()) {
        return tag + "." + el.className.trim().split(/\s+/)[0];
      }
      return tag;
    } catch (e) {
      return "unknown";
    }
  }

  // Elements already marked "viewed" this session (sel -> true). Lightest
  // impression tier: the element entered the viewport at all, no dwell
  // required. Fires at most once per sel per session.
  var viewed = {};
  try {
    var storedViewed = sessionStorage.getItem(VIEW_KEY);
    if (storedViewed) {
      var viewedArr = JSON.parse(storedViewed);
      for (var vi = 0; vi < viewedArr.length; vi++) viewed[viewedArr[vi]] = true;
    }
  } catch (e) {
    // sessionStorage unavailable — fall back to the in-memory `viewed`
    // object for the rest of the page's life (no persistence across pages).
  }

  function markViewed(sel) {
    if (!sel || viewed[sel]) return;
    viewed[sel] = true;
    track("view", { sel: sel });
    try {
      sessionStorage.setItem(VIEW_KEY, JSON.stringify(Object.keys(viewed)));
    } catch (e) {}
  }

  // Elements already marked "displayed" this session (sel -> true). Feeds
  // CTR = clicks / displays per element, so it must fire at most once per
  // sel per session and a click must always count as a display too.
  var displayed = {};
  try {
    var storedDisplayed = sessionStorage.getItem(DISP_KEY);
    if (storedDisplayed) {
      var displayedArr = JSON.parse(storedDisplayed);
      for (var di = 0; di < displayedArr.length; di++) displayed[displayedArr[di]] = true;
    }
  } catch (e) {
    // sessionStorage unavailable — fall back to the in-memory `displayed`
    // object for the rest of the page's life (no persistence across pages).
  }

  function markDisplayed(sel) {
    if (!sel || displayed[sel]) return;
    markViewed(sel); // displayed implies view: view ⊇ displayed
    displayed[sel] = true;
    track("displayed", { sel: sel });
    try {
      sessionStorage.setItem(DISP_KEY, JSON.stringify(Object.keys(displayed)));
    } catch (e) {}
  }

  function init() {
    var session = getSession();

    // session_start (once per session).
    if (session.isNew) {
      track("session_start", {
        ref: trunc(document.referrer, 100),
        vw: window.innerWidth,
        vh: window.innerHeight
      });
    }

    // pageview — every load, not once per session.
    //
    // session_start answers "where did the visit begin", which is a different
    // question from "which pages were read", and it was doing duty for both.
    // The second page of a visit sent nothing at all unless it happened to
    // carry a [data-track] element that scrolled into view, so a short page
    // read start to finish was invisible. Every beacon already carries the
    // path, so this needs no field of its own: the event is the signal.
    track("pageview", {});

    // Campaign attribution. A mail or an ad that links in with ?em=<campaign>,
    // and optionally ?emc=<which link in it>, gets one event here at init: an
    // app commonly strips its own query params right after mount, and once they
    // are gone nothing later in the visit can report the click-through. The
    // landing path is already on every beacon, so where the link led needs no
    // field of its own.
    try {
      var qs = new URLSearchParams(location.search);
      var campaign = qs.get("em");
      if (campaign) {
        var mail = { em: trunc(campaign, 30) };
        if (qs.get("emc")) mail.emc = trunc(qs.get("emc"), 30);
        track("email_click", mail);
      }
    } catch (err) {
      track("tracker_error", { src: "email_landing", msg: err.message });
    }

    // Clicks — one delegated capture-phase listener so app handlers that call
    // stopPropagation can't swallow them. Only elements explicitly opted in
    // via data-track are tracked; everything else is a silent no-op.
    document.addEventListener("click", function (e) {
      try {
        var el = e.target;
        var tracked = el && el.closest && el.closest("[data-track]");
        if (!tracked && config.clicks !== "all") return;

        var data;
        if (tracked) {
          var sel = tracked.getAttribute("data-track");
          // A click implies the element was seen, so count it toward CTR's
          // denominator even if the impression observer has not fired yet.
          // markDisplayed also backfills the view tier (view ⊇ displayed).
          markDisplayed(sel);
          data = { sel: sel };
        } else {
          // An untagged element has no impression to join to, so it gets no
          // markDisplayed: a view tier invented here would put elements in the
          // CTR denominator that were never measured entering the viewport.
          data = { sel: selectorFor(el) };
        }

        if (config.clicks === "all") {
          var vw = window.innerWidth, vh = window.innerHeight;
          data.txt = trunc(el && el.textContent ? el.textContent.trim() : "", 30);
          data.x = e.clientX;
          data.y = e.clientY;
          // Percentages as well as pixels, because a click map has to lay one
          // visitor's 1440px screen over another's phone.
          data.px = Math.round((e.clientX / vw) * 100);
          data.py = Math.round((e.clientY / vh) * 100);
          data.vw = vw;
          data.vh = vh;
        }

        if (config.clickData) {
          var extra = config.clickData(el);
          if (extra) for (var f in extra) if (extra[f] != null) data[f] = extra[f];
        }

        track("click", data);
      } catch (err) {
        track("tracker_error", { src: "click_handler", msg: err.message });
      }
    }, true);

    // Impressions — two tiers on [data-track] elements only:
    //   view      — the element entered the viewport at all (any ratio > 0).
    //   displayed — >=50% visible for a continuous 3s ("really saw"), so
    //               CTR = clicks / displays per element.
    // Old browsers without IntersectionObserver just skip both.
    if (window.IntersectionObserver) {
      try {
        var io = new IntersectionObserver(function (entries) {
          entries.forEach(function (entry) {
            var el = entry.target;
            var sel = el.getAttribute("data-track");
            if (entry.isIntersecting) {
              markViewed(sel);
            }
            if (entry.isIntersecting && entry.intersectionRatio >= 0.5) {
              if (el.__clvDwell) return; // already timing this visibility span
              el.__clvDwell = setTimeout(function () {
                el.__clvDwell = null;
                markDisplayed(sel);
                io.unobserve(el); // done for the session either way
              }, 3000);
            } else if (el.__clvDwell) {
              clearTimeout(el.__clvDwell); // left before the 3s dwell completed
              el.__clvDwell = null;
            }
          });
        }, { threshold: [0, 0.5] });
        var trackedEls = document.querySelectorAll("[data-track]");
        for (var ti = 0; ti < trackedEls.length; ti++) io.observe(trackedEls[ti]);
      } catch (err) {
        track("tracker_error", { src: "impression_observer", msg: err.message });
      }
    }

    // Scroll-depth milestones, once each per session.
    var milestones = [25, 50, 75, 100];
    var hit = {};
    var maxDepth = 0;
    // How far down the page is right now, where a page with nothing to
    // scroll counts as fully read.
    function depthNow() {
      var doc = document.documentElement;
      var scrollable = doc.scrollHeight - doc.clientHeight;
      return scrollable > 0 ? Math.round((doc.scrollTop / scrollable) * 100) : 100;
    }
    function onScroll() {
      try {
        var pct = depthNow();
        if (pct > maxDepth) maxDepth = pct;
        for (var i = 0; i < milestones.length; i++) {
          var m = milestones[i];
          if (pct >= m && !hit[m]) { hit[m] = true; track("scroll", { depth: m }); }
        }
      } catch (err) {
        track("tracker_error", { src: "scroll_handler", msg: err.message });
      }
    }
    window.addEventListener("scroll", onScroll, { passive: true });

    // Web Vitals (best-effort; older browsers just skip).
    var lcp = null, cls = 0;
    try {
      if (window.PerformanceObserver) {
        new PerformanceObserver(function (list) {
          var entries = list.getEntries();
          lcp = entries[entries.length - 1].startTime;
        }).observe({ type: "largest-contentful-paint", buffered: true });
        new PerformanceObserver(function (list) {
          list.getEntries().forEach(function (entry) {
            if (!entry.hadRecentInput) cls += entry.value;
          });
        }).observe({ type: "layout-shift", buffered: true });
      }
    } catch (err) {
      track("tracker_error", { src: "perf_observer", msg: err.message });
    }

    // INP (Interaction to Next Paint), the vital that replaced FID in March
    // 2024. FID measured how long the page took to START handling the first
    // interaction, which said nothing about the ones that actually felt slow.
    // INP measures how long it took to paint after an interaction, and reports
    // the worst one.
    //
    // Its own observer in its own try: "event" with durationThreshold is the
    // newest entry type here, and a browser that rejects it must not take LCP
    // and CLS down with it.
    //
    // The worst interaction is the INP for any visit with under 50 of them,
    // which is nearly every visit. The full definition discards one outlier per
    // 50 interactions, and that bookkeeping is not worth the bytes here.
    // Threshold 40ms rather than the 104ms default, so an interaction that is
    // sluggish without being broken still registers.
    var inp = 0;
    try {
      if (window.PerformanceObserver) {
        new PerformanceObserver(function (list) {
          list.getEntries().forEach(function (entry) {
            if (entry.interactionId && entry.duration > inp) inp = entry.duration;
          });
        }).observe({ type: "event", buffered: true, durationThreshold: 40 });
      }
    } catch (err) {
      track("tracker_error", { src: "inp_observer", msg: err.message });
    }

    // JS errors. Extension-injected scripts throw on pages they ride
    // along to ("Invalid call to runtime.sendMessage()" et al., found on
    // keitos 2026-08-21); those are the visitor's browser, not the site,
    // so they never reach the board.
    var EXT_ERROR = /extension|runtime\.sendMessage|runtime\.connect/i;
    window.addEventListener("error", function (e) {
      if (e.filename && /^(chrome|moz|safari)-extension:/.test(e.filename)) return;
      if (e.message && EXT_ERROR.test(e.message)) return;
      track("error", {
        msg: trunc(e.message, config.maxStringLength),
        file: trunc(e.filename, 50),
        line: e.lineno,
        type: "js"
      });
    });
    window.addEventListener("unhandledrejection", function (e) {
      var reason = e.reason && e.reason.message ? e.reason.message : e.reason;
      track("error", { msg: trunc(reason, config.maxStringLength), type: "promise" });
    });

    // End of visit — flush vitals + session_end via beacon.
    var started = Date.now();
    var ended = false;
    function endVisit() {
      if (ended) return;
      ended = true;
      if (lcp != null) track("lcp", { v: Math.round(lcp) });
      track("cls", { v: Math.round(cls * 1000) / 1000 });
      // A visit that never interacted has no INP, and 0 would read as instant.
      if (inp > 0) track("inp", { v: Math.round(inp) });
      track("session_end", {
        // A page that fits on the screen fires no scroll event ever, so
        // maxDepth stays 0 and the visit reads as "saw nothing" when the
        // truth is "saw all of it". Falling back to the depth right now
        // separates the two: unscrollable answers 100, scrollable but
        // untouched still answers 0, which is correct. Evaluated here rather
        // than at init because images and fonts change the page height after
        // load, and by page-hide the layout has settled.
        scroll: maxDepth || depthNow(),
        dur: Math.round((Date.now() - started) / 1000)
      });
    }
    window.addEventListener("pagehide", endVisit);
    document.addEventListener("visibilitychange", function () {
      if (document.visibilityState === "hidden") endVisit();
    });
  }

  // The API is published either way, so a site that calls setAuthId on login
  // does not have to know whether tracking is on; track() is the no-op.
  window.__clvtracker = { track: track, setAuthId: setAuthId };

  if (config.enabled) {
    if (document.readyState === "loading") {
      document.addEventListener("DOMContentLoaded", init);
    } else {
      init();
    }
  }
})();
