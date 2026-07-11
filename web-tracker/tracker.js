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
    endpoint: "/t.gif",
    sessionTimeout: 30 * 60 * 1000, // 30 min sliding session
    maxStringLength: 200,
    debug: false
  };
  if (window.TRACKER_CONFIG) {
    for (var k in window.TRACKER_CONFIG) config[k] = window.TRACKER_CONFIG[k];
  }

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

  function track(event, data) {
    try {
      var session = getSession();
      var params = new URLSearchParams();
      params.set("e", event);
      params.set("uid", getUserId());
      params.set("sid", session.id);
      params.set("p", location.pathname);
      params.set("t", String(Date.now()));
      if (data) for (var key in data) if (data[key] != null) params.set(key, data[key]);

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

    // Clicks — one delegated capture-phase listener so app handlers that call
    // stopPropagation can't swallow them. Only elements explicitly opted in
    // via data-track are tracked; everything else is a silent no-op.
    document.addEventListener("click", function (e) {
      try {
        var el = e.target;
        var tracked = el && el.closest && el.closest("[data-track]");
        if (!tracked) return;
        var sel = tracked.getAttribute("data-track");
        // A click implies the element was seen — count it toward CTR's
        // denominator even if the impression observer hasn't fired yet.
        // markDisplayed also backfills the view tier (view ⊇ displayed).
        markDisplayed(sel);
        track("click", { sel: sel });
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
    function onScroll() {
      try {
        var doc = document.documentElement;
        var scrollable = doc.scrollHeight - doc.clientHeight;
        var pct = scrollable > 0 ? Math.round((doc.scrollTop / scrollable) * 100) : 100;
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

    // JS errors.
    window.addEventListener("error", function (e) {
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
      track("session_end", {
        scroll: maxDepth,
        dur: Math.round((Date.now() - started) / 1000)
      });
    }
    window.addEventListener("pagehide", endVisit);
    document.addEventListener("visibilitychange", function () {
      if (document.visibilityState === "hidden") endVisit();
    });
  }

  window.__clvtracker = { track: track };

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();
