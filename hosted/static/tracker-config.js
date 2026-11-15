/* The hosted UI's settings for the canonical tracker (web-tracker/tracker.js,
 * copied unchanged by hosted/scripts/deploy-spa.sh). render-index.mjs loads
 * this file and then tracker.js, in the hosted index.html only: the local
 * `clavesa ui` serves its own index.html, which loads neither, and sends
 * nothing anywhere. A page without the hosted config turns the tracker off
 * here as a second guard.
 *
 * spa: client-side routes count as page views.
 *
 * sanitize runs over the path and every event value (an error's msg and
 * file, the referrer). It replaces what identifies one run or one person:
 *   - Step Functions execution ARNs and run ids (UUIDs, which is what an
 *     execution name is; the 32-hex run ids of clavesa) -> ":run"
 *   - JWTs (the Cognito id, access and refresh tokens all start "eyJ") and
 *     any other run of 40 or more token characters -> ":token"
 *   - email addresses -> ":email"
 * Workspace and pipeline names stay: they are what the board is about.
 *
 * The tracker reads only its own storage keys (clv_*), never the session
 * in sessionStorage. setAuthId is not called: no beacon carries who is
 * signed in.
 */
(function () {
  "use strict";
  var cfg = window.__CLAVESA__;
  var RULES = [
    [/arn:aws[a-z-]*:states:[^\s"'<>?#&]*/gi, ":run"],
    [/eyJ[\w-]*\.[\w-]*\.?[\w-]*/g, ":token"],
    [/[^\s@"'<>/:]+@[^\s@"'<>/]+\.[a-z]{2,}/gi, ":email"],
    [/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/gi, ":run"],
    [/[0-9a-f]{16,}/gi, ":run"],
    [/[A-Za-z0-9_+/=-]{40,}/g, ":token"]
  ];
  window.TRACKER_CONFIG = {
    enabled: !!(cfg && cfg.mode === "hosted"),
    spa: true,
    sanitize: function (v) {
      var s = String(v);
      for (var i = 0; i < RULES.length; i++) s = s.replace(RULES[i][0], RULES[i][1]);
      return s;
    }
  };
})();
