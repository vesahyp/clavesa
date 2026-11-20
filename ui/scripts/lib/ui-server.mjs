// Start `bin/clavesa ui --workspaces <dir> --read-only` on a free port for
// a Playwright script, or use BASE_URL when one is already running.
import { spawn } from "node:child_process";
import { createServer } from "node:net";

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

export async function freePort() {
  return new Promise((resolve, reject) => {
    const srv = createServer();
    srv.once("error", reject);
    srv.listen(0, "127.0.0.1", () => {
      const { port } = srv.address();
      srv.close(() => resolve(port));
    });
  });
}

export async function startUI(bin, wsDir, port) {
  const ui = spawn(bin, ["ui", "--no-browser", "--workspaces", wsDir, "--read-only"], {
    cwd: wsDir,
    env: { ...process.env, CLAVESA_ADDR: `127.0.0.1:${port}` },
    stdio: ["ignore", "pipe", "pipe"],
  });
  let log = "";
  ui.stdout.on("data", (d) => (log += d));
  ui.stderr.on("data", (d) => (log += d));
  let exited = false;
  ui.on("exit", () => (exited = true));
  for (let i = 0; i < 120; i++) {
    if (exited) break;
    try {
      const r = await fetch(`http://127.0.0.1:${port}/api/workspaces`);
      if (r.ok) return ui;
    } catch {}
    await sleep(500);
  }
  ui.kill();
  throw new Error(`clavesa ui did not start on :${port}\n${log.slice(-4000)}`);
}

/** BASE_URL, or a server started here; `stop()` ends it on exit. */
export async function uiServer(bin, wsDir) {
  const given = process.env.BASE_URL?.replace(/\/$/, "");
  if (given) {
    console.log(`using the server at ${given}`);
    return { base: given, stop() {} };
  }
  const port = await freePort();
  const base = `http://127.0.0.1:${port}`;
  console.log(`clavesa ui --workspaces ${wsDir} --read-only on ${base}`);
  const ui = await startUI(bin, wsDir, port);
  process.on("exit", () => ui.kill());
  return { base, stop: () => ui.kill() };
}
