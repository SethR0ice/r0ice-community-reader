// R0ice Resource Team — native census + book-chunk worker (v0.4.0)
//
// v0.3 added: prefetch buffer, async submit, concurrency, telemetry, --bench.
// v0.4 adds:
//   - STREAM GUARD: auto-pauses while OBS is running and during a daily
//     pre-stream quiet window (default 17:45-18:15 local). Unloads the model
//     from VRAM on pause so the GPU is fully free for encoding. Resumes on
//     its own when OBS closes.
//   - STATUS WINDOW: friendly local page at http://127.0.0.1:8477 showing
//     exactly what the GPU is doing — which book, which piece, today's
//     pages, the whole team's live feed. Tray menu opens it. This is the
//     trust surface for friends: transparent, warm, zero mystery.
//   - OLLAMA AUTO-SETUP: if Ollama isn't installed, downloads and silently
//     installs it, then pulls a model sized to the machine's VRAM. A friend
//     runs one exe and their idle GPU joins the library.
//
// Server API unchanged (claim / book-chunk-result / census report).

const fs = require('fs');
const path = require('path');
const os = require('os');
const http = require('http');
const crypto = require('crypto');
const { execFile, execFileSync, spawn } = require('child_process');
const si = require('systeminformation');
const SysTray = require('systray').default;

const API_BASE = 'https://r0ice.com/resource-team/api';
const BASE_DIR = process.pkg ? path.dirname(process.execPath) : __dirname;
const CONFIG_PATH = path.join(BASE_DIR, 'config.json');
const METRICS_PATH = path.join(BASE_DIR, 'metrics.jsonl');
const STATS_PATH = path.join(BASE_DIR, 'stats.json');
const BENCH_PATH = path.join(BASE_DIR, 'bench-result.json');
const REPORT_INTERVAL_MS = 10 * 60 * 1000;
const CLIENT_VERSION = '0.4.4';
const STATUS_PORT = 8477;

const TRAY_ICON_B64 = 'AAABAAEAEBAAAAAAIADFAAAAFgAAAIlQTkcNChoKAAAADUlIRFIAAAAQAAAAEAgGAAAAH/P/YQAAAIxJREFUeJxjYKAQMKILCAvy/Sek6e37T3B9jPg0bp0eC6a9MxfjNIgJly0wzehsdMCIz9mEXAACTAwEwOEbXxfik2ei1AtMhDQTMoSJEudjGLAVagtIIwjbanDHgzCMj80ljLBYQJaAhTp6LGBTwwgzAKYAX5RhU8OIbAA5ABwGyGmbFIA3KRMLKM6NABalTYb+T6cXAAAAAElFTkSuQmCC';

// ---------------------------------------------------------------- config ----

function loadConfig() {
  try { return JSON.parse(fs.readFileSync(CONFIG_PATH, 'utf8')); } catch { return {}; }
}
function saveConfig(cfg) {
  fs.writeFileSync(CONFIG_PATH, JSON.stringify(cfg, null, 2));
}

async function ensureToken() {
  const cfg = loadConfig();
  if (cfg.token) { activeToken = cfg.token; return cfg; }
  const handle = os.hostname();
  // This is the very first network hop a brand-new volunteer's install ever
  // makes -- before any census, before Ollama setup, before anything. It had
  // no AbortSignal at all, so a dead/blackholed connection here would sit
  // silently forever with zero signal (same bug class as the old non-
  // streaming model pull). Now bounded, and the setup_note is visible on the
  // status page because main() starts the status server before calling this.
  live.mode = 'setup';
  live.setup_note = 'joining the Resource Team (first-time setup)…';
  const res = await fetch(`${API_BASE}/jobs/join`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ handle }),
    signal: AbortSignal.timeout(30 * 1000)
  });
  if (!res.ok) throw new Error(`join failed: ${res.status}`);
  const { token } = await res.json();
  const newCfg = { ...cfg, token, handle, paused: false };
  saveConfig(newCfg);
  activeToken = token;
  live.setup_note = '';
  console.log(`Joined Resource Team as "${handle}"`);
  return newCfg;
}

// ------------------------------------------------------- shared live state ----

const live = {
  mode: 'starting',        // starting | setup | reading | idle | paused-user | paused-stream | paused-quiet | dry
  setup_note: '',
  model: null,
  current: [],             // [{title, author, chunk, of, started, wid}]
  recent: [],              // last 10 {title, chunk, of, secs, at}
  today: { chunks: 0, words: 0 },
  session: { chunks: 0, errors: 0, submitted: 0, started: new Date().toISOString() },
  gpu: null,
  concurrency: 1,
  // Set when the server 401s our token (superseded / revoked). Independent of
  // `mode` on purpose -- this must stay visible even if the pipeline's own
  // state machine is busy elsewhere, so the status page can show a loud,
  // unambiguous REJECTED banner instead of quietly looking like "idle".
  token_rejected: false,
  token_rejected_at: null,
  // First time this rejection began (unlike token_rejected_at, which refreshes
  // on every subsequent 401) -- used to measure "sustained" for auto-rejoin.
  token_rejected_since: null,
  // Last fatal error, if any -- reported to the server (see gatherHardware())
  // so a stuck/dead setup is diagnosable remotely instead of only visible in
  // a local console window that's gone the moment the process exits or the
  // window gets closed (bit a volunteer for a full night with zero server-
  // side signal beyond "stopped reporting").
  last_error: null,
  last_error_at: null
};

// The one working token, shared by reportOnce() and the pipeline's claim/submit
// calls. A `let` at module scope on purpose: a rejoin reassigns this single
// place and every closure reading `activeToken` picks up the new value on its
// very next call -- no need to thread a fresh token through every function.
let activeToken = null;
let rejoinInFlight = false;
let nextRejoinAttemptAt = 0;

// ---------------------------------------------------------------- census ----

async function checkOllama() {
  try {
    const res = await fetch('http://127.0.0.1:11434/api/tags', { signal: AbortSignal.timeout(1500) });
    return res.ok;
  } catch { return false; }
}

async function gatherHardware() {
  const [cpu, mem, graphics] = await Promise.all([si.cpu(), si.mem(), si.graphics()]);
  const gpu = (graphics.controllers || []).find(c => c.vram && c.vram > 0) || graphics.controllers?.[0] || {};
  const hasOllama = await checkOllama();
  return {
    platform: os.platform(),
    cpu_model: `${cpu.manufacturer || ''} ${cpu.brand || ''}`.trim(),
    cpu_cores: cpu.cores || cpu.physicalCores || null,
    ram_gb: +(mem.total / (1024 ** 3)).toFixed(1),
    gpu_model: gpu.model || null,
    vram_gb: gpu.vram ? +(gpu.vram / 1024).toFixed(1) : null,
    has_ollama: hasOllama,
    client_version: CLIENT_VERSION,
    // Live status, piggybacked on the same census payload so a stuck/crashed
    // setup is diagnosable remotely -- no need to relay F12 console output by
    // hand, the server already has whatever the status page would show.
    mode: live.mode,
    setup_note: live.setup_note,
    last_error: live.last_error,
    last_error_at: live.last_error_at
  };
}

async function reportOnce() {
  const hw = await gatherHardware();
  try {
    // No AbortSignal here meant a stalled connection would hang this call
    // (and thus this whole periodic report) silently forever -- same bug
    // class as the old model pull, just on the census path instead.
    const res = await fetch(`${API_BASE}/jobs/census/report`, {
      method: 'POST',
      headers: { Authorization: `Bearer ${activeToken}`, 'content-type': 'application/json' },
      body: JSON.stringify(hw),
      signal: AbortSignal.timeout(20 * 1000)
    });
    console.log(`[${new Date().toISOString()}] census report: ${res.ok ? 'ok' : 'failed (' + res.status + ')'}`);
    if (res.status === 401) {
      // Token has been marked superseded/revoked server-side. Without this,
      // the worker just repeats "failed (401)" forever with no other signal --
      // that silent-for-hours failure mode is exactly what bit us twice in one
      // night (2026-07-19). Make it loud in the log AND on the status page.
      if (!live.token_rejected) {
        console.error(`[FATAL] TOKEN REJECTED (401) -- config at ${CONFIG_PATH} holds a token the server no longer accepts (superseded/revoked). This worker will keep running but cannot report census or claim work until config.json is fixed and the process is restarted. See WHICH_CONFIG.txt next to this file before editing config.json.`);
        live.token_rejected_since = new Date().toISOString();
      }
      live.token_rejected = true;
      live.token_rejected_at = new Date().toISOString();
      await maybeRejoin(); // additive: try auto-rejoin once the rejection has persisted (see below)
    } else if (res.ok && live.token_rejected) {
      console.log('[recovered] census report succeeded again -- clearing REJECTED status.');
      live.token_rejected = false;
      live.token_rejected_at = null;
      live.token_rejected_since = null;
    }
  } catch (e) {
    console.log(`census report failed: ${e.message}`);
  }
  return hw;
}

// ------------------------------------------------------------ auto-rejoin ----
// A 401 by itself is only ever visibility (above): it never recovers on its
// own. If the rejection has been sustained for REJOIN_AFTER_MS -- long enough
// to rule out a one-off blip -- call the same /jobs/join endpoint ensureToken()
// already uses at first boot, get a brand-new token for this same handle, and
// swap it into `activeToken` + config.json. Every caller reads `activeToken`
// live, so report/claim/submit all resume under the new identity immediately,
// with no process restart needed.
const REJOIN_AFTER_MS = 2 * 60 * 1000; // 2 min of sustained 401s before rejoining
const REJOIN_RETRY_MS = 60 * 1000;     // cooldown if the rejoin call itself fails

async function rejoinTeam() {
  const cfg = loadConfig();
  const handle = cfg.handle || os.hostname();
  console.log(`[REJOIN] token rejected for ${Math.round(REJOIN_AFTER_MS / 60000)}+ min -- rejoining as "${handle}" for a fresh token…`);
  // Same bare-fetch-no-timeout gap as ensureToken()'s first-boot join call --
  // a stalled connection here would hang silently forever with no signal.
  const res = await fetch(`${API_BASE}/jobs/join`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ handle }),
    signal: AbortSignal.timeout(30 * 1000)
  });
  if (!res.ok) throw new Error(`rejoin failed: ${res.status}`);
  const { token } = await res.json();
  activeToken = token;
  saveConfig({ ...loadConfig(), token, handle });
  live.token_rejected = false;
  live.token_rejected_at = null;
  live.token_rejected_since = null;
  console.log(`[REJOIN] success -- new token written to ${CONFIG_PATH}, resuming as "${handle}".`);
}

async function maybeRejoin() {
  if (!live.token_rejected || !live.token_rejected_since) return;
  if (rejoinInFlight || Date.now() < nextRejoinAttemptAt) return;
  if (Date.now() - new Date(live.token_rejected_since).getTime() < REJOIN_AFTER_MS) return;
  rejoinInFlight = true;
  try {
    await rejoinTeam();
  } catch (e) {
    console.error('[REJOIN] failed:', e.message);
    nextRejoinAttemptAt = Date.now() + REJOIN_RETRY_MS;
  } finally {
    rejoinInFlight = false;
  }
}

// ------------------------------------------------------------ GPU sampler ----

const gpuSamples = [];
let gpuSamplerAvailable = true;

function sampleGpu() {
  if (!gpuSamplerAvailable) return;
  execFile('nvidia-smi',
    ['--query-gpu=utilization.gpu,memory.used,power.draw', '--format=csv,noheader,nounits'],
    { timeout: 3000, windowsHide: true }, (err, stdout) => {
      if (err) { gpuSamplerAvailable = false; return; }
      const [util, mem, power] = String(stdout).trim().split(',').map(s => parseFloat(s));
      const sample = { t: Date.now(), util: util || 0, mem_mb: mem || 0, power_w: power || 0 };
      gpuSamples.push(sample);
      live.gpu = sample;
      const cutoff = Date.now() - 15 * 60 * 1000;
      while (gpuSamples.length && gpuSamples[0].t < cutoff) gpuSamples.shift();
    });
}

function gpuWindowStats(windowMs) {
  const cutoff = Date.now() - windowMs;
  const win = gpuSamples.filter(s => s.t >= cutoff);
  if (!win.length) return null;
  const avg = win.reduce((a, s) => a + s.util, 0) / win.length;
  const idle = win.filter(s => s.util < 20).length / win.length;
  return { util_avg: +avg.toFixed(1), idle_pct: +(idle * 100).toFixed(1), samples: win.length };
}

// ------------------------------------------------------------ stream guard ----

// The GPU belongs to the stream. OBS often sits open for HOURS before going
// live (prep), so a bare process check would waste whole afternoons. When
// obs-websocket is enabled we read its config right off this machine (port +
// password) and ask OBS directly whether it is actually streaming/recording —
// pausing only then. Machines without obs-websocket fall back to the blunt
// process check.
const OBS_PROCESSES = ['obs64.exe', 'obs32.exe', 'streamlabs obs.exe'];

function obsRunning() {
  return new Promise(resolve => {
    execFile('tasklist', ['/FO', 'CSV', '/NH'], { timeout: 5000, windowsHide: true }, (err, stdout) => {
      if (err) return resolve(false);
      const low = String(stdout).toLowerCase();
      resolve(OBS_PROCESSES.some(p => low.includes(p)));
    });
  });
}

function obsWsConfig() {
  try {
    const p = path.join(process.env.APPDATA || '', 'obs-studio', 'plugin_config', 'obs-websocket', 'config.json');
    const c = JSON.parse(fs.readFileSync(p, 'utf8'));
    if (!c.server_enabled) return null;
    return { port: c.server_port || 4455, password: c.server_password || '', auth: !!c.auth_required };
  } catch { return null; }
}

// obs-websocket v5 handshake: Hello(0) -> Identify(1) -> Identified(2) -> Request(6) -> RequestResponse(7)
function obsWsStatus() {
  return new Promise(resolve => {
    const cfg = obsWsConfig();
    if (!cfg || typeof WebSocket === 'undefined') return resolve(null);
    let settled = false;
    const done = v => { if (!settled) { settled = true; try { ws.close(); } catch {} resolve(v); } };
    const timer = setTimeout(() => done(null), 4000);
    let ws;
    try { ws = new WebSocket(`ws://127.0.0.1:${cfg.port}`); } catch { clearTimeout(timer); return resolve(null); }
    const status = { streaming: false, recording: false };
    let answers = 0;
    ws.onerror = () => { clearTimeout(timer); done({ ...status, obs_closed: true }); }; // refused = OBS not running
    ws.onmessage = ev => {
      let m; try { m = JSON.parse(ev.data); } catch { return; }
      if (m.op === 0) { // Hello
        const d = { rpcVersion: 1 };
        if (m.d.authentication && cfg.auth) {
          const secret = crypto.createHash('sha256').update(cfg.password + m.d.authentication.salt).digest('base64');
          d.authentication = crypto.createHash('sha256').update(secret + m.d.authentication.challenge).digest('base64');
        }
        ws.send(JSON.stringify({ op: 1, d }));
      } else if (m.op === 2) { // Identified
        ws.send(JSON.stringify({ op: 6, d: { requestType: 'GetStreamStatus', requestId: 's' } }));
        ws.send(JSON.stringify({ op: 6, d: { requestType: 'GetRecordStatus', requestId: 'r' } }));
      } else if (m.op === 7) { // RequestResponse
        if (m.d.requestId === 's') status.streaming = !!(m.d.responseData && m.d.responseData.outputActive);
        if (m.d.requestId === 'r') status.recording = !!(m.d.responseData && m.d.responseData.outputActive);
        if (++answers >= 2) { clearTimeout(timer); done(status); }
      }
    };
  });
}

// Returns a pause reason string or null.
async function obsBlocking() {
  const ws = await obsWsStatus();
  if (ws) return (ws.streaming || ws.recording) ? 'paused-stream' : null;
  // no websocket on this machine — blunt fallback
  return (await obsRunning()) ? 'paused-stream' : null;
}

function inQuietWindow(cfg) {
  // Daily stream block — EXPLICIT config only ("prestream": "17:45-22:05" in
  // config.json). Never defaulted: every streamer's hours are different, so a
  // baked-in window would idle the wrong GPUs at the wrong times. Machines
  // without it rely on OBS detection + the big pause button.
  const win = cfg.prestream;
  if (!win) return false;
  const m = /^(\d{1,2}):(\d{2})-(\d{1,2}):(\d{2})$/.exec(win);
  if (!m) return false;
  const now = new Date();
  const mins = now.getHours() * 60 + now.getMinutes();
  const a = +m[1] * 60 + +m[2], b = +m[3] * 60 + +m[4];
  return a <= b ? (mins >= a && mins < b) : (mins >= a || mins < b);
}

async function unloadModel(model) {
  try {
    await fetch('http://127.0.0.1:11434/api/generate', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ model, prompt: '', keep_alive: 0 }),
      signal: AbortSignal.timeout(10000)
    });
    console.log('model unloaded from VRAM (stream guard)');
  } catch {}
}

// ---------------------------------------------------------- ollama setup ----

const OLLAMA_INSTALLER_URL = 'https://ollama.com/download/OllamaSetup.exe';

function ollamaCliPath() {
  const guesses = [
    path.join(process.env.LOCALAPPDATA || '', 'Programs', 'Ollama', 'ollama.exe'),
    'D:\\R0iceResourceTeam\\Ollama\\ollama.exe',
    'E:\\R0iceResourceTeam\\Ollama\\ollama.exe',
  ];
  for (const g of guesses) { try { if (fs.existsSync(g)) return g; } catch {} }
  return 'ollama';
}

function ollamaInstalledOnDisk() {
  return ollamaCliPath() !== 'ollama';
}

function modelForVram(vramGb) {
  if ((vramGb || 0) >= 8) return 'llama3.1:8b';
  return 'llama3.2:3b'; // 6 GB cards: small, fast, fully on-GPU
}

async function waitForOllama(timeoutMs) {
  const t0 = Date.now();
  while (Date.now() - t0 < timeoutMs) {
    if (await checkOllama()) return true;
    await new Promise(r => setTimeout(r, 3000));
  }
  return false;
}

// Drive choice: the model download is multiple GB, so first-run setup lists
// every drive with its free space in the status window and lets the owner
// pick where Ollama + models live. Resolved via POST /setup/drive.
let driveChoiceResolve = null;

async function censusDrives() {
  try {
    const mounts = await si.fsSize();
    const seen = new Set();
    return mounts
      .filter(m => /^[A-Z]:/i.test(m.mount) && m.size > 20 * 1024 ** 3)
      .filter(m => { const k = m.mount[0].toUpperCase(); if (seen.has(k)) return false; seen.add(k); return true; })
      .map(m => ({
        mount: m.mount[0].toUpperCase() + ':',
        free_gb: +(m.available / 1024 ** 3).toFixed(0),
        total_gb: +(m.size / 1024 ** 3).toFixed(0)
      }))
      .sort((a, b) => b.free_gb - a.free_gb);
  } catch { return [{ mount: 'C:', free_gb: null, total_gb: null }]; }
}

// If the person never clicks a drive in the status window (browser didn't
// open, they stepped away overnight like the volunteer who hit the pull-hang
// bug, tab got closed, etc.) this used to await a promise that NOTHING would
// ever resolve -- an indefinite hang with no self-recovery, same shape as
// tonight's silent-pull bug. Bounded wait + auto-pick-most-free-space keeps
// the same "surface it, then don't hang forever" philosophy as the model
// pull's STALL_LIMIT_MS watchdog above.
const DRIVE_CHOICE_TIMEOUT_MS = 10 * 60 * 1000; // 10min, no click -> auto-pick

async function chooseInstallDrive(cfg) {
  if (cfg.ollamaDrive) return cfg.ollamaDrive;
  const drives = await censusDrives();
  if (drives.length === 1) return drives[0].mount;
  live.mode = 'setup-choice';
  live.drives = drives;
  live.setup_note = 'choose a drive for the reading engine';
  openStatusWindow();
  console.log('Waiting for drive choice in the status window…');
  let timer;
  const clicked = await new Promise(resolve => {
    driveChoiceResolve = mount => { clearTimeout(timer); resolve(mount); };
    timer = setTimeout(() => resolve(null), DRIVE_CHOICE_TIMEOUT_MS);
  });
  driveChoiceResolve = null;
  live.drives = null;
  // drives is already sorted by free_gb desc (see censusDrives), so [0] is
  // the most-free-space option -- the same choice a careful person would
  // likely make themselves.
  const mount = clicked || drives[0].mount;
  if (!clicked) {
    console.log(`No drive choice after ${DRIVE_CHOICE_TIMEOUT_MS / 60000} min -- defaulting to ${mount} (most free space) so setup doesn't hang forever.`);
    live.setup_note = `no reply to the drive question -- defaulting to ${mount} (most free space)`;
  }
  saveConfig({ ...loadConfig(), ollamaDrive: mount });
  return mount;
}

function setUserEnv(name, value) {
  return new Promise(resolve => {
    execFile('setx', [name, value], { timeout: 15000, windowsHide: true }, err => {
      if (err) console.log(`setx ${name} failed:`, err.message);
      process.env[name] = value;
      resolve();
    });
  });
}

async function ensureOllamaAndModel(hw) {
  // Visibility fix (2026-07-22): when has_ollama is already true (Ollama
  // detected running), this function used to fall straight through to
  // pickOllamaModel() with ZERO status update -- live.mode stayed at its
  // initial 'starting' value with no indication anything was happening at
  // all. Confirmed as a real gap on a volunteer's PC that reported has_ollama:
  // true, sat at mode='starting' for 2+ days with no error, and never showed
  // any download progress even left running a long time -- there was no
  // signal anywhere (console or status page) to show whether it was hung,
  // slow, or just never got here. This one line guarantees SOMETHING always
  // shows before any further await, however that further step behaves.
  console.log('checking for an installed reading model…');
  live.mode = 'setup';
  live.setup_note = 'checking for an installed reading model…';
  if (!hw.has_ollama) {
    // BOOT RACE GUARD: at PC startup this worker often wakes before Ollama.
    // Never reinstall what exists — wait, then start the installed binary,
    // and only download the installer if Ollama truly isn't on this machine.
    live.mode = 'setup';
    live.setup_note = 'waiting for the reading engine to wake up…';
    if (await waitForOllama(90 * 1000)) { hw.has_ollama = true; }
    if (!hw.has_ollama && ollamaInstalledOnDisk()) {
      console.log('Ollama installed but not running — starting it');
      live.setup_note = 'starting the reading engine…';
      try {
        const child = spawn(ollamaCliPath(), ['serve'], { detached: true, stdio: 'ignore', windowsHide: true });
        child.unref();
      } catch (e) { console.log('ollama start failed:', e.message); }
      if (await waitForOllama(60 * 1000)) hw.has_ollama = true;
    }
  }
  if (!hw.has_ollama) {
    const drive = await chooseInstallDrive(loadConfig());
    live.mode = 'setup';
    live.setup_note = 'downloading the reading engine (Ollama)…';
    console.log(`Ollama not found — installing to ${drive} — downloading installer…`);
    // models are the multi-GB part: point OLLAMA_MODELS at the chosen drive
    // BEFORE install so the service picks it up on first start
    await setUserEnv('OLLAMA_MODELS', `${drive}\\R0iceResourceTeam\\ollama-models`);
    const installerPath = path.join(os.tmpdir(), 'OllamaSetup.exe');
    // Same bug class as the model pull below, and the originally-flagged
    // instance of it: a bare `await fetch(...)` + `.arrayBuffer()` here had
    // NO AbortSignal at all and buffered the whole installer silently in one
    // shot -- indistinguishable from a hang to anything watching the
    // connection, and nothing would ever time it out. Now streamed with byte
    // progress in live.setup_note plus an AbortController + 10min stall
    // watchdog, matching the model-pull fix's pattern exactly.
    const installerController = new AbortController();
    let installerLastProgressAt = Date.now();
    const INSTALLER_STALL_LIMIT_MS = 10 * 60 * 1000;
    const installerStallWatchdog = setInterval(() => {
      if (Date.now() - installerLastProgressAt > INSTALLER_STALL_LIMIT_MS) {
        installerController.abort(new Error('installer download stalled -- no progress for 10 minutes'));
      }
    }, 15 * 1000);
    try {
      const res = await fetch(OLLAMA_INSTALLER_URL, { signal: installerController.signal });
      if (!res.ok || !res.body) throw new Error(`installer download failed: ${res.status}`);
      const totalBytes = Number(res.headers.get('content-length')) || 0;
      let received = 0;
      const parts = [];
      for await (const chunk of res.body) {
        installerLastProgressAt = Date.now();
        const buf = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
        parts.push(buf);
        received += buf.length;
        const mb = n => (n / 1e6).toFixed(0);
        live.setup_note = totalBytes
          ? `downloading the reading engine (Ollama) — ${mb(received)}/${mb(totalBytes)} MB (${Math.round((received / totalBytes) * 100)}%)…`
          : `downloading the reading engine (Ollama) — ${mb(received)} MB…`;
      }
      fs.writeFileSync(installerPath, Buffer.concat(parts));
    } finally {
      clearInterval(installerStallWatchdog);
    }
    live.setup_note = 'installing the reading engine…';
    console.log('Installing Ollama silently…');
    const installArgs = ['/VERYSILENT', '/NORESTART', '/SP-'];
    if (drive.toUpperCase() !== 'C:') installArgs.push(`/DIR=${drive}\\R0iceResourceTeam\\Ollama`);
    await new Promise((resolve, reject) => {
      execFile(installerPath, installArgs, { timeout: 10 * 60 * 1000 }, err => err ? reject(err) : resolve());
    });
    live.setup_note = 'starting the reading engine…';
    if (!await waitForOllama(3 * 60 * 1000)) {
      // installer may not auto-start the service; launch `ollama serve` detached
      try {
        const child = spawn(ollamaCliPath(), ['serve'], { detached: true, stdio: 'ignore', windowsHide: true });
        child.unref();
      } catch {}
      if (!await waitForOllama(2 * 60 * 1000)) throw new Error('Ollama did not come up after install');
    }
    console.log('Ollama installed and running.');
    hw.has_ollama = true;
  }

  const wanted = modelForVram(hw.vram_gb);
  const have = await pickOllamaModel();
  if (have) return have;

  live.mode = 'setup';
  live.setup_note = `downloading the reading model (${wanted}) — one-time few-GB download…`;
  console.log(`No model present — pulling ${wanted}…`);
  // Streamed on purpose: a non-streaming /api/pull makes Ollama buffer the
  // WHOLE multi-GB download internally and send nothing back until it's
  // 100% done -- a long-lived, silent, zero-bytes-visible connection that
  // routers/AV/ISP middleboxes routinely kill for looking dead, with no
  // error surfaced anywhere (confirmed real-world failure: a volunteer's
  // pull sat frozen all night with no progress and no error). Streaming
  // gives real progress AND periodic traffic on the connection so it
  // doesn't look idle to anything watching it.
  const controller = new AbortController();
  let lastProgressAt = Date.now();
  const STALL_LIMIT_MS = 10 * 60 * 1000; // no progress for 10min -> treat as dead, not a slow-but-alive download
  const stallWatchdog = setInterval(() => {
    if (Date.now() - lastProgressAt > STALL_LIMIT_MS) controller.abort(new Error('model pull stalled -- no progress for 10 minutes'));
  }, 15 * 1000);
  try {
    const pull = await fetch('http://127.0.0.1:11434/api/pull', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ name: wanted, stream: true }),
      signal: controller.signal
    });
    if (!pull.ok || !pull.body) throw new Error(`model pull failed: ${pull.status}`);
    let buf = '';
    for await (const chunk of pull.body) {
      lastProgressAt = Date.now();
      buf += Buffer.isBuffer(chunk) ? chunk.toString('utf8') : chunk;
      let nl;
      while ((nl = buf.indexOf('\n')) >= 0) {
        const line = buf.slice(0, nl); buf = buf.slice(nl + 1);
        if (!line.trim()) continue;
        let evt; try { evt = JSON.parse(line); } catch { continue; }
        if (evt.error) throw new Error(`model pull failed: ${evt.error}`);
        if (evt.total && evt.completed) {
          const pct = Math.round((evt.completed / evt.total) * 100);
          const gb = (n) => (n / 1e9).toFixed(1);
          live.setup_note = `downloading the reading model (${wanted}) — ${gb(evt.completed)}/${gb(evt.total)} GB (${pct}%)…`;
        } else if (evt.status) {
          live.setup_note = `downloading the reading model (${wanted}) — ${evt.status}…`;
        }
      }
    }
  } finally {
    clearInterval(stallWatchdog);
  }
  console.log(`Model ${wanted} ready.`);
  return wanted;
}

// ---------------------------------------------------------------- ollama ----

const SUMMARY_PROMPT_PREFIX = 'Summarize this book excerpt in 3-5 sentences, focused on the key ideas and any actionable advice. Reply with ONLY the summary text itself -- do NOT begin with \'Here is a summary...\', \'Here\'s a summary...\', \'Sure,\', \'Certainly,\', \'Below is...\', or any other preamble or meta-commentary; start directly with the first substantive sentence:\n\n';
// leaked instruction-echo preamble the model sometimes emits despite being
// told not to (e.g. "Here is a summary of the book excerpt in 3-5 sentences:");
// defensive strip since the prompt-level instruction alone doesn't fully work
// on an 8B model. Mirrors cloud-gpu-worker.py's strip_preamble().
const PREAMBLE_RE = /^(here'?s|here is|sure,?|certainly,?|of course,?|below is)\b.*?:\s*\n+/i;
function stripPreamble(text) {
  return text.replace(PREAMBLE_RE, '');
}
const PREFERRED_MODELS = ['llama3.1:8b', 'llama3.1', 'llama3', 'mistral', 'qwen2.5', 'gemma2', 'phi3', 'llama3.2'];
const OLLAMA_GENERATE_TIMEOUT_MS = 4 * 60 * 1000;
const MAX_CHUNK_CHARS = 20000;

// Local corpus embedding (2026-07-22) -- escape hatch from Cloudflare Workers
// AI's shared 10k-neuron/day quota, which was blocking bulk chunk embedding
// same-day once hit. bge-m3 specifically (NOT nomic-embed-text) to match the
// existing CF-side model family/1024-dim so the two vector sets have a real
// chance of being comparable later -- confirmed via ChatGPT cross-check
// 2026-07-22 that mixing model families would create an incompatible second
// index. Piggybacks on the SAME stream-guarded chunk pipeline (this function
// only ever runs from inside workerLoop, which guard() already pauses/resumes
// with the rest of summarization) -- no separate job-queue type, no separate
// pause logic needed.
const EMBED_MODEL = 'bge-m3';
const OLLAMA_EMBED_TIMEOUT_MS = 60 * 1000;
let embedModelReady = false;

async function ensureEmbedModel() {
  try {
    const res = await fetch('http://127.0.0.1:11434/api/tags', { signal: AbortSignal.timeout(3000) });
    if (res.ok) {
      const { models } = await res.json();
      if (models && models.some(m => m.name === EMBED_MODEL || m.name.startsWith(EMBED_MODEL))) {
        embedModelReady = true;
        return;
      }
    }
  } catch { return; } // Ollama not reachable yet -- ensureOllamaAndModel() owns that failure path
  try {
    console.log(`pulling local embedding model (${EMBED_MODEL}, ~1.2GB, one-time)…`);
    const pull = await fetch('http://127.0.0.1:11434/api/pull', {
      method: 'POST', headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ name: EMBED_MODEL, stream: false }),
      signal: AbortSignal.timeout(10 * 60 * 1000)
    });
    if (pull.ok) { embedModelReady = true; console.log(`${EMBED_MODEL} ready -- chunks will now be embedded locally alongside summarization.`); }
  } catch (e) {
    // Non-fatal by design: embedding is a bonus on top of summarization, not a
    // requirement. A volunteer machine that can't pull bge-m3 just keeps
    // reading without local embeddings; CF/cron still backfills what it can.
    console.error(`local embed model unavailable (${e.message}) -- chunks will still be summarized, just not embedded locally.`);
  }
}

async function embedWithOllama(text) {
  const res = await fetch('http://127.0.0.1:11434/api/embeddings', {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ model: EMBED_MODEL, prompt: text.slice(0, MAX_CHUNK_CHARS) }),
    signal: AbortSignal.timeout(OLLAMA_EMBED_TIMEOUT_MS)
  });
  if (!res.ok) throw new Error(`ollama embeddings failed: ${res.status}`);
  const data = await res.json();
  if (!Array.isArray(data.embedding) || !data.embedding.length) throw new Error('empty embedding');
  return data.embedding;
}

const EMBED_BACKFILL_N = 32;

// Standalone embedding pass for already-summarized chunks that predate local
// embedding support. Only reached from claimMore() when both summarization
// passes are dry -- never competes with the primary job, just fills otherwise-
// idle GPU time. Coordinated with the Colab worker via the
// embedding_backfill_queue's atomic lease, so the two can never double-embed
// the same chunk. Returns true if it found (and attempted) work.
async function embedBackfillRound() {
  let claimed;
  try {
    const res = await fetch(`${API_BASE}/jobs/embed-claim?target=local_bge_m3&n=${EMBED_BACKFILL_N}`, {
      headers: { Authorization: `Bearer ${activeToken}` },
      signal: AbortSignal.timeout(20 * 1000)
    });
    claimed = await res.json();
  } catch (e) {
    console.error('embed-claim failed:', e.message);
    return false;
  }
  const items = claimed.items || [];
  if (!items.length) return false;

  const results = [];
  for (const it of items) {
    try {
      const embedding = await embedWithOllama(it.text);
      results.push({ target: 'local_bge_m3', chunk_id: it.chunk_id, embedding, embedding_model: EMBED_MODEL });
    } catch (e) {
      results.push({ target: 'local_bge_m3', chunk_id: it.chunk_id, error: e.message });
    }
  }
  try {
    await fetch(`${API_BASE}/jobs/embed-result`, {
      method: 'POST',
      headers: { Authorization: `Bearer ${activeToken}`, 'content-type': 'application/json' },
      body: JSON.stringify(results),
      signal: AbortSignal.timeout(20 * 1000)
    });
  } catch (e) {
    console.error('embed-result submit failed (results lost for this batch):', e.message);
  }
  const ok = results.filter(r => r.embedding).length;
  console.log(`embed-backfill: submitted ${results.length} (${ok} ok) from the standalone queue`);
  return true;
}

async function pickOllamaModel() {
  try {
    const res = await fetch('http://127.0.0.1:11434/api/tags', { signal: AbortSignal.timeout(3000) });
    if (!res.ok) { console.log(`pickOllamaModel: /api/tags returned ${res.status}`); return null; }
    const { models } = await res.json();
    if (!models || !models.length) { console.log('pickOllamaModel: Ollama reachable but no models installed yet'); return null; }
    const names = models.map(m => m.name);
    for (const pref of PREFERRED_MODELS) {
      const hit = names.find(n => n === pref || n.startsWith(pref));
      if (hit) return hit;
    }
    return names[0];
  } catch (e) { console.log(`pickOllamaModel: /api/tags unreachable (${e.message})`); return null; }
}

async function summarizeWithOllama(model, text, promptPrefix, jsonMode) {
  const body = { model, prompt: (promptPrefix || SUMMARY_PROMPT_PREFIX) + text.slice(0, MAX_CHUNK_CHARS), stream: false };
  if (jsonMode) body.format = 'json';
  const res = await fetch('http://127.0.0.1:11434/api/generate', {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(body),
    signal: AbortSignal.timeout(OLLAMA_GENERATE_TIMEOUT_MS)
  });
  if (!res.ok) throw new Error(`ollama generate failed: ${res.status}`);
  const data = await res.json();
  const out = (data.response || '').trim();
  return jsonMode ? out : stripPreamble(out);
}

const DEEP_PROMPT = 'Find the single most surprising, useful, or memorable passage in this book excerpt. Copy it out VERBATIM -- exact wording, character for character, no paraphrasing, no trimming mid-sentence. Then in 1-2 sentences state the specific idea or claim that makes it worth reading -- name the actual content directly. Do NOT open with stock phrases like \'this passage highlights\', \'this quote matters because\', or \'this passage reveals\'. Reply with ONLY this JSON object, nothing else:\n{"quote": "...", "insight": "..."}\n\nExcerpt:\n\n';

const TECHNIQUE_PROMPT = 'Look for a concrete, actionable technique, method, or step-by-step procedure in this book excerpt -- something a reader could actually DO (a specific practice, recipe step, physical technique, exercise, or method), not a general idea or opinion. Most excerpts won\'t have one; that is fine and expected.\n\nIf you find one, reply with ONLY this JSON:\n{"found": true, "title": "short name for the technique", "steps": ["step 1", "step 2"], "cautions": "any warning or common mistake mentioned, or empty string", "source_quote": "a short VERBATIM excerpt from the text that grounds this technique"}\n\nIf there is no concrete technique/procedure in this excerpt, reply with ONLY:\n{"found": false}\n\nRules: source_quote must be copied character-for-character from the excerpt, no paraphrasing. Steps must be concrete and imperative. Do not invent steps not actually present in the text.\n\nExcerpt:\n\n';

// ----------------------------------------------------------------- pipeline ----

const MIN_VRAM_GB = 6;
const CLAIM_N = 5;
const SUBMIT_BATCH = 5;
const DRY_BACKOFF_MS = 5 * 60 * 1000;
// 1 in DEEP_EVERY claim cycles goes to the second-pass (best-quote) queue
// regardless of first-pass depth -- that queue is otherwise never empty, so a
// fallback-only trigger (try deep only when first-pass returns zero) never
// actually fires in practice. Mirrors cloud-gpu-worker.py's same rotation.
const DEEP_EVERY = 4;
// 1 in TECHNIQUE_EVERY cycles goes to the technique-extraction queue (server-
// scoped to health_medicine/practical_skills/sexuality_relationships books --
// a much smaller pool than the full corpus, so it gets a smaller share than
// DEEP_EVERY). Checked before DEEP_EVERY in claimMore() so the two never both
// fire on the same cycle.
const TECHNIQUE_EVERY = 8;

function makePipeline(model, opts) {
  const bufferTarget = Math.max(CLAIM_N, opts.bufferTarget || 12);
  const state = {
    queue: [], outbox: [], deepOutbox: [], techniqueOutbox: [], attemptsOutbox: [], claimInFlight: false, dryUntil: 0, stopped: false,
    done: 0, errors: 0, submitted: 0, recent: [], claimCycle: 0,
    submitInFlight: false, submitFailStreak: 0, submitBackoffUntil: 0
  };

  async function claimType(type) {
    // No AbortSignal here meant a stalled connection would wedge claimMore()
    // forever (claimInFlight never clears, since nothing ever rejects) --
    // the ongoing-reading-loop instance of the same no-timeout bug class.
    // claimMore()'s surrounding try/catch already handles a thrown abort.
    // 30s (was 20s) to match cloud-gpu-worker.py's claim timeout -- the VPS
    // can stall this long under swap pressure without it being a real hang
    // (confirmed 2026-08-02: a cold request to yt-mine-web took 3.5s+ just
    // from the process getting paged back in, worse under real contention).
    const res = await fetch(`${API_BASE}/jobs/claim?type=${type}&n=${CLAIM_N}`, {
      headers: { Authorization: `Bearer ${activeToken}` },
      signal: AbortSignal.timeout(30 * 1000)
    });
    if (res.status === 401) {
      // Same rejection signal as reportOnce's census 401 (claim runs far more
      // often -- every few seconds -- so it's usually the first to notice a
      // superseded token). Additive: reuses the same live.token_rejected*
      // fields and the same maybeRejoin() sustained-rejection check.
      if (!live.token_rejected) {
        console.error(`[FATAL] TOKEN REJECTED (401) on claim -- config at ${CONFIG_PATH} holds a token the server no longer accepts (superseded/revoked). This worker will keep running but cannot report census or claim work until config.json is fixed and the process is restarted. See WHICH_CONFIG.txt next to this file before editing config.json.`);
        live.token_rejected_since = new Date().toISOString();
      }
      live.token_rejected = true;
      live.token_rejected_at = new Date().toISOString();
      await maybeRejoin();
      return { items: [] };
    }
    if (!res.ok) {
      // Added 2026-08-07: this used to return {items:[]} on ANY non-ok status
      // (a 502 while yt-mine-web is mid-flap included), which is indistinguishable
      // from "the queue is genuinely empty" -- claimMore() would then hit the
      // 5-minute DRY_BACKOFF_MS for what was actually a transient server error,
      // stalling a worker with 100k+ real pending chunks waiting. Throwing here
      // instead routes it through claimMore()'s catch block, which sets no
      // backoff at all -- the next claimMore() tick (every 3s) just retries.
      throw new Error(`claim http ${res.status}`);
    }
    return res.json();
  }

  async function claimMore() {
    if (state.claimInFlight || state.stopped) return;
    if (Date.now() < state.dryUntil) return;
    if (state.queue.length >= bufferTarget) return;
    state.claimInFlight = true;
    try {
      state.claimCycle++;
      const wantTechnique = (state.claimCycle % TECHNIQUE_EVERY === 0);
      const wantDeep = !wantTechnique && (state.claimCycle % DEEP_EVERY === 0);
      const primaryType = wantTechnique ? 'book-chunk-technique' : wantDeep ? 'book-chunk-deep' : 'book-chunk';
      let r = await claimType(primaryType);
      if (r.reason === 'insufficient_hardware') {
        // server hasn't seen this machine's census yet (fresh install / Ollama
        // still starting) — that's "warming up", NOT an empty library
        state.dryUntil = Date.now() + 60 * 1000;
        if (!state.queue.length) live.mode = 'warming';
        return;
      }
      let deep = wantDeep, technique = wantTechnique;
      if (!r.items || !r.items.length) {
        // scheduled type came back empty -- fall back to plain book-chunk
        // (essentially never empty) unless that WAS the scheduled type, in
        // which case try deep instead. Same reasoning as before: don't burn
        // the cycle, and don't let a scoped queue (technique/deep) stay
        // starved just because it happened to be the pick this round.
        const fallbackType = primaryType === 'book-chunk' ? 'book-chunk-deep' : 'book-chunk';
        r = await claimType(fallbackType);
        deep = fallbackType === 'book-chunk-deep';
        technique = false;
      }
      if (r.items && r.items.length) {
        for (const it of r.items) state.queue.push({ ...it, deep, technique, claimedAt: Date.now() });
        if (live.mode === 'dry' || live.mode === 'idle' || live.mode === 'warming') live.mode = 'reading';
      } else {
        // Both summarization passes dry -- fill otherwise-idle GPU time with the
        // standalone embed-backfill queue (already-summarized chunks missing a
        // local vector) instead of just sleeping. Coordinated with Colab via the
        // embedding_backfill_queue's atomic lease, so the two can never double-embed
        // the same chunk. Short backoff on success so it keeps draining the backlog
        // quickly; falls back to the full DRY_BACKOFF_MS only once that's dry too.
        const embedded = embedModelReady ? await embedBackfillRound() : false;
        state.dryUntil = Date.now() + (embedded ? 5 * 1000 : DRY_BACKOFF_MS);
        if (!embedded) {
          if (!state.queue.length && live.mode === 'reading') live.mode = 'dry';
          console.log(`queue dry (summarize + embed-backfill) — backing off ${DRY_BACKOFF_MS / 60000} min`);
        }
      }
    } catch (e) {
      console.error('claim failed:', e.message);
    } finally {
      state.claimInFlight = false;
    }
  }

  async function flushOne(box, endpoint) {
    const batch = box.splice(0, SUBMIT_BATCH);
    if (!batch.length) return true;
    try {
      // 35s (was 20s) -- the VPS can stall this long under swap pressure
      // (confirmed 2026-08-02: cold requests to yt-mine-web cost 3.5s+ just
      // paging the process back in, worse under real contention) without it
      // being a genuine hang, so a short abort was firing false failures.
      const res = await fetch(`${API_BASE}/jobs/${endpoint}`, {
        method: 'POST',
        headers: { Authorization: `Bearer ${activeToken}`, 'content-type': 'application/json' },
        body: JSON.stringify(batch),
        signal: AbortSignal.timeout(35 * 1000)
      });
      if (res.ok) { state.submitted += batch.length; live.session.submitted = state.submitted; return true; }
      box.unshift(...batch);
      return false;
    } catch (e) {
      box.unshift(...batch);
      console.error('submit failed (will retry):', e.message);
      return false;
    }
  }

  async function flushAttempts() {
    // Best-effort telemetry -- deliberately does NOT touch state.submitted
    // (that counter feeds the "chunks submitted" status display and must only
    // reflect real book-chunk-result submissions, not attempt-log calls).
    const batch = state.attemptsOutbox.splice(0, SUBMIT_BATCH);
    if (!batch.length) return;
    try {
      await fetch(`${API_BASE}/jobs/attempt-log`, {
        method: 'POST',
        headers: { Authorization: `Bearer ${activeToken}`, 'content-type': 'application/json' },
        body: JSON.stringify(batch),
        signal: AbortSignal.timeout(20 * 1000)
      });
    } catch (e) {
      console.error('attempt-log submit failed (non-fatal):', e.message);
      // Not re-queued on purpose -- telemetry, not a job result. A lost batch
      // just means a gap in the throughput graph, not lost work.
    }
  }

  async function flushOutbox() {
    // Guard against overlap: the caller re-invokes this every 4s regardless
    // of whether the previous call finished. With a 35s abort timeout that
    // used to mean up to ~9 concurrent flushes could stack up during a
    // sustained VPS stall, multiplying load on the exact server that's
    // already struggling. Also backs off after repeated failures instead of
    // retrying every 4s into a server that's clearly still down.
    if (state.submitInFlight) return;
    if (Date.now() < state.submitBackoffUntil) return;
    state.submitInFlight = true;
    try {
      const okA = await flushOne(state.outbox, 'book-chunk-result');
      const okB = await flushOne(state.deepOutbox, 'book-deep-result');
      const okC = await flushOne(state.techniqueOutbox, 'technique-result');
      await flushAttempts();
      if (okA && okB && okC) {
        state.submitFailStreak = 0;
      } else {
        state.submitFailStreak++;
        // 8s, 16s, 32s, capped at 60s -- resets to 0 backoff on the next success.
        const backoffMs = Math.min(60000, 4000 * Math.pow(2, state.submitFailStreak));
        state.submitBackoffUntil = Date.now() + backoffMs;
      }
    } finally {
      state.submitInFlight = false;
    }
  }

  async function workerLoop(id) {
    while (!state.stopped) {
      const chunk = state.queue.shift();
      if (!chunk) { await new Promise(r => setTimeout(r, 500)); continue; }
      claimMore();
      const cur = { title: (chunk.title || 'a public-domain book') + (chunk.deep ? ' · second read' : chunk.technique ? ' · technique scan' : ''), author: chunk.author || '', chunk: (chunk.chunk_index ?? 0) + 1, of: chunk.total_chunks || null, started: Date.now(), wid: id };
      live.current = live.current.filter(c => c.wid !== id).concat(cur);
      const t0 = Date.now();
      try {
        let summary;
        if (chunk.technique) {
          // third pass: hunt a concrete how-to/procedure -- same JSON-mode +
          // server-side verbatim-verification trust model as the deep pass.
          const raw = await summarizeWithOllama(model, chunk.text, TECHNIQUE_PROMPT, true);
          let result = { chunk_id: chunk.id, found: false };
          try {
            const parsed = JSON.parse(raw);
            if (parsed.found) {
              result = { chunk_id: chunk.id, found: true, title: String(parsed.title || '').trim(),
                steps: Array.isArray(parsed.steps) ? parsed.steps.map(s => String(s).trim()).filter(Boolean) : [],
                cautions: String(parsed.cautions || '').trim(), source_quote: String(parsed.source_quote || '').trim() };
            }
          } catch { /* leave found:false -- server marks the chunk done either way */ }
          state.techniqueOutbox.push(result);
          summary = result.found ? `"${result.title}"` : '(no technique found)';
        } else if (chunk.deep) {
          // second pass: hunt the single most valuable VERBATIM passage — the
          // server verifies the quote against the source, so exactness matters.
          // JSON mode (not free-text QUOTE:/WHY: + regex) so a model deviating
          // from the template doesn't silently produce an unparseable response —
          // this matters most on exactly the low-VRAM cards that fall back to a
          // weaker model, since those are also the ones most likely to deviate.
          const raw = await summarizeWithOllama(model, chunk.text, DEEP_PROMPT, true);
          let quote = '', insight = '';
          try {
            const parsed = JSON.parse(raw);
            quote = String(parsed.quote || '').trim();
            insight = String(parsed.insight || '').trim();
          } catch { /* leave both empty -- server marks the chunk done either way */ }
          state.deepOutbox.push({ chunk_id: chunk.id, quote, insight });
          summary = quote ? `"${quote.slice(0, 80)}…"` : '(no passage found)';
        } else {
          summary = await summarizeWithOllama(model, chunk.text);
        }
        const gen_ms = Date.now() - t0;
        if (!chunk.deep && !chunk.technique) state.outbox.push({ chunk_id: chunk.id, summary });
        state.done++;
        live.session.chunks = state.done;
        live.today.chunks++;
        live.today.words += Math.round((chunk.text || '').length / 5.5);
        live.recent.unshift({ title: cur.title, chunk: cur.chunk, of: cur.of, secs: +(gen_ms / 1000).toFixed(1), at: new Date().toISOString() });
        live.recent = live.recent.slice(0, 10);
        const rec = { t: Date.now(), gen_ms, queue_ms: t0 - chunk.claimedAt, chars_in: (chunk.text || '').length, chars_out: summary.length };
        state.recent.push(rec);
        const cutoff = Date.now() - 15 * 60 * 1000;
        while (state.recent.length && state.recent[0].t < cutoff) state.recent.shift();
        try { fs.appendFileSync(METRICS_PATH, JSON.stringify({ id: chunk.id, worker: id, model, ...rec }) + '\n'); } catch {}
        console.log(`[book-chunk] ${chunk.id} "${cur.title}" ${cur.chunk}/${cur.of} done in ${(gen_ms / 1000).toFixed(1)}s [w${id}]`);
        // Fleet-wide throughput telemetry (2026-07-22) -- batched onto the same
        // flush cadence as outbox/deepOutbox, never a per-chunk HTTP call.
        state.attemptsOutbox.push({ job_type: chunk.technique ? 'technique' : chunk.deep ? 'deep' : 'summarize', model_name: model,
          input_chars: rec.chars_in, output_chars: rec.chars_out, queue_wait_ms: rec.queue_ms, inference_ms: rec.gen_ms, status: 'ok' });
      } catch (e) {
        state.errors++;
        live.session.errors = state.errors;
        if (!chunk.deep && !chunk.technique) state.outbox.push({ chunk_id: chunk.id, status: 'error' });
        state.attemptsOutbox.push({ job_type: chunk.technique ? 'technique' : chunk.deep ? 'deep' : 'summarize', model_name: model,
          input_chars: (chunk.text || '').length, queue_wait_ms: Date.now() - chunk.claimedAt, inference_ms: Date.now() - t0,
          status: 'error', error_text: String(e.message || e).slice(0, 200) });
        console.error(`[book-chunk] ${chunk.id} failed:`, e.message);
      } finally {
        live.current = live.current.filter(c => c.wid !== id);
      }
    }
  }

  const timers = [];
  function start(concurrency) {
    claimMore();
    timers.push(setInterval(claimMore, 3000));
    timers.push(setInterval(flushOutbox, 4000));
    for (let i = 1; i <= concurrency; i++) workerLoop(i);
  }
  async function stop() {
    state.stopped = true;
    for (const t of timers) clearInterval(t);
    state.queue = []; // unprocessed claims requeue server-side after 20 min
    while (state.outbox.length || state.deepOutbox.length || state.techniqueOutbox.length) await flushOutbox();
    live.current = [];
  }

  function windowStats(windowMs) {
    const cutoff = Date.now() - windowMs;
    const win = state.recent.filter(r => r.t >= cutoff);
    if (!win.length) return { chunks: 0, per_hour: 0, avg_gen_s: 0 };
    const span = Math.max(Date.now() - Math.max(cutoff, win[0].t), 60 * 1000);
    return {
      chunks: win.length,
      per_hour: +(win.length / (span / 3600000)).toFixed(1),
      avg_gen_s: +(win.reduce((a, r) => a + r.gen_ms, 0) / win.length / 1000).toFixed(1)
    };
  }

  return { state, start, stop, windowStats };
}

// ------------------------------------------------------------ stats writer ----

function startStatsReporter(getPipeline, concurrency) {
  let lastPrint = 0;
  return setInterval(() => {
    const pipeline = getPipeline();
    if (!pipeline) return;
    const w = pipeline.windowStats(10 * 60 * 1000);
    const g = gpuWindowStats(10 * 60 * 1000);
    const snap = {
      t: new Date().toISOString(), mode: live.mode, concurrency,
      queue_depth: pipeline.state.queue.length, outbox: pipeline.state.outbox.length,
      done_total: pipeline.state.done, submitted_total: pipeline.state.submitted,
      errors_total: pipeline.state.errors, last10min: w, gpu_last10min: g
    };
    try { fs.writeFileSync(STATS_PATH, JSON.stringify(snap, null, 2)); } catch {}
    if (Date.now() - lastPrint >= 60 * 1000) {
      lastPrint = Date.now();
      console.log(`[stats] ${w.per_hour} chunks/hr (10min), avg gen ${w.avg_gen_s}s, queue ${snap.queue_depth}, ` +
        (g ? `GPU ${g.util_avg}% avg / ${g.idle_pct}% idle` : 'GPU n/a') +
        `, total ${snap.done_total}, errors ${snap.errors_total}`);
    }
  }, 30 * 1000);
}

// ------------------------------------------------------------ status window ----

function statusPage() {
  return `<!doctype html>
<html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>R0ice Resource Team — your GPU is reading books</title>
<link href="https://fonts.googleapis.com/css2?family=Fraunces:ital,wght@0,500;1,500&family=Public+Sans:wght@400;500;600&display=swap" rel="stylesheet">
<style>
:root { --bg:#100d0a; --ember:#ff8a3d; --gold:#d9a55c; --text:#f2e9dd; --dim:#b5a996; --mute:#7d7263; --line:#2c2117; --card:#1a140dcc; --green:#8fbf6f; }
*{box-sizing:border-box} body{margin:0;background:var(--bg);color:var(--text);font-family:'Public Sans',sans-serif;padding:0 20px 60px}
.wrap{max-width:640px;margin:0 auto} h1{font-family:'Fraunces',serif;font-style:italic;font-weight:500;font-size:28px;margin:40px 0 4px}
.tag{color:var(--dim);margin-bottom:26px;font-size:14px;line-height:1.5}
.mode{display:inline-block;padding:5px 12px;border-radius:20px;font-size:12px;font-weight:600;margin-bottom:20px}
.mode.reading{background:#20301b;color:var(--green)} .mode.paused{background:#332014;color:var(--ember)} .mode.setup{background:#2a2416;color:var(--gold)} .mode.idle{background:#221d15;color:var(--mute)}
.mode.rejected{background:#3a1414;color:#ff5c5c;font-weight:700}
.dot{display:inline-block;width:8px;height:8px;border-radius:50%;background:var(--green);margin-right:7px;animation:pulse 1.6s infinite;vertical-align:1px}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.25}}
.card{background:var(--card);border:1px solid var(--line);border-radius:12px;padding:16px 18px;margin-bottom:12px}
.book{font-family:'Fraunces',serif;font-style:italic;font-size:20px} .sub{color:var(--mute);font-size:12px;margin-top:4px}
.stats{display:flex;gap:10px;flex-wrap:wrap;margin:16px 0}
.stat{flex:1;min-width:100px;background:var(--card);border:1px solid var(--line);border-radius:10px;padding:12px 14px}
.stat .n{font-family:'Fraunces',serif;font-size:22px;color:var(--ember)} .stat .l{font-size:10px;color:var(--mute);text-transform:uppercase;letter-spacing:.06em}
h2{font-family:'Fraunces',serif;font-style:italic;font-weight:500;font-size:17px;color:var(--gold);margin:26px 0 10px}
.feed .fi{padding:8px 0;border-bottom:1px solid var(--line);font-size:13px;color:var(--dim)} .feed .fi:last-child{border-bottom:none}
.feed .bk{font-style:italic;color:var(--text)} .feed .t{float:right;color:var(--mute);font-size:11px}
.note{color:var(--mute);font-size:12px;line-height:1.6;margin-top:26px;border-top:1px solid var(--line);padding-top:14px}
.pbtn{padding:9px 16px;margin-right:6px;margin-bottom:4px;background:#241b10;border:1px solid var(--line);border-radius:9px;color:var(--dim);font-family:inherit;font-size:13px;cursor:pointer}
.pbtn:hover{border-color:var(--ember);color:var(--ember)}
</style></head><body><div class="wrap">
<h1>your graphics card is reading books</h1>
<p class="tag">This helper uses your idle GPU to read public-domain classics — Aristotle, Marcus Aurelius, Ben Franklin — one chapter-sized piece at a time, and sends back a short summary to the R0ice community library. Nothing else. It pauses itself the moment your machine needs the GPU back.</p>
<div id="mode" class="mode idle">starting…</div>
<div class="card" id="pausecard">
  <div id="pauserow">
    <div style="color:#b5a996;font-size:13px;margin-bottom:8px">need your GPU? pause any time — nothing bad happens, it just stops reading.</div>
    <button class="pbtn" onclick="doPause(60)">pause 1 hour</button>
    <button class="pbtn" onclick="doPause(240)">pause 4 hours</button>
    <button class="pbtn" onclick="doPause(0)">pause until I turn it back on</button>
  </div>
  <div id="resumerow" style="display:none">
    <div id="pausedmsg" style="color:#ff8a3d;font-size:13px;margin-bottom:8px">reading is paused.</div>
    <button class="pbtn" style="border-color:#8fbf6f;color:#8fbf6f;font-size:15px;padding:12px 22px" onclick="doResume()">▶ resume reading</button>
  </div>
</div>
<div id="drivepick" style="display:none" class="card">
  <div class="book" style="font-size:17px">where should the reading engine live?</div>
  <div class="sub" style="margin-bottom:10px">the reading model is a few GB — pick the drive with room to spare.</div>
  <div id="drives"></div>
</div>
<div id="current"></div>
<div class="stats">
  <div class="stat"><div class="n" id="tChunks">0</div><div class="l">pieces read today</div></div>
  <div class="stat"><div class="n" id="tWords">0</div><div class="l">words read today</div></div>
  <div class="stat"><div class="n" id="tPages">0</div><div class="l">pages today</div></div>
</div>
<h2>you just read</h2>
<div class="feed card" id="mine"></div>
<h2>the whole team, live</h2>
<div class="feed card" id="team"></div>
<div class="note">Run by R0ice for the community library at r0ice.com. Your machine only ever receives pieces of public-domain books and returns short summaries. Pause or quit any time from the tray icon by your clock.</div>
</div>
<script>
const esc = s => String(s ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
function ago(ts){ if(!ts) return ''; const raw = ts.includes('T') ? ts : ts.replace(' ','T')+'Z'; const d=(Date.now()-new Date(raw))/1000; if(d<90) return Math.max(1,Math.round(d))+'s ago'; if(d<5400) return Math.round(d/60)+'m ago'; return Math.round(d/3600)+'h ago'; }
function el(tag, css, text){ const e=document.createElement(tag); if(css) e.style.cssText=css; if(text!=null) e.textContent=text; return e; }
function doPause(minutes){ fetch('/pause',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({minutes})}).then(tick); }
function doResume(){ fetch('/resume',{method:'POST'}).then(tick); }
function renderPause(s){
  const paused = s.mode === 'paused-user';
  document.getElementById('pauserow').style.display = paused ? 'none' : 'block';
  document.getElementById('resumerow').style.display = paused ? 'block' : 'none';
  if (paused) {
    document.getElementById('pausedmsg').textContent = s.paused_until
      ? 'reading is paused until ' + new Date(s.paused_until).toLocaleTimeString([], {hour:'numeric', minute:'2-digit'}) + ' — or resume now:'
      : 'reading is paused until you turn it back on.';
  }
}
function renderSetup(s){
  const dp = document.getElementById('drivepick');
  const dl = document.getElementById('drives');
  if (s.mode === 'setup-choice' && s.drives) {
    dp.style.display = 'block';
    dl.replaceChildren(...s.drives.map(d => {
      const label = d.mount + ' — ' + (d.free_gb != null ? d.free_gb + ' GB free of ' + d.total_gb + ' GB' : 'unknown space');
      const b = el('button', 'display:block;width:100%;text-align:left;margin:6px 0;padding:12px 14px;background:#241b10;border:1px solid #2c2117;border-radius:10px;color:#f2e9dd;font-family:inherit;font-size:14px;cursor:pointer', label);
      b.onclick = () => { fetch('/setup/drive', { method:'POST', headers:{'content-type':'application/json'}, body: JSON.stringify({ mount: d.mount }) }); dp.style.display='none'; };
      return b;
    }));
  } else dp.style.display = 'none';
  if (s.autostart === false && s.mode !== 'setup-choice' && !document.getElementById('autostart-row')) {
    const row = el('div'); row.id = 'autostart-row'; row.className = 'card';
    row.append(el('div', 'color:#7d7263;font-size:12px', 'want this to start reading automatically when the PC turns on?'));
    const b = el('button', 'margin-top:8px;padding:8px 14px;background:#241b10;border:1px solid #2c2117;border-radius:8px;color:#d9a55c;font-family:inherit;cursor:pointer', 'yes, start with Windows');
    b.onclick = () => fetch('/setup/autostart', { method:'POST' }).then(() => row.remove());
    row.append(b);
    document.getElementById('current').before(row);
  }
}
async function tick(){
  try {
    const s = await fetch('/state').then(r=>r.json());
    const modeEl = document.getElementById('mode');
    const labels = { reading:'<span class="dot"></span>reading', idle: s.setup_note ? esc(s.setup_note) : 'waiting for work', warming:'warming up — the library is checking your hardware, reading starts in a minute', dry:'every book is read — waiting for new arrivals', 'paused-user':'paused by you', 'paused-stream':'paused — the stream has the GPU', 'paused-quiet':'paused — daily stream block, GPU stays cold', 'paused-gpu':'paused — something else is using the GPU', setup:'setting up: '+esc(s.setup_note||''), 'setup-choice':'one quick question below', starting:'starting…' };
    if (s.token_rejected) {
      // Loud and unmissable on purpose: a superseded/revoked token otherwise
      // looks identical to a healthy idle worker on this page. Plain text
      // only (no server-derived values interpolated), so textContent is enough.
      modeEl.textContent = 'STATUS: REJECTED — token invalid (401). This worker cannot report census or claim work. Fix config.json and restart.';
      modeEl.className = 'mode rejected';
    } else {
      modeEl.innerHTML = labels[s.mode] || esc(s.mode);
      modeEl.className = 'mode ' + (s.mode==='reading' ? 'reading' : s.mode.startsWith('paused') ? 'paused' : s.mode.startsWith('setup') ? 'setup' : 'idle');
    }
    renderSetup(s);
    renderPause(s);
    document.getElementById('current').innerHTML = (s.current||[]).map(c=>\`<div class="card"><div class="book">\${esc(c.title)}</div><div class="sub">piece \${c.chunk}\${c.of?' of '+c.of:''} \${c.author?'— '+esc(c.author):''}</div></div>\`).join('');
    document.getElementById('tChunks').textContent = s.today.chunks.toLocaleString();
    document.getElementById('tWords').textContent = s.today.words.toLocaleString();
    document.getElementById('tPages').textContent = Math.round(s.today.words/275).toLocaleString();
    document.getElementById('mine').innerHTML = (s.recent||[]).length ? s.recent.map(r=>\`<div class="fi"><span class="t">\${ago(r.at)}</span>piece \${r.chunk}\${r.of?'/'+r.of:''} of <span class="bk">\${esc(r.title)}</span> in \${r.secs}s</div>\`).join('') : '<div class="fi">nothing yet this session</div>';
  } catch {}
  try {
    const f = await fetch('${API_BASE}/jobs/farm-live').then(r=>r.json());
    document.getElementById('team').innerHTML = (f.recent||[]).slice(0,10).map(r=>\`<div class="fi"><span class="t">\${ago(r.at)}</span>\${esc(r.worker)} read piece \${r.chunk}/\${r.of} of <span class="bk">\${esc(r.title)}</span></div>\`).join('') || '<div class="fi">quiet right now</div>';
  } catch {}
}
tick(); setInterval(tick, 4000);
</script></body></html>`;
}

function installAutostart() {
  const startup = path.join(process.env.APPDATA || '', 'Microsoft', 'Windows', 'Start Menu', 'Programs', 'Startup');
  const ps = `$s=(New-Object -ComObject WScript.Shell).CreateShortcut('${path.join(startup, 'R0ice Resource Team.lnk')}');$s.TargetPath='${process.execPath}';$s.WorkingDirectory='${path.dirname(process.execPath)}';$s.Save()`;
  return new Promise(resolve => {
    execFile('powershell.exe', ['-NoProfile', '-NonInteractive', '-Command', ps], { timeout: 15000, windowsHide: true }, err => {
      if (err) console.log('autostart install failed:', err.message);
      else console.log('autostart installed');
      resolve(!err);
    });
  });
}

function hasAutostart() {
  try {
    return fs.existsSync(path.join(process.env.APPDATA || '', 'Microsoft', 'Windows', 'Start Menu', 'Programs', 'Startup', 'R0ice Resource Team.lnk'));
  } catch { return false; }
}

function startStatusServer() {
  const server = http.createServer((req, res) => {
    if (req.url === '/state') {
      res.writeHead(200, { 'content-type': 'application/json' });
      return res.end(JSON.stringify({ ...live, autostart: hasAutostart() }));
    }
    if (req.url === '/setup/drive' && req.method === 'POST') {
      let body = '';
      req.on('data', c => { body += c; if (body.length > 1000) req.destroy(); });
      req.on('end', () => {
        let mount = null;
        try { mount = JSON.parse(body).mount; } catch {}
        if (mount && /^[A-Z]:$/i.test(mount) && driveChoiceResolve) {
          driveChoiceResolve(mount.toUpperCase());
          res.writeHead(200, { 'content-type': 'application/json' });
          return res.end('{"ok":true}');
        }
        res.writeHead(400, { 'content-type': 'application/json' });
        return res.end('{"ok":false}');
      });
      return;
    }
    if (req.url === '/setup/autostart' && req.method === 'POST') {
      installAutostart().then(ok => {
        res.writeHead(200, { 'content-type': 'application/json' });
        res.end(JSON.stringify({ ok }));
      });
      return;
    }
    if (req.url === '/pause' && req.method === 'POST') {
      let body = '';
      req.on('data', c => { body += c; if (body.length > 1000) req.destroy(); });
      req.on('end', () => {
        let minutes = 0;
        try { minutes = Number(JSON.parse(body).minutes) || 0; } catch {}
        const cfg = loadConfig();
        if (minutes > 0) { cfg.pausedUntil = Date.now() + Math.min(minutes, 24 * 60) * 60000; cfg.paused = false; }
        else { cfg.paused = true; cfg.pausedUntil = null; }
        saveConfig(cfg);
        console.log(minutes > 0 ? `paused for ${minutes} min via status window` : 'paused until resumed via status window');
        res.writeHead(200, { 'content-type': 'application/json' });
        res.end('{"ok":true}');
      });
      return;
    }
    if (req.url === '/resume' && req.method === 'POST') {
      const cfg = loadConfig();
      cfg.paused = false; cfg.pausedUntil = null;
      saveConfig(cfg);
      console.log('resumed via status window');
      res.writeHead(200, { 'content-type': 'application/json' });
      return res.end('{"ok":true}');
    }
    res.writeHead(200, { 'content-type': 'text/html; charset=utf-8' });
    res.end(statusPage());
  });
  server.on('error', e => console.log('status server error:', e.message));
  server.listen(STATUS_PORT, '127.0.0.1', () => console.log(`Status window: http://127.0.0.1:${STATUS_PORT}`));
}

function openStatusWindow() {
  const child = spawn('cmd.exe', ['/c', 'start', '', `http://127.0.0.1:${STATUS_PORT}`], { detached: true, stdio: 'ignore', windowsHide: true });
  child.unref();
}

// -------------------------------------------------------------------- tray ----

function setupTray(state) {
  const systray = new SysTray({
    menu: {
      icon: TRAY_ICON_B64,
      title: 'R0ice Resource Team',
      tooltip: 'R0ice Resource Team — your GPU reads books for the community library',
      items: [
        { title: 'Open status window', tooltip: '', checked: false, enabled: true },
        { title: state.paused ? 'Paused (click to resume)' : 'Active (click to pause)', tooltip: '', checked: false, enabled: true },
        { title: 'Quit (stays set up for next login)', tooltip: '', checked: false, enabled: true },
        { title: 'Stop for good (removes auto-start)', tooltip: '', checked: false, enabled: true }
      ]
    },
    debug: false,
    copyDir: true
  });
  systray.onClick(action => {
    if (action.seq_id === 0) {
      openStatusWindow();
    } else if (action.seq_id === 1) {
      state.paused = !state.paused;
      saveConfig({ ...loadConfig(), paused: state.paused });
      systray.sendAction({
        type: 'update-item',
        item: { ...action.item, title: state.paused ? 'Paused (click to resume)' : 'Active (click to pause)' },
        seq_id: 1
      });
      console.log(state.paused ? 'Paused by user.' : 'Resumed by user.');
    } else if (action.seq_id === 2) {
      systray.kill();
      process.exit(0);
    } else if (action.seq_id === 3) {
      removeAutoStart();
      try { fs.unlinkSync(CONFIG_PATH); } catch {}
      systray.kill();
      process.exit(0);
    }
  });
  return systray;
}

// Undoes both persistence mechanisms the installer sets up (Startup-folder
// shortcut for reboots, Scheduled Task watchdog for same-session crashes) --
// this is the tray's one-click equivalent of running STOP.bat, so "make it
// stop for good" is never more than a right-click away, not buried in an
// AppData folder the person has to know to find.
function removeAutoStart() {
  try {
    const shortcutPath = path.join(process.env.APPDATA || '', 'Microsoft', 'Windows', 'Start Menu', 'Programs', 'Startup', 'R0ice Resource Team.lnk');
    if (fs.existsSync(shortcutPath)) fs.unlinkSync(shortcutPath);
  } catch (e) { console.log('could not remove startup shortcut:', e.message); }
  try {
    execFileSync('schtasks.exe', ['/Delete', '/TN', 'R0ice Resource Team Watchdog', '/F'], { stdio: 'ignore' });
  } catch {} // fine if the task never existed
}

// -------------------------------------------------------------------- bench ----

async function runBench(cfg, hw, model, levels, minutes) {
  console.log(`BENCH: concurrency levels [${levels.join(', ')}], ${minutes} min each.`);
  const results = [];
  for (const c of levels) {
    console.log(`\n=== bench: concurrency ${c} ===`);
    const pipe = makePipeline(model, { bufferTarget: Math.max(12, c * 6) });
    pipe.start(c);
    const t0 = Date.now();
    await new Promise(r => setTimeout(r, minutes * 60 * 1000));
    const w = pipe.windowStats(Date.now() - t0);
    const g = gpuWindowStats(Date.now() - t0);
    await pipe.stop();
    results.push({ concurrency: c, chunks: w.chunks, per_hour: w.per_hour, avg_gen_s: w.avg_gen_s, gpu: g });
    console.log(`=== concurrency ${c}: ${w.per_hour} chunks/hr, avg gen ${w.avg_gen_s}s`);
  }
  let best = results[0];
  for (const r of results) if (r.per_hour > best.per_hour * 1.03) best = r;
  const out = { at: new Date().toISOString(), model, hw: { gpu: hw.gpu_model, vram_gb: hw.vram_gb }, minutes_per_level: minutes, results, recommended_concurrency: best.concurrency };
  fs.writeFileSync(BENCH_PATH, JSON.stringify(out, null, 2));
  console.log(`\nBENCH COMPLETE. Recommended concurrency: ${best.concurrency} (${best.per_hour} chunks/hr).`);
  return out;
}

// --------------------------------------------------------------------- main ----

async function main() {
  const args = process.argv.slice(2);
  const benchIdx = args.indexOf('--bench');

  // Start the status window + GPU sampler BEFORE the first network call
  // (ensureToken() below). That first join is the very first network hop a
  // brand-new volunteer's install ever makes; if it stalls, the status page
  // needs to already be up so live.setup_note is visible instead of the
  // process just sitting there with no window at all.
  setInterval(sampleGpu, 2000);
  sampleGpu();
  startStatusServer();

  const cfg = await ensureToken();
  // Two independent config.json files exist for this project (source dir vs
  // dist/, read by `node index.js` vs the packaged .exe respectively) and
  // they can silently drift. Print which one THIS process resolved and a
  // token prefix on every boot so a log tail immediately answers "which
  // identity is this" without opening either file. See WHICH_CONFIG.txt.
  console.log(`[config] using ${CONFIG_PATH} -- token ${(cfg.token || '').slice(0, 8)}... handle "${cfg.handle || '?'}"`);

  // While mid-setup (Ollama install / model pull), report far more often than
  // the normal 10min cadence -- setup is exactly when something's most likely
  // to be silently stuck, and this is the only remote signal available short
  // of relaying F12 console output by hand. No-op once mode leaves 'setup'.
  setInterval(() => { if (live.mode === 'setup') reportOnce(); }, 30 * 1000);

  let hw = await reportOnce();

  // full setup path: install Ollama + pull a model if this machine qualifies
  let model = null;
  const gpuCapable = (hw.vram_gb || 0) >= MIN_VRAM_GB;
  let setupError = null;
  if (gpuCapable) {
    try {
      model = await ensureOllamaAndModel(hw);
      hw = await gatherHardware(); // refresh has_ollama for the census
      reportOnce();
    } catch (e) {
      console.error('Ollama setup failed:', e.message);
      setupError = e.message;
    }
  }

  if (!model) {
    // Census-only forever from here on. Without a concrete reason, this looks
    // IDENTICAL on the status page to a machine that's perfectly capable but
    // just has no book chunks queued right now (labels.idle in statusPage()
    // says the same "waiting for work" either way) -- a non-technical friend
    // has no way to tell "this PC will never read books" from "check back
    // later." Give idle mode a real explanation instead of a generic one.
    live.mode = 'idle';
    live.setup_note = gpuCapable
      ? `the reading engine couldn't finish setting up on this PC (${setupError || 'unknown error'}) -- you're still counted, but it won't read books until this is fixed.`
      : `your graphics card has ${hw.vram_gb != null ? hw.vram_gb + ' GB' : 'an unknown amount'} of video memory -- reading books needs at least ${MIN_VRAM_GB} GB, so this PC won't do that part. You're still counted in the community census -- thanks for joining! If the graphics card ever changes, it'll pick up reading work automatically next time it starts.`;
    console.log(`Census-only mode (${gpuCapable ? 'setup failed' : 'no capable GPU'}): ${live.setup_note}`);
    setupTray({ paused: !!cfg.paused });
    setInterval(() => reportOnce(), REPORT_INTERVAL_MS);
    return;
  }
  live.model = model;
  console.log(`Using model: ${model}`);

  if (benchIdx !== -1) {
    const levels = (args[benchIdx + 1] || '1,2,3').split(',').map(Number).filter(n => n >= 1 && n <= 8);
    const mIdx = args.indexOf('--bench-minutes');
    const minutes = mIdx !== -1 ? Math.max(2, Number(args[mIdx + 1]) || 6) : 6;
    live.mode = 'reading';
    const out = await runBench(cfg, hw, model, levels, minutes);
    saveConfig({ ...loadConfig(), concurrency: out.recommended_concurrency });
    console.log(`config.json updated: concurrency=${out.recommended_concurrency}.`);
    process.exit(0);
  }

  const state = { paused: !!cfg.paused };
  setupTray(state);
  setInterval(() => reportOnce(), REPORT_INTERVAL_MS);

  const concurrency = Math.max(1, Math.min(8, Number(cfg.concurrency) || 1));
  live.concurrency = concurrency;
  console.log(`Worker: concurrency ${concurrency}, stream guard on (OBS + ${cfg.prestream || '17:45-18:15'} quiet window).`);

  let pipeline = null;
  startStatsReporter(() => pipeline, concurrency);

  // The daily stream block only auto-applies on machines that actually have
  // OBS installed (the streaming rig). Non-streaming rigs (PC #2, friends
  // without OBS) read straight through unless their config sets `prestream`.
  const isStreamingRig = obsWsConfig() !== null || fs.existsSync(path.join(process.env.APPDATA || '', 'obs-studio'));

  const guard = async () => {
    const liveCfg = loadConfig();
    // timed pauses ("pause 1h" in the status window) persist in config and
    // auto-expire; the plain paused flag holds until manually resumed
    if (liveCfg.pausedUntil && Date.now() >= liveCfg.pausedUntil) {
      saveConfig({ ...liveCfg, pausedUntil: null });
      liveCfg.pausedUntil = null;
    }
    live.paused_until = liveCfg.pausedUntil || null;
    let reason = null;
    if (liveCfg.paused || state.paused || (liveCfg.pausedUntil && Date.now() < liveCfg.pausedUntil)) reason = 'paused-user';
    else if (inQuietWindow(liveCfg)) reason = 'paused-quiet';
    else reason = await obsBlocking(); // null unless OBS is actually streaming/recording

    if (!reason && !pipeline) {
      // good-citizen resume gate: if something else (a game) is hammering the
      // GPU while we're paused, stay out of its way and check again later.
      const g = gpuWindowStats(30 * 1000);
      if (g && g.util_avg > 40) reason = 'paused-gpu';
    }

    if (reason) {
      if (pipeline) {
        console.log(`stream guard: pausing (${reason})`);
        await pipeline.stop();
        pipeline = null;
        await unloadModel(model);
      }
      live.mode = reason;
    } else if (!pipeline) {
      console.log('stream guard: clear — reading resumes');
      pipeline = makePipeline(model, { bufferTarget: Math.max(12, concurrency * 6) });
      pipeline.start(concurrency);
      live.mode = 'reading';
    }
  };
  await guard();
  setInterval(guard, 20 * 1000);

  // reset the daily counter at local midnight
  setInterval(() => {
    const now = new Date();
    if (now.getHours() === 0 && now.getMinutes() === 0) { live.today = { chunks: 0, words: 0 }; }
  }, 55 * 1000);

  console.log('Running. Tray icon: open status window, pause, or quit.');
}

// Best-effort: tell the server what killed this process before it exits, so
// a fatal crash is diagnosable remotely instead of vanishing along with the
// console window (exactly the gap that made an overnight setup failure
// undiagnosable -- the only trace was "stopped reporting", no error). Races
// against a short timeout so a broken network can't hang the exit.
async function reportFatal(err) {
  live.last_error = String((err && err.stack) || err).slice(0, 2000);
  live.last_error_at = new Date().toISOString();
  try {
    await Promise.race([
      reportOnce(),
      new Promise(r => setTimeout(r, 5000))
    ]);
  } catch {}
}

main().catch(async e => {
  console.error('fatal:', e);
  await reportFatal(e);
  process.exit(1);
});

process.on('uncaughtException', async e => {
  console.error('uncaughtException:', e);
  await reportFatal(e);
  process.exit(1);
});

process.on('unhandledRejection', async e => {
  console.error('unhandledRejection:', e);
  await reportFatal(e);
  process.exit(1);
});
