const fs = require('fs');
const os = require('os');
const path = require('path');
const { exec } = require('child_process');
const Database = require('better-sqlite3');

const DB_DIR = path.join(os.homedir(), '.queuectl');
const DB_PATH = path.join(DB_DIR, 'queuectl.db');

const DEFAULT_MAX_RETRIES = 3;
const DEFAULT_BACKOFF_BASE = 2;
const DEFAULT_POLL_INTERVAL = 2.0;
const LOCK_STALE_SECONDS = 300;

let dbInstance = null;
let initialized = false;

function ensureDbDir() {
  if (!fs.existsSync(DB_DIR)) {
    fs.mkdirSync(DB_DIR, { recursive: true });
  }
}

function getDb() {
  if (!dbInstance) {
    ensureDbDir();
    dbInstance = new Database(DB_PATH);
    dbInstance.pragma('journal_mode = WAL');
  }
  return dbInstance;
}

function utcIso() {
  return new Date().toISOString().replace(/\.\d+Z$/, 'Z');
}

function isoMinusSeconds(sec) {
  const d = new Date(Date.now() - sec * 1000);
  return d.toISOString().replace(/\.\d+Z$/, 'Z');
}

function initDb() {
  if (initialized) return;
  const db = getDb();
  db.exec(`
    CREATE TABLE IF NOT EXISTS jobs (
      id TEXT PRIMARY KEY,
      command TEXT NOT NULL,
      state TEXT NOT NULL,
      attempts INTEGER NOT NULL DEFAULT 0,
      max_retries INTEGER NOT NULL,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL,
      next_run_at TEXT,
      last_error TEXT,
      locked_by TEXT,
      locked_at TEXT
    );
  `);
  db.exec(`
    CREATE TABLE IF NOT EXISTS config (
      key TEXT PRIMARY KEY,
      value TEXT NOT NULL
    );
  `);
  const getStmt = db.prepare('SELECT value FROM config WHERE key = ?');
  const setStmt = db.prepare(
    'INSERT INTO config(key, value) VALUES(?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value'
  );
  if (!getStmt.get('max_retries')) setStmt.run('max_retries', String(DEFAULT_MAX_RETRIES));
  if (!getStmt.get('backoff_base')) setStmt.run('backoff_base', String(DEFAULT_BACKOFF_BASE));
  if (!getStmt.get('poll_interval')) setStmt.run('poll_interval', String(DEFAULT_POLL_INTERVAL));
  if (!getStmt.get('stop_workers')) setStmt.run('stop_workers', '0');
  initialized = true;
}

function getConfig(key) {
  initDb();
  const db = getDb();
  const row = db.prepare('SELECT value FROM config WHERE key = ?').get(key);
  return row ? row.value : null;
}

function setConfig(key, value) {
  initDb();
  const db = getDb();
  db.prepare(
    'INSERT INTO config(key, value) VALUES(?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value'
  ).run(key, String(value));
}

function enqueueJob(jobJson) {
  initDb();
  const db = getDb();
  const data = JSON.parse(jobJson);
  const now = utcIso();
  const id = data.id || require('crypto').randomUUID();
  const maxRetriesConf = parseInt(getConfig('max_retries') || DEFAULT_MAX_RETRIES, 10);
  const job = {
    id,
    command: data.command,
    state: data.state || 'pending',
    attempts: data.attempts != null ? parseInt(data.attempts, 10) : 0,
    max_retries: data.max_retries != null ? parseInt(data.max_retries, 10) : maxRetriesConf,
    created_at: data.created_at || now,
    updated_at: now,
    next_run_at: data.next_run_at || now,
    last_error: data.last_error || null,
    locked_by: null,
    locked_at: null
  };
  db.prepare(
    `INSERT INTO jobs
     (id, command, state, attempts, max_retries, created_at, updated_at, next_run_at, last_error, locked_by, locked_at)
     VALUES (@id, @command, @state, @attempts, @max_retries, @created_at, @updated_at, @next_run_at, @last_error, @locked_by, @locked_at)`
  ).run(job);
  return id;
}

function listJobs(state = 'all', limit = 100) {
  initDb();
  const db = getDb();
  if (state && state !== 'all') {
    return db
      .prepare(
        'SELECT * FROM jobs WHERE state = ? ORDER BY created_at LIMIT ?'
      )
      .all(state, limit);
  }
  return db
    .prepare(
      'SELECT * FROM jobs ORDER BY created_at LIMIT ?'
    )
    .all(limit);
}

function listDlq(limit = 100) {
  return listJobs('dead', limit);
}

function retryDlqJob(jobId) {
  initDb();
  const db = getDb();
  const row = db
    .prepare('SELECT * FROM jobs WHERE id = ? AND state = \'dead\'')
    .get(jobId);
  if (!row) return false;
  const now = utcIso();
  db.prepare(
    `UPDATE jobs
     SET state = 'pending',
         attempts = 0,
         next_run_at = ?,
         updated_at = ?,
         last_error = NULL,
         locked_by = NULL,
         locked_at = NULL
     WHERE id = ?`
  ).run(now, now, jobId);
  return true;
}

function fetchNextJob(workerId) {
  initDb();
  const db = getDb();
  const now = utcIso();
  const stale = isoMinusSeconds(LOCK_STALE_SECONDS);
  try {
    db.prepare('BEGIN IMMEDIATE').run();
    const row = db
      .prepare(
        `SELECT * FROM jobs
         WHERE state IN ('pending', 'failed')
           AND (next_run_at IS NULL OR next_run_at <= ?)
           AND (locked_by IS NULL OR locked_at <= ?)
         ORDER BY created_at
         LIMIT 1`
      )
      .get(now, stale);
    if (!row) {
      db.prepare('COMMIT').run();
      return null;
    }
    db.prepare(
      `UPDATE jobs
       SET state = 'processing',
           locked_by = ?,
           locked_at = ?,
           updated_at = ?
       WHERE id = ?`
    ).run(workerId, now, now, row.id);
    db.prepare('COMMIT').run();
    return row;
  } catch (err) {
    try {
      db.prepare('ROLLBACK').run();
    } catch (_) {}
    throw err;
  }
}

function handleJobResult(job, code, stderrText) {
  initDb();
  const db = getDb();
  const now = utcIso();
  const attempts = parseInt(job.attempts, 10);
  const maxRetries = parseInt(job.max_retries, 10);
  const backoffBase = parseFloat(getConfig('backoff_base') || DEFAULT_BACKOFF_BASE);
  if (code === 0) {
    db.prepare(
      `UPDATE jobs
       SET state = 'completed',
           updated_at = ?,
           locked_by = NULL,
           locked_at = NULL,
           last_error = NULL
       WHERE id = ?`
    ).run(now, job.id);
  } else {
    const newAttempts = attempts + 1;
    const errText = (stderrText || '').slice(0, 1000);
    if (newAttempts > maxRetries) {
      db.prepare(
        `UPDATE jobs
         SET state = 'dead',
             attempts = ?,
             updated_at = ?,
             locked_by = NULL,
             locked_at = NULL,
             last_error = ?
         WHERE id = ?`
      ).run(newAttempts, now, errText, job.id);
    } else {
      const delay = Math.pow(backoffBase, newAttempts);
      const nextRun = new Date(Date.now() + delay * 1000)
        .toISOString()
        .replace(/\.\d+Z$/, 'Z');
      db.prepare(
        `UPDATE jobs
         SET state = 'failed',
             attempts = ?,
             next_run_at = ?,
             updated_at = ?,
             locked_by = NULL,
             locked_at = NULL,
             last_error = ?
         WHERE id = ?`
      ).run(newAttempts, nextRun, now, errText, job.id);
    }
  }
}

function shouldStop() {
  const val = getConfig('stop_workers');
  return val === '1';
}

function setStopAll(flag) {
  setConfig('stop_workers', flag ? '1' : '0');
}

function runCommand(cmd) {
  return new Promise((resolve) => {
    exec(cmd, (error, stdout, stderr) => {
      if (error) {
        const code = typeof error.code === 'number' ? error.code : 1;
        resolve({ code, stderr: stderr || error.message || '' });
      } else {
        resolve({ code: 0, stderr: stderr || '' });
      }
    });
  });
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function workerLoop(workerId, opts = {}) {
  initDb();
  const pollInterval = parseFloat(getConfig('poll_interval') || DEFAULT_POLL_INTERVAL);
  const maxJobs = opts.maxJobs != null ? opts.maxJobs : null;
  const maxIdleSeconds = opts.maxIdleSeconds != null ? opts.maxIdleSeconds : null;
  let processed = 0;
  let lastActivity = Date.now();
  while (true) {
    if (shouldStop()) break;
    const job = fetchNextJob(workerId);
    if (!job) {
      if (maxIdleSeconds != null) {
        const idle = (Date.now() - lastActivity) / 1000;
        if (idle >= maxIdleSeconds) break;
      }
      await sleep(pollInterval * 1000);
      continue;
    }
    lastActivity = Date.now();
    let res;
    try {
      res = await runCommand(job.command);
    } catch (err) {
      res = { code: 1, stderr: String(err) };
    }
    handleJobResult(job, res.code, (res.stderr || '').trim());
    processed += 1;
    if (maxJobs != null && processed >= maxJobs) break;
  }
}

function getStatus() {
  initDb();
  const db = getDb();
  const states = ['pending', 'processing', 'completed', 'failed', 'dead'];
  const res = {};
  for (const s of states) {
    const row = db.prepare('SELECT COUNT(*) AS c FROM jobs WHERE state = ?').get(s);
    res[s] = row.c;
  }
  return res;
}

module.exports = {
  initDb,
  enqueueJob,
  listJobs,
  listDlq,
  retryDlqJob,
  getStatus,
  setConfig,
  getConfig,
  workerLoop,
  setStopAll
};
