const path = require('path');
const { fork } = require('child_process');

const {
  enqueueJob,
  listJobs,
  listDlq,
  retryDlqJob,
  getStatus,
  setConfig,
  getConfig,
  setStopAll,
  initDb
} = require('./core');

function parseOptions(argv) {
  const opts = { _: [] };
  let i = 0;
  while (i < argv.length) {
    const a = argv[i];
    if (a.startsWith('--')) {
      const key = a.slice(2);
      const next = argv[i + 1];
      if (next && !next.startsWith('--')) {
        opts[key] = next;
        i += 2;
      } else {
        opts[key] = true;
        i += 1;
      }
    } else {
      opts._.push(a);
      i += 1;
    }
  }
  return opts;
}

function printTable(rows, headers, truncateCols = {}) {
  if (!rows || rows.length === 0) {
    console.log('No rows.');
    return;
  }
  const widths = {};
  for (const h of headers) {
    widths[h] = h.length;
  }
  for (const row of rows) {
    for (const h of headers) {
      let v = row[h] != null ? String(row[h]) : '';
      if (truncateCols[h] && v.length > truncateCols[h]) {
        v = v.slice(0, truncateCols[h] - 3) + '...';
      }
      if (v.length > widths[h]) widths[h] = v.length;
    }
  }
  const headerLine = headers.map((h) => h.padEnd(widths[h])).join(' | ');
  console.log(headerLine);
  console.log('-'.repeat(headerLine.length));
  for (const row of rows) {
    const line = headers
      .map((h) => {
        let v = row[h] != null ? String(row[h]) : '';
        if (truncateCols[h] && v.length > truncateCols[h]) {
          v = v.slice(0, truncateCols[h] - 3) + '...';
        }
        return v.padEnd(widths[h]);
      })
      .join(' | ');
    console.log(line);
  }
}

function cmdEnqueue(args) {
  const jobJson = args._[1];
  if (!jobJson) {
    console.error('Usage: queuectl enqueue \'{"id":"job1","command":"echo hi"}\'');
    process.exit(1);
  }
  try {
    const id = enqueueJob(jobJson);
    console.log(`Enqueued job ${id}`);
  } catch (err) {
    console.error('Failed to enqueue job:', err.message);
    process.exit(1);
  }
}

function cmdList(args) {
  const state = args.state || 'all';
  const limit = args.limit ? parseInt(args.limit, 10) : 100;
  const jobs = listJobs(state, limit);
  if (!jobs.length) {
    console.log('No jobs found');
    return;
  }
  const headers = [
    'id',
    'command',
    'state',
    'attempts',
    'max_retries',
    'created_at',
    'updated_at',
    'next_run_at',
    'last_error',
    'locked_by'
  ];
  printTable(jobs, headers, { last_error: 40, command: 40 });
}

function cmdStatus() {
  const st = getStatus();
  const total = Object.values(st).reduce((a, b) => a + b, 0);
  console.log(`Total jobs: ${total}`);
  for (const [k, v] of Object.entries(st)) {
    console.log(`${k}: ${v}`);
  }
  const stop = getConfig('stop_workers') || '0';
  console.log(`Workers stop flag: ${stop}`);
}

function startWorkerProcesses(count) {
  initDb();
  setStopAll(false);
  const workers = [];
  const workerPath = path.join(__dirname, 'worker.js');
  for (let i = 0; i < count; i += 1) {
    const wid = `worker-${i + 1}`;
    const p = fork(workerPath, [], {
      env: { ...process.env, WORKER_ID: wid },
      stdio: 'inherit'
    });
    workers.push(p);
    console.log(`Started ${wid} with PID ${p.pid}`);
  }
  let remaining = workers.length;
  workers.forEach((p) => {
    p.on('exit', (code) => {
      console.log(`Worker PID ${p.pid} exited with code ${code}`);
      remaining -= 1;
      if (remaining === 0) {
        process.exit(0);
      }
    });
  });
}

function cmdWorker(args) {
  const action = args._[1];
  if (action === 'start') {
    const count = args.count ? parseInt(args.count, 10) : 1;
    startWorkerProcesses(count);
  } else if (action === 'stop') {
    setStopAll(true);
    console.log('Stop signal set; running workers will exit after current job.');
  } else {
    console.error('Usage: queuectl worker start --count N | queuectl worker stop');
    process.exit(1);
  }
}

function cmdDlq(args) {
  const sub = args._[1];
  if (sub === 'list') {
    const limit = args.limit ? parseInt(args.limit, 10) : 100;
    const jobs = listDlq(limit);
    if (!jobs.length) {
      console.log('DLQ is empty');
      return;
    }
    const headers = ['id', 'command', 'attempts', 'max_retries', 'updated_at', 'last_error'];
    printTable(jobs, headers, { last_error: 40, command: 40 });
  } else if (sub === 'retry') {
    const jobId = args._[2];
    if (!jobId) {
      console.error('Usage: queuectl dlq retry <job-id>');
      process.exit(1);
    }
    const ok = retryDlqJob(jobId);
    if (ok) {
      console.log(`Job ${jobId} moved back to pending`);
    } else {
      console.error(`Job ${jobId} not found in DLQ`);
      process.exit(1);
    }
  } else {
    console.error('Usage: queuectl dlq list | queuectl dlq retry <job-id>');
    process.exit(1);
  }
}

function cmdConfig(args) {
  const sub = args._[1];
  if (sub === 'set') {
    const key = args._[2];
    const value = args._[3];
    if (!key || value == null) {
      console.error('Usage: queuectl config set <key> <value>');
      process.exit(1);
    }
    let internalKey = key;
    if (key === 'max-retries') internalKey = 'max_retries';
    if (key === 'backoff-base') internalKey = 'backoff_base';
    if (key === 'poll-interval') internalKey = 'poll_interval';
    setConfig(internalKey, value);
    console.log(`Config ${key} set to ${value}`);
  } else if (sub === 'get') {
    const key = args._[2];
    if (!key) {
      console.error('Usage: queuectl config get <key>');
      process.exit(1);
    }
    let internalKey = key;
    if (key === 'max-retries') internalKey = 'max_retries';
    if (key === 'backoff-base') internalKey = 'backoff_base';
    if (key === 'poll-interval') internalKey = 'poll_interval';
    const val = getConfig(internalKey);
    if (val == null) {
      console.log(`${key} is not set`);
    } else {
      console.log(`${key} = ${val}`);
    }
  } else {
    console.error('Usage: queuectl config set|get ...');
    process.exit(1);
  }
}

function printHelp() {
  console.log(`queuectl - background job queue

Usage:
  queuectl enqueue '<job-json>'
  queuectl list [--state STATE] [--limit N]
  queuectl status
  queuectl worker start [--count N]
  queuectl worker stop
  queuectl dlq list [--limit N]
  queuectl dlq retry <job-id>
  queuectl config set <key> <value>
  queuectl config get <key>

Examples:
  queuectl enqueue '{"id":"job1","command":"echo hi"}'
  queuectl worker start --count 3
  queuectl status
`);
}

function main(argv) {
  const args = parseOptions(argv);
  const cmd = args._[0];
  if (!cmd) {
    printHelp();
    process.exit(0);
  }
  if (cmd === 'enqueue') return cmdEnqueue(args);
  if (cmd === 'list') return cmdList(args);
  if (cmd === 'status') return cmdStatus(args);
  if (cmd === 'worker') return cmdWorker(args);
  if (cmd === 'dlq') return cmdDlq(args);
  if (cmd === 'config') return cmdConfig(args);
  printHelp();
  process.exit(1);
}

module.exports = { main };
