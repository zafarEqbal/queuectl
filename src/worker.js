const { workerLoop } = require('./core');

async function run() {
  const wid = process.env.WORKER_ID || `worker-${process.pid}`;
  try {
    await workerLoop(wid, { maxJobs: null, maxIdleSeconds: null });
    process.exit(0);
  } catch (err) {
    console.error('Worker error:', err);
    process.exit(1);
  }
}

run();
