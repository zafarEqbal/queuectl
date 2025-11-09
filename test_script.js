const {
  initDb,
  enqueueJob,
  workerLoop,
  listJobs,
  getStatus,
  setConfig
} = require('./src/core');

async function main() {
  initDb();
  setConfig('max_retries', '2');
  setConfig('backoff_base', '2');
  console.log('Enqueuing test jobs...');
  enqueueJob('{"id":"job-success","command":"echo success"}');
  enqueueJob('{"id":"job-fail","command":"nonexistent_command_123"}');
  enqueueJob('{"id":"job-slow","command":"sleep 2"}');
  console.log('Running worker loop (single in-process worker)...');
  await workerLoop('test-worker', { maxJobs: null, maxIdleSeconds: 15 });
  console.log('Final status:');
  console.log(getStatus());
  console.log('All jobs:');
  const jobs = listJobs('all', 50);
  for (const j of jobs) {
    console.log(j);
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
