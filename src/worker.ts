import "reflect-metadata";
import { WageringService } from "./application/wagering.service";
import { closeDatabase, connectDatabase } from "./infrastructure/database";
import { ensureQueues } from "./infrastructure/queues";
import { Workers } from "./infrastructure/workers";
import { log } from "./infrastructure/telemetry";

async function start(): Promise<void> {
  await connectDatabase();
  await ensureQueues();
  const workers = new Workers(new WageringService());
  workers.start();
  log("worker_ready", {
    pid: process.pid,
    database: new URL(process.env.DATABASE_URL!).pathname,
    requestQueue: process.env.SQS_REQUEST_QUEUE,
    sqsEndpoint: process.env.SQS_ENDPOINT,
  });
  let stopping = false;
  const stop = async () => {
    if (stopping) return;
    stopping = true;
    await workers.stop();
    await closeDatabase();
    process.exit(0);
  };
  process.once("SIGTERM", () => void stop());
  process.once("SIGINT", () => void stop());
  process.on("message", (message) => {
    if (message === "shutdown") void stop();
  });
}
start().catch((error) => {
  log("worker_start_failed", { error: String(error) });
  process.exit(1);
});
