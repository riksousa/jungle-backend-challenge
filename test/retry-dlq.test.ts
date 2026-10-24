/// <reference types="node" />
/// <reference types="bun" />

import { afterAll, beforeAll, expect, test } from "bun:test";
import { randomUUID } from "node:crypto";
import { execFileSync, spawn, type ChildProcess } from "node:child_process";
import {
  DeleteQueueCommand,
  GetQueueUrlCommand,
  SendMessageCommand,
} from "@aws-sdk/client-sqs";

import { WageringService } from "../src/application/wagering.service";
import {
  closeDatabase,
  connectDatabase,
  dataSource,
} from "../src/infrastructure/database";
import { sqs } from "../src/infrastructure/queues";

const service = new WageringService();
const suffix = randomUUID();

const requestName = `retry-${suffix}.fifo`;
const dlqName = `retry-dlq-${suffix}.fifo`;
const eventName = `retry-events-${suffix}.fifo`;

let child: ChildProcess | undefined;
let workerOutput = "";

const sleep = (milliseconds: number): Promise<void> =>
  new Promise((resolve) => setTimeout(resolve, milliseconds));

async function startFailingWorker(): Promise<void> {
  const worker = spawn(
    process.execPath,
    [
      "--env-file=.env.test",
      "run",
      "test/fixtures/failing-worker.ts",
    ],
    {
      cwd: process.cwd(),
      env: {
        ...process.env,
        SQS_REQUEST_QUEUE: requestName,
        SQS_DLQ: dlqName,
        SQS_EVENT_QUEUE: eventName,
      },
      stdio: ["ignore", "pipe", "pipe"],
    },
  );

  child = worker;

  await new Promise<void>((resolve, reject) => {
    const timeout = setTimeout(() => {
      reject(new Error(`Worker startup timed out: ${workerOutput}`));
    }, 30_000);

    worker.stdout?.setEncoding("utf8");
    worker.stderr?.setEncoding("utf8");

    worker.stdout?.on("data", (chunk: string) => {
      workerOutput += chunk;

      if (workerOutput.includes('"event":"worker_ready"')) {
        clearTimeout(timeout);
        resolve();
      }
    });

    worker.stderr?.on("data", (chunk: string) => {
      workerOutput += chunk;
    });

    worker.once("error", (error) => {
      clearTimeout(timeout);
      reject(error);
    });

    worker.once("exit", (code, signal) => {
      clearTimeout(timeout);
      reject(
        new Error(
          `Worker exited: code=${code}, signal=${signal}\n${workerOutput}`,
        ),
      );
    });
  });
}

async function stopFailingWorker(): Promise<void> {
  const worker = child;

  if (
    !worker ||
    worker.exitCode !== null ||
    worker.signalCode !== null
  ) {
    return;
  }

  const exited = new Promise<void>((resolve, reject) => {
    const timeout = setTimeout(() => {
      reject(new Error("Retry worker did not stop"));
    }, 20_000);

    worker.once("exit", () => {
      clearTimeout(timeout);
      resolve();
    });
  });

  if (process.platform === "win32" && worker.pid) {
    try {
      execFileSync(
        "taskkill",
        ["/PID", String(worker.pid), "/T", "/F"],
        { stdio: "ignore" },
      );
    } catch {
      worker.kill("SIGKILL");
    }
  } else {
    worker.kill("SIGTERM");
  }

  await exited;
}

beforeAll(async () => {
  if (!process.env.DATABASE_URL?.endsWith("_test")) {
    throw new Error("This test requires an isolated *_test database");
  }

  await connectDatabase();
});

afterAll(async () => {
  try {
    await stopFailingWorker();
  } finally {
    try {
      for (const name of [requestName, dlqName, eventName]) {
        try {
          const queue = await sqs.send(
            new GetQueueUrlCommand({ QueueName: name }),
          );

          if (queue.QueueUrl) {
            await sqs.send(
              new DeleteQueueCommand({ QueueUrl: queue.QueueUrl }),
            );
          }
        } catch (error) {
          if (
            error instanceof Error &&
            [
              "QueueDoesNotExist",
              "AWS.SimpleQueueService.NonExistentQueue",
            ].includes(error.name)
          ) {
            continue;
          }

          throw error;
        }
      }
    } finally {
      await closeDatabase();
    }
  }
});

test("processing failures retry and move automatically to the DLQ", async () => {
  await startFailingWorker();

  const playerId = randomUUID();

  const wallet = await service.createWallet(playerId, {
    amount: "50.00",
    currency: "BRL",
  });

  const externalId = `retry-operation-${randomUUID()}`;
  const messageId = `retry-message-${randomUUID()}`;

  const envelope = {
    messageId,
    type: "WagerTransactionRequested",
    occurredAt: new Date().toISOString(),
    data: {
      providerId: "retry-provider",
      externalTransactionId: externalId,
      idempotencyKey: externalId,
      playerId,
      walletId: wallet.id,
      roundId: "retry-round",
      gameId: "retry-game",
      kind: "BET",
      money: {
        amount: "10.00",
        currency: "BRL",
      },
    },
  };

  const request = await sqs.send(
    new GetQueueUrlCommand({ QueueName: requestName }),
  );

  if (!request.QueueUrl) {
    throw new Error("Could not resolve retry queue");
  }

  // Envia apenas para a fila de requisições. O SQS deve encaminhar a mensagem para a DLQ.
  await sqs.send(
    new SendMessageCommand({
      QueueUrl: request.QueueUrl,
      MessageBody: JSON.stringify(envelope),
      MessageGroupId: wallet.id,
      MessageDeduplicationId: randomUUID(),
    }),
  );

  let failed = false;
  const deadline = Date.now() + 45_000;

  while (Date.now() < deadline) {
    const transactions = await dataSource.query(
      `
        SELECT status
        FROM wager_transactions
        WHERE provider_id = $1
          AND external_transaction_id = $2
      `,
      ["retry-provider", externalId],
    );

    if (transactions[0]?.status === "FAILED") {
      failed = true;
      break;
    }

    await sleep(250);
  }

  if (!failed) {
    console.error(workerOutput);
  }

  expect(failed).toBe(true);

  const events = workerOutput
    .split(/\r?\n/)
    .flatMap((line) => {
      try {
        return [JSON.parse(line)];
      } catch {
        return [];
      }
    });

  const attempts = events.filter(
    (event) =>
      event.event === "test_forced_failure" &&
      event.messageId === messageId,
  );

  expect(attempts.length).toBeGreaterThanOrEqual(5);

  expect(
    events.some(
      (event) =>
        event.event === "test_dlq_delivery" &&
        event.messageId === messageId,
    ),
  ).toBe(true);

  const transaction = await service.getExternal(
    "retry-provider",
    externalId,
  );

  expect(transaction.failureCode).toBe(
    "INFRASTRUCTURE_RETRIES_EXHAUSTED",
  );

  expect((await service.getWallet(wallet.id)).balance.amount).toBe(
    "50.00",
  );

  const ledger = await service.listLedger(wallet.id, undefined, 100);
  expect(ledger.items).toHaveLength(1);

  const failureEvents = await dataSource.query(
    `
      SELECT count(*)::int AS total
      FROM outbox_messages
      WHERE aggregate_id = $1
        AND event_type = 'WagerTransactionFailed'
    `,
    [transaction.transactionId],
  );

  expect(failureEvents[0].total).toBe(1);
  expect((await service.reconcile(wallet.id)).consistent).toBe(true);
});
