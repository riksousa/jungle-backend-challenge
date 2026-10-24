/// <reference types="node" />
/// <reference types="bun" />

import { afterAll, beforeAll, expect, test } from "bun:test";
import { randomUUID } from "node:crypto";
import { execFileSync, spawn, type ChildProcess } from "node:child_process";
import {
  CreateQueueCommand,
  DeleteMessageCommand,
  DeleteQueueCommand,
  GetQueueAttributesCommand,
  GetQueueUrlCommand,
  ReceiveMessageCommand,
  SendMessageCommand,
} from "@aws-sdk/client-sqs";

import { WageringService } from "../src/application/wagering.service";
import { hash } from "../src/application/contracts";
import {
  closeDatabase,
  connectDatabase,
  dataSource,
} from "../src/infrastructure/database";
import {
  deadLetterQueueName,
  ensureQueues,
  queueUrl,
  requestQueueName,
  sqs,
} from "../src/infrastructure/queues";

const service = new WageringService();

let children: ChildProcess[] = [];
let recoveryQueueUrl: string | undefined;


const sleep = (milliseconds: number): Promise<void> =>
  new Promise((resolve) => setTimeout(resolve, milliseconds));

function startWorker(
  queueName: string = requestQueueName,
): Promise<ChildProcess> {
  const child = spawn(
    process.execPath,
    ["--env-file=.env.test", "run", "src/worker.ts"],
    {
      cwd: process.cwd(),
      env: {
        ...process.env,
        SQS_REQUEST_QUEUE: queueName,
      },
      stdio: ["ignore", "pipe", "pipe", "ipc"],
    },
  );

  children.push(child);

  return new Promise((resolve, reject) => {
    let output = "";
    let settled = false;

    const timeout = setTimeout(() => {
      if (settled) return;

      settled = true;
      reject(new Error(`Worker did not become ready: ${output}`));
    }, 30_000);

    const fail = (error: Error): void => {
      if (settled) return;

      settled = true;
      clearTimeout(timeout);
      reject(error);
    };

    child.stdout?.setEncoding("utf8");
    child.stderr?.setEncoding("utf8");

    child.stdout?.on("data", (chunk: string) => {
      output += chunk;
      process.stdout.write(`[worker ${child.pid}] ${chunk}`);

      if (!settled && output.includes('"event":"worker_ready"')) {
        settled = true;
        clearTimeout(timeout);
        resolve(child);
      }
    });

    child.stderr?.on("data", (chunk: string) => {
      output += chunk;
      process.stderr.write(`[worker ${child.pid}] ${chunk}`);
    });

    child.once("error", fail);

    child.once("exit", (code, signal) => {
      fail(
        new Error(
          `Worker exited before becoming ready ` +
            `(code=${code}, signal=${signal}): ${output}`,
        ),
      );
    });
  });
}

async function stopWorkers(): Promise<void> {
  const active = children;
  children = [];

  const running = active.filter(
    (child) => child.exitCode === null && child.signalCode === null,
  );

  const exits = running.map(
    (child) =>
      new Promise<void>((resolve, reject) => {
        const timeout = setTimeout(() => {
          reject(
            new Error(`Worker ${child.pid} did not stop within 20 seconds`),
          );
        }, 20_000);

        child.once("exit", () => {
          clearTimeout(timeout);
          resolve();
        });
      }),
  );

  const completion = Promise.all(exits);

  // Registra imediatamente um tratador enquanto os comandos de encerramento são executados.
  void completion.catch(() => {});

  for (const child of running) {
    if (!child.pid) continue;

    if (child.connected) {
      child.send("shutdown");
    } else if (process.platform === "win32") {
      try {
        execFileSync("taskkill", ["/PID", String(child.pid), "/T", "/F"], {
          stdio: "ignore",
        });
      } catch {
        child.kill("SIGKILL");
      }
    } else {
      child.kill("SIGTERM");
    }
  }

  await completion;
}

async function clearRequestQueue(): Promise<void> {
  for (let attempt = 0; attempt < 100; attempt++) {
    const batch = await sqs.send(
      new ReceiveMessageCommand({
        QueueUrl: queueUrl(requestQueueName),
        WaitTimeSeconds: 0,
        MaxNumberOfMessages: 10,
        VisibilityTimeout: 1,
      }),
    );

    if (!batch.Messages?.length) return;

    await Promise.all(
      batch.Messages.filter((message) => message.ReceiptHandle).map((message) =>
        sqs.send(
          new DeleteMessageCommand({
            QueueUrl: queueUrl(requestQueueName),
            ReceiptHandle: message.ReceiptHandle!,
          }),
        ),
      ),
    );
  }
}

beforeAll(async () => {
  if (!process.env.DATABASE_URL?.endsWith("_test")) {
    throw new Error(
      "Set DATABASE_URL to an isolated *_test database before system tests.",
    );
  }

  if (
    !requestQueueName.endsWith("-test.fifo") ||
    !deadLetterQueueName.endsWith("-test-dlq.fifo")
  ) {
    throw new Error(
      "Configure isolated SQS test queues in .env.test before system tests.",
    );
  }

  await connectDatabase();
  await ensureQueues();
  await clearRequestQueue();

  await dataSource.query(`
    TRUNCATE
      outbox_messages,
      inbox_messages,
      wallet_ledger,
      wager_transactions,
      wallets
    CASCADE
  `);

  // Os workers iniciam somente após a confirmação da reserva abandonada na outbox.
});

afterAll(async () => {
  try {
    await stopWorkers();
  } finally {
    try {
      if (recoveryQueueUrl) {
        await sqs.send(
          new DeleteQueueCommand({
            QueueUrl: recoveryQueueUrl,
          }),
        );
      }
    } finally {
      await closeDatabase();
    }
  }
});

test("three separate worker processes consume through SQS without duplicate financial effects", async () => {

  // Interrompe os publicadores para que a simulação reserve a requisição primeiro.
await stopWorkers();

const publisherPlayerId = randomUUID();

const publisherWallet = await service.createWallet(
  publisherPlayerId,
  {
    amount: "100.00",
    currency: "BRL",
  },
);

const publisherExternalId = `publisher-${randomUUID()}`;

const publisherAccepted = await service.submit({
  providerId: "publisher-provider",
  externalTransactionId: publisherExternalId,
  idempotencyKey: publisherExternalId,
  playerId: publisherPlayerId,
  walletId: publisherWallet.id,
  roundId: "publisher-round",
  gameId: "publisher-game",
  kind: "BET",
  money: {
    amount: "9.00",
    currency: "BRL",
  },
});

const publisherOutbox = (
  await dataSource.query(
    `
      SELECT id
      FROM outbox_messages
      WHERE aggregate_id = $1
        AND event_type = 'WagerTransactionRequested'
    `,
    [publisherAccepted.transactionId],
  )
)[0];

if (!publisherOutbox) {
  throw new Error("Missing durable outbox request");
}

const claimProcess = spawn(
  process.execPath,
  [
    "--env-file=.env.test",
    "run",
    "test/fixtures/claim-outbox.ts",
  ],
  {
    cwd: process.cwd(),
    env: {
      ...process.env,
      TEST_OUTBOX_ID: publisherOutbox.id,
    },
    stdio: ["ignore", "pipe", "pipe"],
  },
);

children.push(claimProcess);

let claimOutput = "";

claimProcess.stdout?.on("data", (chunk) => {
  claimOutput += chunk.toString();
});

claimProcess.stderr?.on("data", (chunk) => {
  claimOutput += chunk.toString();
});

const claimExitCode = await new Promise<number | null>(
  (resolve, reject) => {
    const timeout = setTimeout(() => {
      claimProcess.kill("SIGKILL");

      reject(
        new Error(`Outbox fixture timed out: ${claimOutput}`),
      );
    }, 30_000);

    claimProcess.once("error", (error) => {
      clearTimeout(timeout);
      reject(error);
    });

    claimProcess.once("exit", (code) => {
      clearTimeout(timeout);
      resolve(code);
    });
  },
);

if (claimExitCode !== 19) {
  console.error(claimOutput);
}

expect(claimExitCode).toBe(19);

const reservedMessage = (
  await dataSource.query(
    `
      SELECT published_at, locked_until
      FROM outbox_messages
      WHERE id = $1
    `,
    [publisherOutbox.id],
  )
)[0];

expect(reservedMessage.published_at).toBeNull();
expect(reservedMessage.locked_until).not.toBeNull();

expect(
  (
    await service.getTransaction(
      publisherAccepted.transactionId,
    )
  ).status,
).toBe("PENDING");

expect(
  (await service.getWallet(publisherWallet.id)).balance.amount,
).toBe("100.00");

// Os novos processos devem recuperar a reserva abandonada.
await Promise.all([
  startWorker(),
  startWorker(),
  startWorker(),
]);

let publisherRecovered = false;
const publisherDeadline = Date.now() + 30_000;

while (Date.now() < publisherDeadline) {
  const transaction = await service.getTransaction(
    publisherAccepted.transactionId,
  );

  const publishedMessage = (
    await dataSource.query(
      `
        SELECT published_at
        FROM outbox_messages
        WHERE id = $1
      `,
      [publisherOutbox.id],
    )
  )[0];

  if (
    transaction.status === "PROCESSED" &&
    publishedMessage.published_at !== null
  ) {
    publisherRecovered = true;
    break;
  }

  await sleep(250);
}

expect(publisherRecovered).toBe(true);

expect(
  (await service.getWallet(publisherWallet.id)).balance.amount,
).toBe("91.00");

const publisherLedger = await service.listLedger(
  publisherWallet.id,
  undefined,
  100,
);

expect(
  publisherLedger.items.filter(
    (entry: { direction: string }) =>
      entry.direction === "DEBIT",
  ),
).toHaveLength(1);

const publisherInbox = (
  await dataSource.query(
    `
      SELECT count(*)::int AS total
      FROM inbox_messages
      WHERE consumer_name = 'wager-processor'
        AND message_id = $1
    `,
    [publisherAccepted.transactionId],
  )
)[0];

expect(publisherInbox.total).toBe(1);

expect(
  (await service.reconcile(publisherWallet.id)).consistent,
).toBe(true);
  const playerId = randomUUID();

  const wallet = await service.createWallet(playerId, {
    amount: "25.00",
    currency: "BRL",
  });

  const command = {
    providerId: "system-provider",
    externalTransactionId: "sqs-bet",
    idempotencyKey: "system-idem",
    playerId,
    walletId: wallet.id,
    roundId: "sqs-round",
    gameId: "sqs-game",
    kind: "BET" as const,
    money: {
      amount: "5.00",
      currency: "BRL",
    },
  };

  const accepted = await service.submit(command);

  await Promise.all(Array.from({ length: 49 }, () => service.submit(command)));

  expect(accepted.status).toBe("PENDING");

  let final = await service.getTransaction(accepted.transactionId);
  const deadline = Date.now() + 30_000;

  while (Date.now() < deadline) {
    final = await service.getTransaction(accepted.transactionId);

    if (final.status === "PROCESSED" || final.status === "REJECTED") {
      break;
    }

    await sleep(250);
  }

  expect(final.status).toBe("PROCESSED");
  expect((await service.getWallet(wallet.id)).balance.amount).toBe("20.00");
  expect((await service.reconcile(wallet.id)).consistent).toBe(true);

  const rows = await dataSource.query(
    `
        SELECT count(*)::int AS n
        FROM inbox_messages
        WHERE consumer_name = 'wager-processor'
          AND message_id = $1
      `,
    [accepted.transactionId],
  );

  expect(rows[0].n).toBe(1);

  const entries = await service.listLedger(wallet.id, undefined, 100);

  expect(
    entries.items.filter(
      (entry: { direction: string }) => entry.direction === "DEBIT",
    ),
  ).toHaveLength(1);

  const contenders = await Promise.all(
    [1, 2, 3].map((number) =>
      service.submit({
        ...command,
        externalTransactionId: `race-${number}`,
        idempotencyKey: `race-${number}`,
        money: {
          amount: "10.00",
          currency: "BRL",
        },
      }),
    ),
  );

  const raceDeadline = Date.now() + 30_000;
  let statuses: string[] = [];

  while (Date.now() < raceDeadline) {
    const transactions = await Promise.all(
      contenders.map((contender) =>
        service.getTransaction(contender.transactionId),
      ),
    );

    statuses = transactions.map((transaction) => transaction.status);

    if (
      statuses.every(
        (status) => status === "PROCESSED" || status === "REJECTED",
      )
    ) {
      break;
    }

    await sleep(250);
  }

  expect(statuses.filter((status) => status === "PROCESSED")).toHaveLength(2);

  expect(statuses.filter((status) => status === "REJECTED")).toHaveLength(1);

  expect((await service.getWallet(wallet.id)).balance.amount).toBe("0.00");
  expect((await service.reconcile(wallet.id)).consistent).toBe(true);

  // Cria e processa uma operação diretamente pelo SQS.
  const directPlayerId = randomUUID();

  const directWallet = await service.createWallet(directPlayerId, {
    amount: "100.00",
    currency: "BRL",
  });

  const directExternalId = `direct-sqs-${randomUUID()}`;

  const directEnvelope = {
    messageId: `msg-${randomUUID()}`,
    type: "WagerTransactionRequested",
    occurredAt: new Date().toISOString(),
    data: {
      providerId: "system-provider",
      externalTransactionId: directExternalId,
      idempotencyKey: directExternalId,
      playerId: directPlayerId,
      walletId: directWallet.id,
      roundId: "direct-sqs-round",
      gameId: "direct-sqs-game",
      kind: "BET",
      money: {
        amount: "7.00",
        currency: "BRL",
      },
    },
  };

  await sqs.send(
    new SendMessageCommand({
      QueueUrl: queueUrl(requestQueueName),
      MessageBody: JSON.stringify(directEnvelope),
      MessageGroupId: directWallet.id,
      MessageDeduplicationId: randomUUID(),
    }),
  );

  let directProcessed = false;
  const directDeadline = Date.now() + 30_000;

  while (Date.now() < directDeadline) {
    const transactions = await dataSource.query(
      `
      SELECT status
      FROM wager_transactions
      WHERE provider_id = $1
        AND external_transaction_id = $2
    `,
      ["system-provider", directExternalId],
    );

    if (transactions[0]?.status === "PROCESSED") {
      directProcessed = true;
      break;
    }

    await sleep(250);
  }

  expect(directProcessed).toBe(true);

  expect((await service.getWallet(directWallet.id)).balance.amount).toBe(
    "93.00",
  );

  expect((await service.reconcile(directWallet.id)).consistent).toBe(true);

// Audita na DLQ uma requisição enviada diretamente ao SQS.
const dlqPlayerId = randomUUID();

const dlqWallet = await service.createWallet(dlqPlayerId, {
  amount: "50.00",
  currency: "BRL",
});

const dlqExternalId = `dlq-${randomUUID()}`;

const dlqEnvelope = {
  messageId: `msg-dlq-${randomUUID()}`,
  type: "WagerTransactionRequested",
  occurredAt: new Date().toISOString(),
  data: {
    providerId: "system-provider",
    externalTransactionId: dlqExternalId,
    idempotencyKey: dlqExternalId,
    playerId: dlqPlayerId,
    walletId: dlqWallet.id,
    roundId: "dlq-round",
    gameId: "dlq-game",
    kind: "BET",
    money: {
      amount: "10.00",
      currency: "BRL",
    },
  },
};

await sqs.send(
  new SendMessageCommand({
    QueueUrl: queueUrl(deadLetterQueueName),
    MessageBody: JSON.stringify(dlqEnvelope),
    MessageGroupId: dlqWallet.id,
    MessageDeduplicationId: randomUUID(),
  }),
);

let dlqFailed = false;
const dlqDeadline = Date.now() + 30_000;

while (Date.now() < dlqDeadline) {
  const transactions = await dataSource.query(
    `
      SELECT status
      FROM wager_transactions
      WHERE provider_id = $1
        AND external_transaction_id = $2
    `,
    ["system-provider", dlqExternalId],
  );

  if (transactions[0]?.status === "FAILED") {
    dlqFailed = true;
    break;
  }

  await sleep(250);
}

expect(dlqFailed).toBe(true);

const failedTransaction = await service.getExternal(
  "system-provider",
  dlqExternalId,
);

expect(failedTransaction.failureCode).toBe(
  "INFRASTRUCTURE_RETRIES_EXHAUSTED",
);

// Repetir a auditoria não deve criar outro evento de falha.
await service.failFromDeadLetter(dlqEnvelope);

const failureEvents = await dataSource.query(
  `
    SELECT count(*)::int AS total
    FROM outbox_messages
    WHERE aggregate_id = $1
      AND event_type = 'WagerTransactionFailed'
  `,
  [failedTransaction.transactionId],
);

expect(failureEvents[0].total).toBe(1);

expect(
  (await service.getWallet(dlqWallet.id)).balance.amount,
).toBe("50.00");

const dlqLedger = await service.listLedger(
  dlqWallet.id,
  undefined,
  100,
);

// Existe apenas o crédito inicial: a aposta com falha não gerou débito.
expect(dlqLedger.items).toHaveLength(1);

expect(
  (await service.reconcile(dlqWallet.id)).consistent,
).toBe(true);

  // Interrompe os consumidores antes de receber manualmente a mensagem de recuperação.
  await stopWorkers();

  const recoveryPlayer = randomUUID();

  const recoveryWallet = await service.createWallet(recoveryPlayer, {
    amount: "8.00",
    currency: "BRL",
  });

  const recovery = await service.submit({
    providerId: "system-provider",
    externalTransactionId: "after-commit-crash",
    idempotencyKey: "after-commit-crash",
    playerId: recoveryPlayer,
    walletId: recoveryWallet.id,
    roundId: "recovery-round",
    gameId: "recovery-game",
    kind: "BET",
    money: {
      amount: "3.00",
      currency: "BRL",
    },
  });

  const outboxRows = await dataSource.query(
    `
        SELECT payload
        FROM outbox_messages
        WHERE aggregate_id = $1
          AND event_type = 'WagerTransactionRequested'
      `,
    [recovery.transactionId],
  );

  const outbox = outboxRows[0];

  if (!outbox) {
    throw new Error("Recovery request was not recorded in the outbox");
  }

  const envelope =
    typeof outbox.payload === "string"
      ? JSON.parse(outbox.payload)
      : outbox.payload;

  const recoveryQueueName = `wager-recovery-${randomUUID()}.fifo`;

  const dlqAttributes = await sqs.send(
    new GetQueueAttributesCommand({
      QueueUrl: queueUrl(deadLetterQueueName),
      AttributeNames: ["QueueArn"],
    }),
  );

  const deadLetterTargetArn = dlqAttributes.Attributes?.QueueArn;

  if (!deadLetterTargetArn) {
    throw new Error("Could not resolve recovery queue DLQ ARN");
  }

  const createdQueue = await sqs.send(
    new CreateQueueCommand({
      QueueName: recoveryQueueName,
      Attributes: {
        FifoQueue: "true",
        ContentBasedDeduplication: "false",
        VisibilityTimeout: "60",
        RedrivePolicy: JSON.stringify({
          deadLetterTargetArn,
          maxReceiveCount: "5",
        }),
      },
    }),
  );

  recoveryQueueUrl = createdQueue.QueueUrl;

  if (!recoveryQueueUrl) {
    const resolvedQueue = await sqs.send(
      new GetQueueUrlCommand({
        QueueName: recoveryQueueName,
      }),
    );

    recoveryQueueUrl = resolvedQueue.QueueUrl;
  }

  if (!recoveryQueueUrl) {
    throw new Error("Could not resolve recovery queue URL");
  }

  await sqs.send(
    new SendMessageCommand({
      QueueUrl: recoveryQueueUrl,
      MessageBody: JSON.stringify(envelope),
      MessageGroupId: recoveryWallet.id,
      MessageDeduplicationId: randomUUID(),
    }),
  );

  // O teste publicou manualmente esta requisição da outbox.
  await dataSource.query(
    `
        UPDATE outbox_messages
        SET published_at = now(), locked_until = NULL
        WHERE aggregate_id = $1
          AND event_type = 'WagerTransactionRequested'
      `,
    [recovery.transactionId],
  );

  let receiptHandle: string | undefined;
  const deliveryDeadline = Date.now() + 15_000;

  while (!receiptHandle && Date.now() < deliveryDeadline) {
    const delivery = await sqs.send(
      new ReceiveMessageCommand({
        QueueUrl: recoveryQueueUrl,
        WaitTimeSeconds: 2,
        MaxNumberOfMessages: 10,
        VisibilityTimeout: 1,
      }),
    );

    receiptHandle = delivery.Messages?.find((message) => {
      try {
        const received = JSON.parse(message.Body ?? "");
        return received.messageId === envelope.messageId;
      } catch {
        return false;
      }
    })?.ReceiptHandle;
  }

  expect(receiptHandle).toBeTruthy();

  // Confirma o débito sem confirmar o recebimento da mensagem no SQS.
  await service.process(recovery.transactionId, {
    messageId: envelope.messageId,
    payloadHash: hash(envelope),
  });

  // O tempo de invisibilidade expira como se um consumidor tivesse caído após confirmar a transação.
  await sleep(1_200);

  await Promise.all([
    startWorker(recoveryQueueName),
    startWorker(recoveryQueueName),
    startWorker(recoveryQueueName),
  ]);

  let recoveryCompleted = false;
  const recoveryDeadline = Date.now() + 30_000;

  while (Date.now() < recoveryDeadline) {
    const attributes = await sqs.send(
      new GetQueueAttributesCommand({
        QueueUrl: recoveryQueueUrl,
        AttributeNames: [
          "ApproximateNumberOfMessages",
          "ApproximateNumberOfMessagesNotVisible",
        ],
      }),
    );

    const visible = Number(
      attributes.Attributes?.ApproximateNumberOfMessages ?? 0,
    );

    const inFlight = Number(
      attributes.Attributes?.ApproximateNumberOfMessagesNotVisible ?? 0,
    );

    const pendingRows = await dataSource.query(
      `
          SELECT count(*)::int AS n
          FROM outbox_messages
          WHERE aggregate_id = $1
            AND event_type = 'WagerTransactionRequested'
            AND published_at IS NULL
        `,
      [recovery.transactionId],
    );

    const pending = pendingRows[0].n;

    if (visible === 0 && inFlight === 0 && pending === 0) {
      recoveryCompleted = true;
      break;
    }

    await sleep(250);
  }

  expect(recoveryCompleted).toBe(true);

  expect((await service.getWallet(recoveryWallet.id)).balance.amount).toBe(
    "5.00",
  );

  const recoveryLedger = await service.listLedger(
    recoveryWallet.id,
    undefined,
    10,
  );

  expect(
    recoveryLedger.items.filter(
      (entry: { direction: string }) => entry.direction === "DEBIT",
    ),
  ).toHaveLength(1);

  expect((await service.reconcile(recoveryWallet.id)).consistent).toBe(true);
});
