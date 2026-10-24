import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { randomUUID } from "node:crypto";
import { WageringService } from "../src/application/wagering.service";
import {
  dataSource,
  connectDatabase,
  closeDatabase,
} from "../src/infrastructure/database";
import { ensureQueues } from "../src/infrastructure/queues";

const service = new WageringService();
let playerId: string, walletId: string;
const amount = (amount: string) => ({ amount, currency: "BRL" });
function bet(external: string, value: string, idem = external) {
  return {
    providerId: "provider-test",
    externalTransactionId: external,
    idempotencyKey: idem,
    playerId,
    walletId,
    roundId: "round-test",
    gameId: "game-test",
    kind: "BET" as const,
    money: amount(value),
  };
}

beforeAll(async () => {
  if (!process.env.DATABASE_URL?.endsWith("_test"))
    throw new Error(
      "Set DATABASE_URL to an isolated *_test database before integration tests.",
    );
  await connectDatabase();
  await ensureQueues();
  await dataSource.query(
    "TRUNCATE outbox_messages,inbox_messages,wallet_ledger,wager_transactions,wallets CASCADE",
  );
});
afterAll(async () => {
  await closeDatabase();
});

describe("PostgreSQL integration and concurrency", () => {
  test("migrations and database constraints create a wallet with an opening ledger entry", async () => {
    playerId = randomUUID();
    const wallet = await service.createWallet(playerId, amount("100.00"));
    walletId = wallet.id;
    expect(wallet.balance.amount).toBe("100.00");
    const ledger = await service.listLedger(wallet.id, undefined, 10);
    expect(ledger.items).toHaveLength(1);
    expect(ledger.items[0]?.direction).toBe("CREDIT");
  });

  test("rolls back all financial effects when event persistence fails", async () => {
  const suffix = randomUUID().replaceAll("-", "");
  const providerId = `atomicity-${suffix}`;
  const externalId = `bet-${suffix}`;
  const messageId = `message-${suffix}`;

  const functionName = `fail_outbox_${suffix}`;
  const triggerName = `fail_outbox_trigger_${suffix}`;

  const atomicPlayerId = randomUUID();

  const wallet = await service.createWallet(atomicPlayerId, {
    amount: "100.00",
    currency: "BRL",
  });

  const envelope = {
    messageId,
    type: "WagerTransactionRequested",
    occurredAt: new Date().toISOString(),
    data: {
      providerId,
      externalTransactionId: externalId,
      idempotencyKey: externalId,
      playerId: atomicPlayerId,
      walletId: wallet.id,
      roundId: "atomicity-round",
      gameId: "atomicity-game",
      kind: "BET",
      money: {
        amount: "10.00",
        currency: "BRL",
      },
    },
  };

  try {
    // Os identificadores e providerId contêm apenas prefixos fixos e UUIDs gerados.
    await dataSource.query(`
      CREATE FUNCTION ${functionName}()
      RETURNS trigger
      LANGUAGE plpgsql
      AS $$
      BEGIN
        IF NEW.event_type = 'WagerTransactionProcessed'
          AND NEW.payload->'data'->>'providerId' = '${providerId}'
        THEN
          RAISE EXCEPTION 'TEST_ATOMICITY_FAILURE';
        END IF;

        RETURN NEW;
      END;
      $$
    `);

    await dataSource.query(`
      CREATE TRIGGER ${triggerName}
      BEFORE INSERT ON outbox_messages
      FOR EACH ROW
      EXECUTE FUNCTION ${functionName}()
    `);

    await expect(
      service.consumeEnvelope(envelope),
    ).rejects.toThrow("TEST_ATOMICITY_FAILURE");
  } finally {
    try {
      await dataSource.query(`
        DROP TRIGGER IF EXISTS ${triggerName}
        ON outbox_messages
      `);
    } finally {
      await dataSource.query(`
        DROP FUNCTION IF EXISTS ${functionName}()
      `);
    }
  }

  // Todas as gravações feitas durante a operação com falha devem ter sido desfeitas.
  const walletAfterFailure = await service.getWallet(wallet.id);

  expect(walletAfterFailure.balance.amount).toBe("100.00");
  expect(walletAfterFailure.version).toBe(1);

  const ledgerAfterFailure = await service.listLedger(
    wallet.id,
    undefined,
    100,
  );

  // O crédito inicial permanece; o débito que falhou não deve permanecer.
  expect(ledgerAfterFailure.items).toHaveLength(1);

  const transactionsAfterFailure = await dataSource.query(
    `
      SELECT count(*)::int AS total
      FROM wager_transactions
      WHERE provider_id = $1
        AND external_transaction_id = $2
    `,
    [providerId, externalId],
  );

  expect(transactionsAfterFailure[0].total).toBe(0);

  const inboxAfterFailure = await dataSource.query(
    `
      SELECT count(*)::int AS total
      FROM inbox_messages
      WHERE consumer_name = 'wager-processor'
        AND message_id = $1
    `,
    [messageId],
  );

  expect(inboxAfterFailure[0].total).toBe(0);

  const outboxAfterFailure = await dataSource.query(
    `
      SELECT count(*)::int AS total
      FROM outbox_messages
      WHERE aggregate_id = $1
         OR payload->'data'->>'providerId' = $2
    `,
    [wallet.id, providerId],
  );

  expect(outboxAfterFailure[0].total).toBe(0);

  // Após remover a falha injetada, a mesma mensagem pode ser processada com sucesso.
  await service.consumeEnvelope(envelope);

  const processed = await service.getExternal(
    providerId,
    externalId,
  );

  expect(processed.status).toBe("PROCESSED");

  expect(
    (await service.getWallet(wallet.id)).balance.amount,
  ).toBe("90.00");

  const finalLedger = await service.listLedger(
    wallet.id,
    undefined,
    100,
  );

  expect(finalLedger.items).toHaveLength(2);

  expect(
    (await service.reconcile(wallet.id)).consistent,
  ).toBe(true);
});

  test("50 concurrent identical submissions produce one durable transaction and one debit", async () => {
    const requests = await Promise.all(
      Array.from({ length: 50 }, () =>
        service.submit(bet("same-external", "10.00", "same-idempotency-key")),
      ),
    );
    const ids = new Set(requests.map((x) => x.transactionId));
    expect(ids.size).toBe(1);
    const id = requests[0]!.transactionId;
    await Promise.all(Array.from({ length: 50 }, () => service.process(id)));
    const wallet = await service.getWallet(walletId),
      ledger = await service.listLedger(walletId, undefined, 100);
    expect(wallet.balance.amount).toBe("90.00");
    expect(
      ledger.items.filter(
        (x: { direction: string }) => x.direction === "DEBIT",
      ),
    ).toHaveLength(1);
    expect((await service.reconcile(walletId)).consistent).toBe(true);
  });

  test("two 80.00 bets racing against 100.00 yield one success and no overdraft", async () => {
    playerId = randomUUID();
    walletId = (await service.createWallet(playerId, amount("100.00"))).id;
    const a = await service.submit(bet("race-a", "80.00")),
      b = await service.submit(bet("race-b", "80.00"));
    const results = await Promise.all([
      service.process(a.transactionId),
      service.process(b.transactionId),
    ]);
    expect(
      results.filter((x) => "status" in x && x.status === "PROCESSED"),
    ).toHaveLength(1);
    expect(
      results.filter((x) => "status" in x && x.status === "REJECTED"),
    ).toHaveLength(1);
    expect((await service.getWallet(walletId)).balance.amount).toBe("20.00");
    expect((await service.reconcile(walletId)).consistent).toBe(true);
  });

  test("BET, WIN, LOSS, REFUND and ROLLBACK follow their ledger rules", async () => {
    playerId = randomUUID();
    walletId = (await service.createWallet(playerId, amount("100.00"))).id;
    const original = await service.submit(bet("kind-bet", "20.00"));
    await service.process(original.transactionId);
    const win = await service.submit({
      ...bet("kind-win", "50.00"),
      kind: "WIN",
      referenceExternalTransactionId: "kind-bet",
    });
    await service.process(win.transactionId);
    const loss = await service.submit({
      ...bet("kind-loss", "0.00"),
      kind: "LOSS",
    });
    await service.process(loss.transactionId);
    const refund = await service.submit({
      ...bet("kind-refund", "20.00"),
      kind: "REFUND",
      referenceExternalTransactionId: "kind-bet",
    });
    await service.process(refund.transactionId);
    const duplicateRefund = await service.submit({
      ...bet("kind-refund-duplicate", "20.00"),
      kind: "REFUND",
      referenceExternalTransactionId: "kind-bet",
    });
    await service.process(duplicateRefund.transactionId);
    const rollback = await service.submit({
      ...bet("kind-rollback", "50.00"),
      kind: "ROLLBACK",
      referenceExternalTransactionId: "kind-win",
    });
    await service.process(rollback.transactionId);
    expect((await service.getTransaction(loss.transactionId)).status).toBe(
      "PROCESSED",
    );
    expect(
      (await service.getTransaction(duplicateRefund.transactionId)).status,
    ).toBe("REJECTED");
    expect((await service.getWallet(walletId)).balance.amount).toBe("100.00");
    expect(
      (await service.listLedger(walletId, undefined, 100)).items,
    ).toHaveLength(5); // opening + four movements
    expect((await service.reconcile(walletId)).consistent).toBe(true);
  });

  test("pending refund is recovered after its referenced bet arrives", async () => {
    playerId = randomUUID();
    walletId = (await service.createWallet(playerId, amount("50.00"))).id;
    const refund = await service.submit({
      ...bet("refund-x", "12.00"),
      kind: "REFUND",
      referenceExternalTransactionId: "bet-late",
    });
    const pending = await service.process(refund.transactionId);
    expect("status" in pending && pending.status).toBe("PENDING_REFERENCE");
    const wager = await service.submit(bet("bet-late", "12.00"));
    await service.process(wager.transactionId);
    await dataSource.query(
      `UPDATE wager_transactions SET next_reference_at=now() WHERE id=$1`,
      [refund.transactionId],
    );
    await service.retryPendingReferences();
    expect((await service.getTransaction(refund.transactionId)).status).toBe(
      "PROCESSED",
    );
    expect((await service.reconcile(walletId)).consistent).toBe(true);
  });

  test("same idempotency key with different business payload is a conflict", async () => {
    const one = bet("key-one", "1.00", "conflicting-key");
    await service.submit(one);
    await expect(
      service.submit({
        ...one,
        externalTransactionId: "key-two",
        money: amount("2.00"),
      }),
    ).rejects.toThrow("PAYLOAD_CONFLICT");
  });
});
