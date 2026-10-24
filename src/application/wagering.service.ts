import { randomUUID } from "node:crypto";
import { Injectable } from "@nestjs/common";
import { QueryRunner } from "typeorm";
import {
  ConflictError,
  DomainError,
  FailureCode,
  NotFoundError,
} from "../domain/errors";
import { Money, MoneyProps } from "../domain/money";
import { Wallet } from "../domain/wallet";

import {
  WagerTransaction,
  type Kind,
  type Status,
} from "../domain/transaction";
import { envelopeSchema, hash } from "./contracts";
import { Command } from "../domain/transaction";
import { dataSource, transaction } from "../infrastructure/database";
import { count, latency, log } from "../infrastructure/telemetry";

import {
  IntegrationEvent,
  WalletBalanceChanged,
  WagerTransactionFailed,
  WagerTransactionPendingReference,
  WagerTransactionProcessed,
  WagerTransactionRejected,
} from "../domain/events";

type DbTransaction = {
  id: string;
  provider_id: string;
  external_transaction_id: string;
  idempotency_key: string;
  payload_hash: string;
  wallet_id: string;
  player_id: string;
  round_id: string;
  game_id: string;
  kind: Kind;
  amount: string;
  currency: string;
  reference_external_transaction_id: string | null;
  reference_transaction_id: string | null;
  status: Status;
  failure_code: string | null;
  balance_snapshot: string | null;
  reference_attempts: number;
};
type SubmitCommand = Command & { idempotencyKey: string };
const prop = (row: DbTransaction) => ({
  amount: row.amount,
  currency: row.currency,
});
const output = (row: DbTransaction, replay = false) => ({
  transactionId: row.id,
  status: row.status,
  ...(row.balance_snapshot === null
    ? {}
    : {
        balance: Money.rehydrate({
          amount: row.balance_snapshot,
          currency: row.currency,
        }).toJSON(),
      }),
  ...(row.failure_code ? { failureCode: row.failure_code } : {}),
  idempotentReplay: replay,
});

@Injectable()
export class WageringService {
  async createWallet(playerId: string, initial: MoneyProps) {
    const money = Money.from(initial),
      id = randomUUID();
    return transaction(async (q) => {
      const now = new Date();
      try {
        await q.query(
          `INSERT INTO wallets(id,player_id,currency,balance,version,created_at,updated_at) VALUES($1,$2,$3,$4,1,$5,$5)`,
          [id, playerId, money.currency, money.toString(), now],
        );
      } catch (e) {
        if (pgCode(e) === "23505")
          throw new ConflictError("WALLET_ALREADY_EXISTS");
        throw e;
      }
      if (money.isPositive()) {
        const txId = randomUUID(),
          cmd = {
            providerId: "internal",
            externalTransactionId: `opening:${id}`,
            idempotencyKey: `opening:${id}`,
            playerId,
            walletId: id,
            roundId: "opening",
            gameId: "internal",
            kind: "OPENING",
            money: money.toJSON(),
          };
        await q.query(
          `INSERT INTO wager_transactions(id,provider_id,external_transaction_id,idempotency_key,payload_hash,wallet_id,player_id,round_id,game_id,kind,amount,currency,status,balance_snapshot,processed_at)
          VALUES($1,'internal',$2,$3,$4,$5,$6,'opening','internal','OPENING',$7,$8,'PROCESSED',$7,$9)`,
          [
            txId,
            `opening:${id}`,
            `opening:${id}`,
            hash(cmd),
            id,
            playerId,
            money.toString(),
            money.currency,
            now,
          ],
        );
        await q.query(
          `INSERT INTO wallet_ledger(id,wallet_id,transaction_id,direction,amount,currency,balance_before,balance_after,created_at) VALUES($1,$2,$3,'CREDIT',$4,$5,'0.00',$4,$6)`,
          [randomUUID(), id, txId, money.toString(), money.currency, now],
        );
      }
      return { id, playerId, balance: money.toJSON(), version: 1 };
    });
  }

  private rehydrateTransaction(
  row: DbTransaction,
): WagerTransaction {
  return WagerTransaction.rehydrate(
    {
      providerId: row.provider_id,
      externalTransactionId: row.external_transaction_id,
      walletId: row.wallet_id,
      playerId: row.player_id,
      roundId: row.round_id,
      gameId: row.game_id,
      kind: row.kind,
      money: {
        amount: row.amount,
        currency: row.currency,
      },
      referenceExternalTransactionId:
        row.reference_external_transaction_id ?? undefined,
    },
    row.status,
    row.failure_code ?? undefined,
  );
}

  private runInTransaction<T>(
    runner: QueryRunner | undefined,
    work: (q: QueryRunner) => Promise<T>,
  ): Promise<T> {
    return runner ? work(runner) : transaction(work);
  }

  async consumeEnvelope(input: unknown) {
    const envelope = envelopeSchema.parse(input);

    return transaction(async (q) => {
      const accepted = await this.submit(envelope.data, q, false);

      return this.process(
        accepted.transactionId,
        {
          messageId: envelope.messageId,
          payloadHash: hash(envelope),
        },
        q,
      );
    });
  }

  async submit(
    command: SubmitCommand,
    runner?: QueryRunner,
    publishRequest = true,
  ) {
    // A requisição HTTP apenas registra a intenção. O publicador da outbox a envia ao SQS após confirmar a transação.
    const transactionAggregate = WagerTransaction.create(command);
    const money = Money.from(transactionAggregate.command.money);
    const { idempotencyKey: _key, ...businessPayload } = command;
    const payloadHash = hash(businessPayload),
      transactionId = randomUUID();
    return this.runInTransaction(runner, async (q) => {
      const existing = (
        await q.query(
          `SELECT * FROM wager_transactions WHERE provider_id=$1 AND idempotency_key=$2 FOR UPDATE`,
          [command.providerId, command.idempotencyKey],
        )
      )[0] as DbTransaction | undefined;
      if (existing) {
        if (existing.payload_hash !== payloadHash)
          throw new ConflictError("PAYLOAD_CONFLICT");
        count("idempotency", "duplicate");
        return output(existing, true);
      }
      const wallet = (
        await q.query(`SELECT id,player_id,currency FROM wallets WHERE id=$1`, [
          command.walletId,
        ])
      )[0];
      if (!wallet) throw new NotFoundError("WALLET_NOT_FOUND");
      if (wallet.player_id !== command.playerId)
        throw new DomainError("PLAYER_WALLET_MISMATCH");
      if (wallet.currency !== money.currency)
        throw new DomainError("CURRENCY_MISMATCH");
      const now = new Date();
      const inserted = await q.query(
        `INSERT INTO wager_transactions(id,provider_id,external_transaction_id,idempotency_key,payload_hash,wallet_id,player_id,round_id,game_id,kind,amount,currency,reference_external_transaction_id,status,created_at)
        VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,'PENDING',$14) ON CONFLICT DO NOTHING RETURNING id`,
        [
          transactionId,
          command.providerId,
          command.externalTransactionId,
          command.idempotencyKey,
          payloadHash,
          command.walletId,
          command.playerId,
          command.roundId,
          command.gameId,
          command.kind,
          money.toString(),
          money.currency,
          command.referenceExternalTransactionId ?? null,
          now,
        ],
      );
      if (!inserted.length) {
        const conflict = (
          await q.query(
            `SELECT * FROM wager_transactions WHERE provider_id=$1 AND (idempotency_key=$2 OR external_transaction_id=$3)`,
            [
              command.providerId,
              command.idempotencyKey,
              command.externalTransactionId,
            ],
          )
        )[0] as DbTransaction | undefined;
        if (conflict?.payload_hash === payloadHash) {
          count("idempotency", "duplicate");
          return output(conflict, true);
        }
        throw new ConflictError("PAYLOAD_CONFLICT");
      }
      const envelope = {
        messageId: transactionId,
        type: "WagerTransactionRequested",
        occurredAt: now.toISOString(),
        data: command,
      };

      if (publishRequest) {
        await this.enqueue(
          q,
          transactionId,
          "WagerTransactionRequested",
          envelope,
          now,
        );
      }
      return { transactionId, status: "PENDING", idempotentReplay: false };
    });
  }

  async process(
    transactionId: string,
    inbox?: { messageId: string; payloadHash: string },
    runner?: QueryRunner,
  ) {
    const started = Date.now();
    try {
      return await this.runInTransaction(runner, async (q) => {
        if (inbox) {
          const inserted = await q.query(
            `INSERT INTO inbox_messages(consumer_name,message_id,payload_hash) VALUES('wager-processor',$1,$2) ON CONFLICT DO NOTHING RETURNING message_id`,
            [inbox.messageId, inbox.payloadHash],
          );
          if (!inserted.length) {
            const previous = (
              await q.query(
                `SELECT payload_hash FROM inbox_messages WHERE consumer_name='wager-processor' AND message_id=$1`,
                [inbox.messageId],
              )
            )[0];
            if (previous?.payload_hash !== inbox.payloadHash)
              throw new ConflictError("PAYLOAD_CONFLICT");
            return { duplicateMessage: true };
          }
        }
        const tx = (
          await q.query(
            `SELECT * FROM wager_transactions WHERE id=$1 FOR UPDATE`,
            [transactionId],
          )
        )[0] as DbTransaction | undefined;
       if (!tx) {
  throw new NotFoundError("INVALID_REQUEST");
}

const transactionAggregate = this.rehydrateTransaction(tx);

if (transactionAggregate.isTerminal()) {
  return output(tx, true);
}
        const wallet = (
          await q.query(`SELECT * FROM wallets WHERE id=$1 FOR UPDATE`, [
            tx.wallet_id,
          ])
        )[0];
        if (!wallet) return this.reject(q, tx, "WALLET_NOT_FOUND");
        if (
          wallet.player_id !== tx.player_id ||
          wallet.currency !== tx.currency
        )
          return this.reject(q, tx, "REFERENCE_MISMATCH");
        const amount = Money.rehydrate(prop(tx));
        let reference: DbTransaction | undefined;
        if (tx.reference_external_transaction_id) {
          reference = (
            await q.query(
              `SELECT * FROM wager_transactions WHERE provider_id=$1 AND external_transaction_id=$2`,
              [tx.provider_id, tx.reference_external_transaction_id],
            )
          )[0] as DbTransaction | undefined;
          if (!reference) return this.pendingOrReject(q, tx, true);
          const validation = this.validateReference(tx, reference);
          if (validation) return this.reject(q, tx, validation);
          if (reference.status !== "PROCESSED") {
            if (["REJECTED", "FAILED"].includes(reference.status))
              return this.reject(q, tx, "REFERENCE_NOT_PROCESSED");
            return this.pendingOrReject(q, tx, false);
          }
        } else if (["REFUND", "ROLLBACK"].includes(tx.kind))
          return this.reject(q, tx, "REFERENCE_REQUIRED");

        if (["REFUND", "ROLLBACK"].includes(tx.kind)) {
          const prior = (
            await q.query(
              `SELECT id FROM wager_transactions WHERE reference_transaction_id=$1 AND kind=$2 AND status='PROCESSED'`,
              [reference!.id, tx.kind],
            )
          )[0];
          if (prior) return this.reject(q, tx, "ALREADY_REVERSED");
        }

        const before = Money.rehydrate({
          amount: String(wallet.balance),
          currency: wallet.currency,
        });
        const walletAggregate = Wallet.rehydrate({
          id: wallet.id,
          playerId: wallet.player_id,
          balance: before,
          version: Number(wallet.version),
        });
       const direction = transactionAggregate.affectsBalance()
  ? transactionAggregate.ledgerDirectionFor(
      reference
        ? this.rehydrateTransaction(reference)
        : undefined,
    )
  : undefined;
        let after = before;
        if (direction) {
          let ledgerEntry;
          try {
            ledgerEntry = walletAggregate.move(
              tx.id,
              amount,
              direction,
              tx.kind === "ROLLBACK",
            );
            after = walletAggregate.balance;
          } catch (e) {
            if (e instanceof DomainError && e.code === "CURRENCY_MISMATCH")
              return this.reject(q, tx, "CURRENCY_MISMATCH");
            if (e instanceof DomainError && e.code === "INSUFFICIENT_FUNDS")
              return this.reject(q, tx, "INSUFFICIENT_FUNDS");
            if (
              e instanceof DomainError &&
              e.code === "REVERSAL_INSUFFICIENT_FUNDS"
            )
              return this.reject(q, tx, "REVERSAL_INSUFFICIENT_FUNDS");
            if (e instanceof DomainError)
              return this.reject(
                q,
                tx,
                tx.kind === "ROLLBACK"
                  ? "REVERSAL_INSUFFICIENT_FUNDS"
                  : "INSUFFICIENT_FUNDS",
              );
            throw e;
          }
          await q.query(
            `UPDATE wallets SET balance=$2,version=$3,updated_at=now() WHERE id=$1`,
            [wallet.id, after.toString(), walletAggregate.version],
          );
          await q.query(
            `INSERT INTO wallet_ledger(id,wallet_id,transaction_id,direction,amount,currency,balance_before,balance_after) VALUES($1,$2,$3,$4,$5,$6,$7,$8)`,
            [
              randomUUID(),
              wallet.id,
              tx.id,
              ledgerEntry!.direction,
              ledgerEntry!.money.toString(),
              ledgerEntry!.money.currency,
              ledgerEntry!.balanceBefore.toString(),
              ledgerEntry!.balanceAfter.toString(),
            ],
          );
          await this.enqueueEvent(
            q,
            WalletBalanceChanged.create(
              tx.wallet_id,
              tx.id,
              {
                walletId: tx.wallet_id,
                transactionId: tx.id,
                direction,
                money: amount.toJSON(),
                balanceBefore: before.toJSON(),
                balanceAfter: after.toJSON(),
                walletVersion: walletAggregate.version,
              },
              inbox?.messageId,
            ),
          );
        }
        transactionAggregate.markProcessed();

await q.query(
  `
    UPDATE wager_transactions
    SET
      status = $4,
      reference_transaction_id = $2,
      balance_snapshot = $3,
      processed_at = now(),
      failure_code = NULL
    WHERE id = $1
  `,
  [
    tx.id,
    reference?.id ?? null,
    after.toString(),
    transactionAggregate.status,
  ],
);
        await this.enqueueEvent(
          q,
          WagerTransactionProcessed.create(
            tx.id,
            tx.id,
            {
              transactionId: tx.id,
              providerId: tx.provider_id,
              status: "PROCESSED",
              balance: after.toJSON(),
            },
            inbox?.messageId,
          ),
        );
        count("transaction", "PROCESSED");
        log("wager_processed", {
          correlationId: tx.id,
          transactionId: tx.id,
          walletId: tx.wallet_id,
          providerId: tx.provider_id,
        });
        return {
          transactionId: tx.id,
          status: "PROCESSED",
          balance: after.toJSON(),
          idempotentReplay: false,
        };
      });
    } finally {
      latency.observe((Date.now() - started) / 1000);
    }
  }

  private validateReference(
    tx: DbTransaction,
    ref: DbTransaction,
  ): FailureCode | undefined {
    if (
      ref.provider_id !== tx.provider_id ||
      ref.player_id !== tx.player_id ||
      ref.wallet_id !== tx.wallet_id ||
      ref.round_id !== tx.round_id
    )
      return "REFERENCE_MISMATCH";
    if (ref.currency !== tx.currency) return "CURRENCY_MISMATCH";
    if (tx.kind === "REFUND" && ref.kind !== "BET")
      return "INVALID_REFERENCE_KIND";
    if (tx.kind === "WIN" && ref.kind !== "BET")
      return "INVALID_REFERENCE_KIND";
    if (tx.kind === "ROLLBACK" && !["BET", "WIN", "REFUND"].includes(ref.kind))
      return "INVALID_REFERENCE_KIND";
    if (
      ["REFUND", "ROLLBACK"].includes(tx.kind) &&
      !Money.rehydrate(prop(tx)).equals(Money.rehydrate(prop(ref)))
    )
      return "REFERENCE_AMOUNT_MISMATCH";
    return undefined;
  }

  private async pendingOrReject(
    q: QueryRunner,
    tx: DbTransaction,
    referenceMissing: boolean,
  ) {
    const attempts = Number(tx.reference_attempts) + 1,
      max = Number(process.env.REFERENCE_MAX_ATTEMPTS ?? 10);
    if (attempts >= max)
      return this.reject(
        q,
        tx,
        referenceMissing ? "REFERENCE_NOT_FOUND" : "REFERENCE_NOT_PROCESSED",
      );
    const delay = Math.min(
      Number(process.env.REFERENCE_BASE_MS ?? 1000) * 2 ** (attempts - 1),
      60000,
    );
    const aggregate = this.rehydrateTransaction(tx);
aggregate.markPendingReference();

await q.query(
  `
    UPDATE wager_transactions
    SET
      status = $4,
      reference_attempts = $2,
      next_reference_at =
        now() + ($3 || ' milliseconds')::interval
    WHERE id = $1
  `,
  [tx.id, attempts, delay, aggregate.status],
);
    await this.enqueueEvent(
      q,
      WagerTransactionPendingReference.create(tx.id, tx.id, {
        transactionId: tx.id,
        attempts,
        nextAttemptAt: new Date(Date.now() + delay).toISOString(),
      }),
    );
    count("transaction", "PENDING_REFERENCE");
    return {
      transactionId: tx.id,
      status: "PENDING_REFERENCE",
      idempotentReplay: false,
    };
  }

  private async reject(q: QueryRunner, tx: DbTransaction, code: FailureCode) {
    const aggregate = this.rehydrateTransaction(tx);
aggregate.reject(code);

await q.query(
  `
    UPDATE wager_transactions
    SET
      status = $3,
      failure_code = $2,
      processed_at = now()
    WHERE id = $1
  `,
  [tx.id, aggregate.failureCode, aggregate.status],
);
    await this.enqueueEvent(
      q,
      WagerTransactionRejected.create(tx.id, tx.id, {
        transactionId: tx.id,
        providerId: tx.provider_id,
        failureCode: code,
      }),
    );
    count("transaction", "REJECTED");
    log("wager_rejected", {
      correlationId: tx.id,
      transactionId: tx.id,
      walletId: tx.wallet_id,
      providerId: tx.provider_id,
      failureCode: code,
    });
    return {
      transactionId: tx.id,
      status: "REJECTED",
      failureCode: code,
      idempotentReplay: false,
    };
  }

  private async enqueueEvent(
    q: QueryRunner,
    event: IntegrationEvent,
  ): Promise<void> {
    await this.enqueue(
      q,
      event.aggregateId,
      event.eventType,
      event.toJSON(),
      event.occurredAt,
    );
  }

  private async enqueue(
    q: QueryRunner,
    aggregateId: string,
    eventType: string,
    payload: unknown,
    at = new Date(),
  ) {
    await q.query(
      `INSERT INTO outbox_messages(id,aggregate_id,event_type,payload,occurred_at) VALUES($1,$2,$3,$4,$5)`,
      [randomUUID(), aggregateId, eventType, JSON.stringify(payload), at],
    );
  }

  async retryPendingReferences(limit = 100) {
    // Reserva referências com prazo atingido entre todas as instâncias; a reserva expira se um worker cair.
    const rows = await transaction(async (q) => {
      const result = await q.query(
        `WITH due AS (
      SELECT id
      FROM wager_transactions
      WHERE status = 'PENDING_REFERENCE'
        AND next_reference_at <= now()
      ORDER BY next_reference_at
      LIMIT $1
      FOR UPDATE SKIP LOCKED
    )
    UPDATE wager_transactions t
    SET next_reference_at = now() + interval '60 seconds'
    FROM due
    WHERE t.id = due.id
    RETURNING t.id`,
        [limit],
        true,
      );

      return result.records as { id: string }[];
    });
    for (const row of rows) await this.process(row.id);
    return rows.length;
  }
  async failFromDeadLetter(input: unknown): Promise<void> {
    const envelope = envelopeSchema.parse(input);
    const payloadHash = hash(envelope);

    const failed = await transaction(async (q) => {
      const inserted = await q.query(
        `
        INSERT INTO inbox_messages (
          consumer_name,
          message_id,
          payload_hash
        )
        VALUES ('wager-dlq-audit', $1, $2)
        ON CONFLICT DO NOTHING
        RETURNING message_id
      `,
        [envelope.messageId, payloadHash],
      );

      if (!inserted.length) {
        const previous = (
          await q.query(
            `
            SELECT payload_hash
            FROM inbox_messages
            WHERE consumer_name = 'wager-dlq-audit'
              AND message_id = $1
          `,
            [envelope.messageId],
          )
        )[0];

        if (previous?.payload_hash !== payloadHash) {
          throw new ConflictError("PAYLOAD_CONFLICT");
        }

        return;
      }

      // Localiza uma operação existente ou registra uma requisição válida enviada diretamente ao SQS.
      // Reutiliza esta transação SQL sem publicar outra requisição na fila.
      const accepted = await this.submit(envelope.data, q, false);

      const tx = (
        await q.query(
          `
          SELECT *
          FROM wager_transactions
          WHERE id = $1
          FOR UPDATE
        `,
          [accepted.transactionId],
        )
      )[0] as DbTransaction | undefined;

      if (!tx) {
        throw new Error("Could not resolve transaction for DLQ audit");
      }

      // Uma entrega tardia da DLQ não deve alterar uma transação em estado final.
      const aggregate = this.rehydrateTransaction(tx);

if (aggregate.isTerminal()) {
  return;
}

aggregate.fail("INFRASTRUCTURE_RETRIES_EXHAUSTED");

await q.query(
  `
    UPDATE wager_transactions
    SET
      status = $2,
      failure_code = $3,
      processed_at = now()
    WHERE id = $1
  `,
  [tx.id, aggregate.status, aggregate.failureCode],
);

      await this.enqueueEvent(
        q,
        WagerTransactionFailed.create(
          tx.id,
          tx.id,
          {
            transactionId: tx.id,
            providerId: tx.provider_id,
            failureCode: "INFRASTRUCTURE_RETRIES_EXHAUSTED",
          },
          envelope.messageId,
        ),
      );

      return {
        transactionId: tx.id,
        walletId: tx.wallet_id,
        providerId: tx.provider_id,
      };
    });

    if (failed) {
      count("transaction", "FAILED");

      log("wager_failed_after_dlq", {
        correlationId: failed.transactionId,
        messageId: envelope.messageId,
        transactionId: failed.transactionId,
        walletId: failed.walletId,
        providerId: failed.providerId,
      });
    }
  }
  async getWallet(id: string) {
    const row = (
      await dataSource.query(
        `SELECT id,player_id,currency,balance,version,created_at,updated_at FROM wallets WHERE id=$1`,
        [id],
      )
    )[0];
    if (!row) throw new NotFoundError("WALLET_NOT_FOUND");
    return {
      id: row.id,
      playerId: row.player_id,
      balance: { amount: String(row.balance), currency: row.currency },
      version: Number(row.version),
    };
  }
  async getTransaction(id: string) {
    const row = (
      await dataSource.query(`SELECT * FROM wager_transactions WHERE id=$1`, [
        id,
      ])
    )[0] as DbTransaction | undefined;
    if (!row) throw new NotFoundError("INVALID_REQUEST");
    return output(row);
  }
  async getExternal(provider: string, externalId: string) {
    const row = (
      await dataSource.query(
        `SELECT * FROM wager_transactions WHERE provider_id=$1 AND external_transaction_id=$2`,
        [provider, externalId],
      )
    )[0] as DbTransaction | undefined;
    if (!row) throw new NotFoundError("INVALID_REQUEST");
    return output(row);
  }
  async listLedger(walletId: string, cursor?: string, limit = 50) {
    await this.getWallet(walletId);
    let before: { createdAt: string; id: string } | undefined;
    if (cursor) {
      try {
        before = JSON.parse(Buffer.from(cursor, "base64url").toString());
      } catch {
        throw new DomainError("INVALID_REQUEST");
      }
    }
    const rows = before
      ? await dataSource.query(
          `SELECT * FROM wallet_ledger WHERE wallet_id=$1 AND (created_at,id)<($2::timestamptz,$3::uuid) ORDER BY created_at DESC,id DESC LIMIT $4`,
          [walletId, before.createdAt, before.id, limit + 1],
        )
      : await dataSource.query(
          `SELECT * FROM wallet_ledger WHERE wallet_id=$1 ORDER BY created_at DESC,id DESC LIMIT $2`,
          [walletId, limit + 1],
        );
    const hasMore = rows.length > limit,
      page = rows.slice(0, limit),
      last = page.at(-1);
    return {
      items: page.map((r: any) => ({
        id: r.id,
        walletId: r.wallet_id,
        transactionId: r.transaction_id,
        direction: r.direction,
        money: { amount: String(r.amount), currency: r.currency },
        balanceBefore: {
          amount: String(r.balance_before),
          currency: r.currency,
        },
        balanceAfter: { amount: String(r.balance_after), currency: r.currency },
        createdAt: new Date(r.created_at).toISOString(),
      })),
      nextCursor:
        hasMore && last
          ? Buffer.from(
              JSON.stringify({
                createdAt: new Date(last.created_at).toISOString(),
                id: last.id,
              }),
            ).toString("base64url")
          : null,
    };
  }
  async reconcile(walletId: string) {
    const rows = await dataSource.query(
      `SELECT w.balance,w.currency,COALESCE(SUM(CASE WHEN l.direction='CREDIT' THEN l.amount ELSE -l.amount END),0) AS calculated,COUNT(l.id)::int AS entries FROM wallets w LEFT JOIN wallet_ledger l ON l.wallet_id=w.id WHERE w.id=$1 GROUP BY w.id`,
      [walletId],
    );
    const row = rows[0];
    if (!row) throw new NotFoundError("WALLET_NOT_FOUND");
    const stored = Money.rehydrate({
        amount: String(row.balance),
        currency: row.currency,
      }),
      calculated = Money.rehydrate({
        amount: String(row.calculated),
        currency: row.currency,
      }),
      difference = stored.subtract(calculated),
      consistent = difference.isZero();
    if (!consistent) {
      count("reconciliation", "inconsistent");
      log("wallet_reconciliation_mismatch", { walletId });
    }
    return {
      walletId,
      storedBalance: stored.toJSON(),
      calculatedBalance: calculated.toJSON(),
      difference: difference.toJSON(),
      consistent,
      checkedEntries: Number(row.entries),
    };
  }
}

function pgCode(e: unknown): string {
  return typeof e === "object" && e !== null && "code" in e
    ? String((e as { code: unknown }).code)
    : "";
}
