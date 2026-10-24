import { MigrationInterface, QueryRunner } from "typeorm";

export class Initial1770000000000 implements MigrationInterface {
  name = "Initial1770000000000";
  async up(q: QueryRunner): Promise<void> {
    await q.query(`CREATE EXTENSION IF NOT EXISTS pgcrypto`);
    await q.query(`CREATE TABLE wallets (
      id uuid PRIMARY KEY, player_id uuid NOT NULL, currency char(3) NOT NULL,
      balance numeric(20,2) NOT NULL DEFAULT 0 CHECK (balance >= 0),
      version integer NOT NULL DEFAULT 1 CHECK (version >= 1),
      created_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now(),
      UNIQUE(player_id, currency), CHECK(currency IN ('BRL','USD','EUR'))
    )`);
    await q.query(`CREATE TABLE wager_transactions (
      id uuid PRIMARY KEY, provider_id text NOT NULL, external_transaction_id text NOT NULL,
      idempotency_key text NOT NULL, payload_hash char(64) NOT NULL, wallet_id uuid NOT NULL REFERENCES wallets(id),
      player_id uuid NOT NULL, round_id text NOT NULL, game_id text NOT NULL,
      kind text NOT NULL CHECK(kind IN ('OPENING','BET','WIN','LOSS','REFUND','ROLLBACK')),
      amount numeric(20,2) NOT NULL CHECK(amount >= 0), currency char(3) NOT NULL,
      reference_external_transaction_id text, reference_transaction_id uuid REFERENCES wager_transactions(id),
      status text NOT NULL CHECK(status IN ('PENDING','PENDING_REFERENCE','PROCESSED','REJECTED','FAILED')),
      failure_code text, balance_snapshot numeric(20,2), reference_attempts integer NOT NULL DEFAULT 0,
      next_reference_at timestamptz, created_at timestamptz NOT NULL DEFAULT now(), processed_at timestamptz,
      UNIQUE(provider_id, external_transaction_id), UNIQUE(provider_id, idempotency_key)
    )`);
    await q.query(
      `CREATE INDEX wager_reference_lookup ON wager_transactions(provider_id, external_transaction_id, status)`,
    );
    await q.query(
      `CREATE INDEX wager_pending_reference ON wager_transactions(next_reference_at) WHERE status='PENDING_REFERENCE'`,
    );
    await q.query(`CREATE TABLE wallet_ledger (
      id uuid PRIMARY KEY, wallet_id uuid NOT NULL REFERENCES wallets(id), transaction_id uuid NOT NULL UNIQUE REFERENCES wager_transactions(id),
      direction text NOT NULL CHECK(direction IN ('DEBIT','CREDIT')), amount numeric(20,2) NOT NULL CHECK(amount > 0),
      currency char(3) NOT NULL, balance_before numeric(20,2) NOT NULL CHECK(balance_before >= 0),
      balance_after numeric(20,2) NOT NULL CHECK(balance_after >= 0), created_at timestamptz NOT NULL DEFAULT now(),
      CHECK((direction='CREDIT' AND balance_before + amount = balance_after) OR (direction='DEBIT' AND balance_before - amount = balance_after))
    )`);
    await q.query(
      `CREATE INDEX wallet_ledger_cursor ON wallet_ledger(wallet_id, created_at DESC, id DESC)`,
    );
    await q.query(`CREATE FUNCTION prevent_ledger_mutation() RETURNS trigger LANGUAGE plpgsql AS $$
      BEGIN RAISE EXCEPTION 'wallet_ledger is append-only' USING ERRCODE='55000'; END; $$`);
    await q.query(`CREATE TRIGGER wallet_ledger_append_only BEFORE UPDATE OR DELETE ON wallet_ledger
      FOR EACH ROW EXECUTE FUNCTION prevent_ledger_mutation()`);
    await q.query(`CREATE UNIQUE INDEX one_reversal_per_reference_kind ON wager_transactions(reference_transaction_id, kind)
      WHERE kind IN ('REFUND','ROLLBACK') AND status <> 'REJECTED'`);
    await q.query(`CREATE TABLE inbox_messages (
      consumer_name text NOT NULL, message_id text NOT NULL, payload_hash char(64) NOT NULL,
      received_at timestamptz NOT NULL DEFAULT now(), processed_at timestamptz NOT NULL DEFAULT now(),
      PRIMARY KEY(consumer_name, message_id)
    )`);
    await q.query(`CREATE TABLE outbox_messages (
      id uuid PRIMARY KEY, aggregate_id uuid NOT NULL, event_type text NOT NULL, payload jsonb NOT NULL,
      occurred_at timestamptz NOT NULL DEFAULT now(), attempts integer NOT NULL DEFAULT 0,
      next_attempt_at timestamptz NOT NULL DEFAULT now(), locked_until timestamptz, published_at timestamptz
    )`);
    await q.query(
      `CREATE INDEX outbox_pending ON outbox_messages(next_attempt_at, occurred_at) WHERE published_at IS NULL`,
    );
  }
  async down(q: QueryRunner): Promise<void> {
    await q.query(
      "DROP TABLE IF EXISTS outbox_messages, inbox_messages, wallet_ledger, wager_transactions, wallets CASCADE",
    );
    await q.query("DROP FUNCTION IF EXISTS prevent_ledger_mutation()");
  }
}
