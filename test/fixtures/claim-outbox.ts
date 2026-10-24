/// <reference types="node" />

import {
  connectDatabase,
  transaction,
} from "../../src/infrastructure/database";

async function claim(): Promise<void> {
  if (!process.env.DATABASE_URL?.endsWith("_test")) {
    throw new Error("This fixture requires an isolated test database");
  }

  const id = process.env.TEST_OUTBOX_ID;

  if (!id) {
    throw new Error("TEST_OUTBOX_ID is required");
  }

  await connectDatabase();

  const claimed = await transaction(async (q) => {
    const result = await q.query(
      `
        WITH candidate AS (
          SELECT id
          FROM outbox_messages
          WHERE id = $1
            AND published_at IS NULL
            AND (
              locked_until IS NULL
              OR locked_until < now()
            )
          FOR UPDATE SKIP LOCKED
        )
        UPDATE outbox_messages o
        SET locked_until = now() + interval '5 seconds'
        FROM candidate c
        WHERE o.id = c.id
        RETURNING o.id
      `,
      [id],
      true,
    );

    return result.records.length;
  });

  if (claimed !== 1) {
    throw new Error("Could not reserve the outbox message");
  }

  // Encerra propositalmente após confirmar a reserva, antes de enviar ao SQS.
  process.exit(19);
}

claim().catch((error) => {
  console.error(error);
  process.exit(1);
});
