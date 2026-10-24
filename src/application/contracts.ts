import { z } from "zod";
import { createHash } from "node:crypto";
const text = z.string().trim().min(1).max(200);
export const moneySchema = z
  .object({
    amount: z.string().regex(/^\d{1,18}\.\d{2}$/),
    currency: z.enum(["BRL", "USD", "EUR"]),
  })
  .strict();
export const walletSchema = z
  .object({ playerId: z.uuid(), initialBalance: moneySchema })
  .strict();
export const commandSchema = z
  .object({
    providerId: text,
    externalTransactionId: text,
    playerId: z.uuid(),
    walletId: z.uuid(),
    roundId: text,
    gameId: text,
    kind: z.enum(["BET", "WIN", "LOSS", "REFUND", "ROLLBACK"]),
    money: moneySchema,
    referenceExternalTransactionId: text.optional(),
  })
  .strict();
export const submitSchema = commandSchema.extend({ idempotencyKey: text });
export const envelopeSchema = z
  .object({
    messageId: text,
    type: z.literal("WagerTransactionRequested"),
    occurredAt: z.iso.datetime(),
    data: submitSchema,
  })
  .strict();
export function canonical(value: unknown): string {
  if (Array.isArray(value)) return "[" + value.map(canonical).join(",") + "]";
  if (value !== null && typeof value === "object")
    return (
      "{" +
      Object.keys(value)
        .filter((k) => (value as Record<string, unknown>)[k] !== undefined)
        .sort()
        .map(
          (k) =>
            JSON.stringify(k) +
            ":" +
            canonical((value as Record<string, unknown>)[k]),
        )
        .join(",") +
      "}"
    );
  return JSON.stringify(value);
}
export function hash(value: unknown): string {
  return createHash("sha256").update(canonical(value)).digest("hex");
}
