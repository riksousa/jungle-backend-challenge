import { Counter, Gauge, Histogram, Registry } from "prom-client";
export const registry = new Registry();
export const counters = new Counter({
  name: "wager_actions_total",
  help: "Operational events",
  labelNames: ["action", "status"],
  registers: [registry],
});
export const latency = new Histogram({
  name: "wager_processing_seconds",
  help: "Processing latency",
  registers: [registry],
});
export const outboxLag = new Gauge({
  name: "wager_outbox_lag_seconds",
  help: "Age of oldest pending event",
  registers: [registry],
});
export const dlqMessages = new Gauge({
  name: "wager_dlq_messages",
  help: "Approximate messages currently in the dead letter queue",
  registers: [registry],
});
export function log(
  event: string,
  context: Record<string, unknown> = {},
): void {
  console.log(
    JSON.stringify({
      time: new Date().toISOString(),
      event,
      correlationId: null,
      messageId: null,
      transactionId: null,
      walletId: null,
      providerId: null,
      ...context,
    }),
  );
}
export function count(action: string, status = ""): void {
  counters.inc({ action, status });
}
export function sqlCode(error: unknown): string {
  return typeof error === "object" && error !== null && "code" in error
    ? String(error.code)
    : "";
}
