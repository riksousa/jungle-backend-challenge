import { randomUUID } from "node:crypto";
export abstract class IntegrationEvent {
  abstract readonly eventType: string;
  readonly version = 1;
  readonly eventId = randomUUID();
  readonly occurredAt = new Date();
  protected constructor(
    public readonly aggregateId: string,
    public readonly correlationId: string,
    public readonly causationId: string | undefined,
    public readonly data: Readonly<Record<string, unknown>>,
  ) {}
  toJSON(): Record<string, unknown> {
    return {
      eventId: this.eventId,
      eventType: this.eventType,
      version: this.version,
      aggregateId: this.aggregateId,
      correlationId: this.correlationId,
      causationId: this.causationId,
      occurredAt: this.occurredAt.toISOString(),
      data: this.data,
    };
  }
}
export class WagerTransactionProcessed extends IntegrationEvent {
  readonly eventType = "WagerTransactionProcessed";
  static create(
    a: string,
    c: string,
    d: Record<string, unknown>,
    cause?: string,
  ): WagerTransactionProcessed {
    return new this(a, c, cause, d);
  }
}
export class WagerTransactionRejected extends IntegrationEvent {
  readonly eventType = "WagerTransactionRejected";
  static create(
    a: string,
    c: string,
    d: Record<string, unknown>,
    cause?: string,
  ): WagerTransactionRejected {
    return new this(a, c, cause, d);
  }
}
export class WagerTransactionPendingReference extends IntegrationEvent {
  readonly eventType = "WagerTransactionPendingReference";
  static create(
    a: string,
    c: string,
    d: Record<string, unknown>,
    cause?: string,
  ): WagerTransactionPendingReference {
    return new this(a, c, cause, d);
  }
}
export class WalletBalanceChanged extends IntegrationEvent {
  readonly eventType = "WalletBalanceChanged";
  static create(
    a: string,
    c: string,
    d: Record<string, unknown>,
    cause?: string,
  ): WalletBalanceChanged {
    return new this(a, c, cause, d);
  }
}

export class WagerTransactionFailed extends IntegrationEvent {
  readonly eventType = "WagerTransactionFailed";

  static create(
    aggregateId: string,
    correlationId: string,
    data: Record<string, unknown>,
    causationId?: string,
  ): WagerTransactionFailed {
    return new this(
      aggregateId,
      correlationId,
      causationId,
      data,
    );
  }
}
