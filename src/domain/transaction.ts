import { DomainError } from "./errors";
import { Money, MoneyProps } from "./money";
import { Direction } from "./wallet";
export type Kind = "OPENING" | "BET" | "WIN" | "LOSS" | "REFUND" | "ROLLBACK";
export type Status =
  | "PENDING"
  | "PENDING_REFERENCE"
  | "PROCESSED"
  | "REJECTED"
  | "FAILED";
export interface Command {
  providerId: string;
  externalTransactionId: string;
  walletId: string;
  playerId: string;
  roundId: string;
  gameId: string;
  kind: Kind;
  money: MoneyProps;
  referenceExternalTransactionId?: string;
}
export class WagerTransaction {
  private constructor(
    public readonly command: Readonly<Command>,
    private _status: Status,
    private _failureCode?: string,
  ) {}
  static create(command: Command): WagerTransaction {
    if (
      ["REFUND", "ROLLBACK"].includes(command.kind) &&
      !command.referenceExternalTransactionId
    )
      throw new DomainError("REFERENCE_REQUIRED");
    const money = Money.from(command.money);
    if (command.kind === "LOSS" ? !money.isZero() : !money.isPositive())
      throw new DomainError("INVALID_OPERATION_AMOUNT");
    if (
      command.referenceExternalTransactionId === command.externalTransactionId
    )
      throw new DomainError("SELF_REFERENCE");
    return new WagerTransaction(
      Object.freeze({ ...command, money: Object.freeze({ ...command.money }) }),
      "PENDING",
    );
  }
  static rehydrate(
    command: Command,
    status: Status,
    code?: string,
  ): WagerTransaction {
    return new WagerTransaction(command, status, code);
  }
  get status(): Status {
    return this._status;
  }
  get failureCode(): string | undefined {
    return this._failureCode;
  }
  isTerminal(): boolean {
    return ["PROCESSED", "REJECTED", "FAILED"].includes(this._status);
  }
  private transition(status: Status, code?: string): void {
    if (this.isTerminal()) throw new DomainError("INVALID_TRANSACTION_STATE");
    this._status = status;
    this._failureCode = code;
  }
  markProcessed(): void {
    this.transition("PROCESSED");
  }
  markPendingReference(): void {
    this.transition("PENDING_REFERENCE");
  }
  reject(code: string): void {
    this.transition("REJECTED", code);
  }
  fail(code: string): void {
    this.transition("FAILED", code);
  }
  requiresReference(): boolean {
    return ["REFUND", "ROLLBACK"].includes(this.command.kind);
  }
  affectsBalance(): boolean {
    return this.command.kind !== "LOSS";
  }
  ledgerDirectionFor(reference?: WagerTransaction): Direction {
    if (this.command.kind === "ROLLBACK") {
      if (
        !reference ||
        !["BET", "WIN", "REFUND"].includes(reference.command.kind)
      )
        throw new DomainError("INVALID_REFERENCE_KIND");
      return reference.command.kind === "BET" ? "CREDIT" : "DEBIT";
    }
    return this.command.kind === "BET" ? "DEBIT" : "CREDIT";
  }
  validateReference(ref: WagerTransaction): void {
    const c = this.command,
      r = ref.command;
    if (
      ["providerId", "playerId", "walletId", "roundId"].some(
        (k) => c[k as keyof Command] !== r[k as keyof Command],
      )
    )
      throw new DomainError("REFERENCE_MISMATCH");
    const amount = Money.from(c.money),
      original = Money.from(r.money);
    if (
      (c.kind === "REFUND" && r.kind !== "BET") ||
      (c.kind === "WIN" && r.kind !== "BET") ||
      (c.kind === "ROLLBACK" && !["BET", "WIN", "REFUND"].includes(r.kind))
    )
      throw new DomainError("INVALID_REFERENCE_KIND");
    if (["REFUND", "ROLLBACK"].includes(c.kind) && !amount.equals(original))
      throw new DomainError("REFERENCE_AMOUNT_MISMATCH");
    if (c.money.currency !== r.money.currency)
      throw new DomainError("CURRENCY_MISMATCH");
    if (ref.status !== "PROCESSED")
      throw new DomainError("REFERENCE_NOT_PROCESSED");
  }
}
