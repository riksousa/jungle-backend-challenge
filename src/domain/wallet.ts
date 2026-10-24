import { Money } from "./money";
import { DomainError } from "./errors";
export type Direction = "DEBIT" | "CREDIT";
export interface WalletState {
  id: string;
  playerId: string;
  balance: Money;
  version: number;
}
export class WalletLedgerEntry {
  private constructor(
    public readonly walletId: string,
    public readonly transactionId: string,
    public readonly direction: Direction,
    public readonly money: Money,
    public readonly balanceBefore: Money,
    public readonly balanceAfter: Money,
  ) {
    Object.freeze(this);
  }
  static create(s: {
    walletId: string;
    transactionId: string;
    direction: Direction;
    money: Money;
    balanceBefore: Money;
    balanceAfter: Money;
  }): WalletLedgerEntry {
    const e = new WalletLedgerEntry(
      s.walletId,
      s.transactionId,
      s.direction,
      s.money,
      s.balanceBefore,
      s.balanceAfter,
    );
    if (
      !s.money.isPositive() ||
      s.balanceBefore.isNegative() ||
      s.balanceAfter.isNegative() ||
      !e.isBalanced()
    )
      throw new DomainError("UNBALANCED_LEDGER");
    return e;
  }
  static rehydrate(s: {
    walletId: string;
    transactionId: string;
    direction: Direction;
    money: Money;
    balanceBefore: Money;
    balanceAfter: Money;
  }): WalletLedgerEntry {
    return new WalletLedgerEntry(
      s.walletId,
      s.transactionId,
      s.direction,
      s.money,
      s.balanceBefore,
      s.balanceAfter,
    );
  }
  isBalanced(): boolean {
    return (
      this.direction === "CREDIT"
        ? this.balanceBefore.add(this.money)
        : this.balanceBefore.subtract(this.money)
    ).equals(this.balanceAfter);
  }
}
export class Wallet {
  private constructor(
    public readonly id: string,
    public readonly playerId: string,
    private _balance: Money,
    private _version: number,
  ) {}
  static open(s: {
    id: string;
    playerId: string;
    initialBalance: Money;
  }): Wallet {
    if (s.initialBalance.isNegative())
      throw new DomainError("NEGATIVE_BALANCE");
    return new Wallet(s.id, s.playerId, s.initialBalance, 1);
  }
  static rehydrate(s: WalletState): Wallet {
    return new Wallet(s.id, s.playerId, s.balance, s.version);
  }
  get balance(): Money {
    return this._balance;
  }
  get version(): number {
    return this._version;
  }
  move(
    transactionId: string,
    money: Money,
    direction: Direction,
    reversal = false,
  ): WalletLedgerEntry | undefined {
    if (!money.isPositive()) throw new DomainError("POSITIVE_AMOUNT_REQUIRED");
    const before = this._balance;
    const after =
      direction === "CREDIT" ? before.add(money) : before.subtract(money);
    if (after.isNegative())
      throw new DomainError(
        reversal ? "REVERSAL_INSUFFICIENT_FUNDS" : "INSUFFICIENT_FUNDS",
      );
    const entry = WalletLedgerEntry.create({
      walletId: this.id,
      transactionId,
      direction,
      money,
      balanceBefore: before,
      balanceAfter: after,
    });
    this._balance = after;
    this._version++;
    return entry;
  }
}
