import Decimal from "decimal.js";
import { DomainError } from "./errors";

export interface MoneyProps {
  amount: string;
  currency: string;
}
const ExactDecimal = Decimal.clone({ precision: 40 });
const MAX = new ExactDecimal("999999999999999999.99"); // PostgreSQL NUMERIC(20,2)
const INPUT = /^\d{1,18}\.\d{2}$/;
const CURRENCIES = new Set(["BRL", "USD", "EUR"]);

/** Objeto de valor imutável. Os valores monetários são transmitidos entre a API e o banco de dados como strings decimais. */
export class Money {
  private constructor(
    private readonly value: Decimal,
    public readonly currency: string,
  ) {
    Object.freeze(this);
  }

  static from(props: MoneyProps): Money {
    if (
      !props ||
      typeof props.amount !== "string" ||
      !INPUT.test(props.amount)
    ) {
      throw new DomainError("INVALID_AMOUNT");
    }
    if (typeof props.currency !== "string" || !CURRENCIES.has(props.currency)) {
      throw new DomainError("INVALID_CURRENCY");
    }
    return Money.make(new ExactDecimal(props.amount), props.currency);
  }

  /** Reconstitui um valor já protegido pelas restrições do banco de dados. */
  static rehydrate(props: MoneyProps): Money {
    return Money.make(new ExactDecimal(props.amount), props.currency);
  }

  static zero(currency: string): Money {
    return Money.from({ amount: "0.00", currency });
  }

  private static make(value: Decimal, currency: string): Money {
    if (!value.isFinite() || value.abs().gt(MAX) || value.decimalPlaces() > 2) {
      throw new DomainError("INVALID_AMOUNT");
    }
    return new Money(value, currency);
  }

  private assertSameCurrency(other: Money): void {
    if (this.currency !== other.currency)
      throw new DomainError("CURRENCY_MISMATCH");
  }

  add(other: Money): Money {
    this.assertSameCurrency(other);
    return Money.make(this.value.plus(other.value), this.currency);
  }
  subtract(other: Money): Money {
    this.assertSameCurrency(other);
    return Money.make(this.value.minus(other.value), this.currency);
  }
  negate(): Money {
    return Money.make(this.value.negated(), this.currency);
  }
  isZero(): boolean {
    return this.value.isZero();
  }
  isPositive(): boolean {
    return this.value.gt(0);
  }
  isNegative(): boolean {
    return this.value.lt(0);
  }
  isLessThan(other: Money): boolean {
    this.assertSameCurrency(other);
    return this.value.lt(other.value);
  }
  equals(other: Money): boolean {
    return this.currency === other.currency && this.value.eq(other.value);
  }
  toString(): string {
    return this.value.toFixed(2);
  }
  toJSON(): MoneyProps {
    return { amount: this.toString(), currency: this.currency };
  }
}
