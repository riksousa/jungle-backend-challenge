import { describe, expect, test } from "bun:test";
import "reflect-metadata";
import { WageringService } from "../src/application/wagering.service";
import { DomainError } from "../src/domain/errors";
import { Money } from "../src/domain/money";
import { WagerTransaction } from "../src/domain/transaction";
import { Wallet } from "../src/domain/wallet";
import { Workers } from "../src/infrastructure/workers";

describe("Nest worker wiring", () => {
  test("registers WageringService as the worker constructor dependency", () => {
    expect(Reflect.getMetadata("design:paramtypes", Workers)).toEqual([
      WageringService,
    ]);
  });
});

describe("Money", () => {
  test("adds decimal values exactly and serializes with two places", () => {
    expect(
      Money.from({ amount: "0.10", currency: "BRL" })
        .add(Money.from({ amount: "0.20", currency: "BRL" }))
        .toJSON(),
    ).toEqual({ amount: "0.30", currency: "BRL" });
  });
  test("rejects invalid formats instead of rounding", () => {
    for (const amount of [
      "25.001",
      "-1.00",
      "1e3",
      "NaN",
      "Infinity",
      "",
      ".50",
      "1000000000000000000.00",
    ]) {
      expect(() => Money.from({ amount, currency: "BRL" })).toThrow(
        DomainError,
      );
    }
  });
  test("prevents arithmetic across currencies", () => {
    expect(() =>
      Money.from({ amount: "1.00", currency: "BRL" }).add(
        Money.from({ amount: "1.00", currency: "USD" }),
      ),
    ).toThrow("CURRENCY_MISMATCH");
  });
  test("keeps the value object immutable and supports negative results internally", () => {
    const original = Money.from({ amount: "10.00", currency: "BRL" }),
      negative = original.negate();
    expect(original.toString()).toBe("10.00");
    expect(negative.toString()).toBe("-10.00");
  });
});

describe("Wallet domain", () => {
  test("starts with version 1 and creates balanced debit entries", () => {
    const wallet = Wallet.open({
      id: "w",
      playerId: "p",
      initialBalance: Money.from({ amount: "20.00", currency: "BRL" }),
    });
    const entry = wallet.move(
      "t",
      Money.from({ amount: "5.00", currency: "BRL" }),
      "DEBIT",
    );
    expect(wallet.balance.toString()).toBe("15.00");
    expect(wallet.version).toBe(2);
    expect(entry?.isBalanced()).toBe(true);
  });
  test("rejects an overdraft without changing the aggregate", () => {
    const wallet = Wallet.open({
      id: "w",
      playerId: "p",
      initialBalance: Money.from({ amount: "2.00", currency: "BRL" }),
    });
    expect(() =>
      wallet.move(
        "t",
        Money.from({ amount: "3.00", currency: "BRL" }),
        "DEBIT",
      ),
    ).toThrow("INSUFFICIENT_FUNDS");
    expect(wallet.balance.toString()).toBe("2.00");
    expect(wallet.version).toBe(1);
  });
});

describe("WagerTransaction domain", () => {
  const base = {
    providerId: "p",
    externalTransactionId: "t",
    walletId: "w",
    playerId: "player",
    roundId: "r",
    gameId: "g",
    money: { amount: "1.00", currency: "BRL" },
  };
  test("accepts balance-affecting kinds and LOSS only at zero", () => {
    for (const kind of ["BET", "WIN", "REFUND", "ROLLBACK"] as const) {
      const command = {
        ...base,
        kind,
        ...(["REFUND", "ROLLBACK"].includes(kind)
          ? { referenceExternalTransactionId: "ref" }
          : {}),
      };
      expect(WagerTransaction.create(command).status).toBe("PENDING");
    }
    expect(
      WagerTransaction.create({
        ...base,
        kind: "LOSS",
        money: { amount: "0.00", currency: "BRL" },
      }).affectsBalance(),
    ).toBe(false);
    expect(() => WagerTransaction.create({ ...base, kind: "LOSS" })).toThrow(
      "INVALID_OPERATION_AMOUNT",
    );
  });
  test("requires reversal references and rejects self references", () => {
    expect(() => WagerTransaction.create({ ...base, kind: "REFUND" })).toThrow(
      "REFERENCE_REQUIRED",
    );
    expect(() =>
      WagerTransaction.create({
        ...base,
        kind: "ROLLBACK",
        referenceExternalTransactionId: "t",
      }),
    ).toThrow("SELF_REFERENCE");
  });
});
