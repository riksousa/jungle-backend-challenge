export type FailureCode =
  | "INVALID_AMOUNT"
  | "INVALID_CURRENCY"
  | "CURRENCY_MISMATCH"
  | "INSUFFICIENT_FUNDS"
  | "REVERSAL_INSUFFICIENT_FUNDS"
  | "WALLET_NOT_FOUND"
  | "PLAYER_WALLET_MISMATCH"
  | "REFERENCE_NOT_FOUND"
  | "REFERENCE_NOT_PROCESSED"
  | "REFERENCE_MISMATCH"
  | "INVALID_REFERENCE_KIND"
  | "REFERENCE_AMOUNT_MISMATCH"
  | "ALREADY_REVERSED"
  | "INVALID_OPERATION_AMOUNT"
  | "REFERENCE_REQUIRED"
  | "SELF_REFERENCE"
  | "PAYLOAD_CONFLICT"
  | "INVALID_STATE"
  | "WALLET_ALREADY_EXISTS"
  | "INVALID_REQUEST"
  | "INFRASTRUCTURE_RETRIES_EXHAUSTED";

export class DomainError extends Error {
  constructor(public readonly code: string) {
    super(code);
    this.name = "DomainError";
  }
}

export class ConflictError extends DomainError {}
export class NotFoundError extends DomainError {}
