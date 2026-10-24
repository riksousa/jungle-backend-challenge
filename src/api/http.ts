import {
  ArgumentsHost,
  Catch,
  ExceptionFilter,
  HttpException,
  HttpStatus,
} from "@nestjs/common";
import { DomainError } from "../domain/errors";
import { count, log, sqlCode } from "../infrastructure/telemetry";

@Catch()
export class ApiErrorFilter implements ExceptionFilter {
  catch(error: unknown, host: ArgumentsHost): void {
    const response = host.switchToHttp().getResponse();
    if (error instanceof DomainError) {
      const status =
        error.code === "WALLET_NOT_FOUND" || error.code === "INVALID_REQUEST"
          ? HttpStatus.NOT_FOUND
          : [
                "PAYLOAD_CONFLICT",
                "WALLET_ALREADY_EXISTS",
                "ALREADY_REVERSED",
              ].includes(error.code)
            ? HttpStatus.CONFLICT
            : [
                  "INSUFFICIENT_FUNDS",
                  "REVERSAL_INSUFFICIENT_FUNDS",
                  "REFERENCE_NOT_PROCESSED",
                  "REFERENCE_NOT_FOUND",
                ].includes(error.code)
              ? HttpStatus.UNPROCESSABLE_ENTITY
              : HttpStatus.BAD_REQUEST;
      response.status(status).json({ statusCode: status, code: error.code });
      return;
    }
    if (error instanceof HttpException) {
      response.status(error.getStatus()).json(error.getResponse());
      return;
    }
    if (["40P01", "55P03", "40001"].includes(sqlCode(error)))
      count("concurrency", "lock_conflict");
    log("http_unhandled_error", { error: String(error) });
    response
      .status(HttpStatus.INTERNAL_SERVER_ERROR)
      .json({ statusCode: 500, code: "INTERNAL_ERROR" });
  }
}
