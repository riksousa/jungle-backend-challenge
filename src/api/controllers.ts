import {
  BadRequestException,
  Body,
  Controller,
  Get,
  Headers,
  Header,
  HttpCode,
  HttpStatus,
  Param,
  Post,
  Query,
  ServiceUnavailableException,
} from "@nestjs/common";
import { z } from "zod";
import { commandSchema, walletSchema } from "../application/contracts";
import { WageringService } from "../application/wagering.service";
import { dataSource } from "../infrastructure/database";
import { checkSqs } from "../infrastructure/queues";
import { registry } from "../infrastructure/telemetry";

function parse<T>(schema: z.ZodType<T>, value: unknown): T {
  const result = schema.safeParse(value);
  if (!result.success)
    throw new BadRequestException({
      code: "INVALID_REQUEST",
      issues: result.error.issues.map((i) => ({
        path: i.path,
        message: i.message,
      })),
    });
  return result.data;
}

@Controller()
export class ApiController {
  constructor(private readonly service: WageringService) {}
  @Post("wallets")
  @HttpCode(HttpStatus.CREATED)
  createWallet(@Body() body: unknown) {
    const input = parse(walletSchema, body);
    return this.service.createWallet(input.playerId, input.initialBalance);
  }

  @Get("wallets/:walletId") getWallet(@Param("walletId") id: string) {
    return this.service.getWallet(id);
  }
  @Get("wallets/:walletId/ledger") ledger(
    @Param("walletId") id: string,
    @Query("cursor") cursor?: string,
    @Query("limit") limitText?: string,
  ) {
    const limit = limitText === undefined ? 50 : Number(limitText);
    if (!Number.isInteger(limit) || limit < 1 || limit > 100)
      throw new BadRequestException({ code: "INVALID_REQUEST" });
    return this.service.listLedger(id, cursor, limit);
  }
  @Post("wagering/transactions")
  @HttpCode(HttpStatus.ACCEPTED)
  submit(@Body() body: unknown, @Headers("idempotency-key") key?: string) {
    if (!key || key.length > 200)
      throw new BadRequestException({ code: "IDEMPOTENCY_KEY_REQUIRED" });
    return this.service.submit({
      ...parse(commandSchema, body),
      idempotencyKey: key,
    });
  }
  @Get("wagering/transactions/:transactionId") transaction(
    @Param("transactionId") id: string,
  ) {
    return this.service.getTransaction(id);
  }
  @Get("providers/:providerId/wagering/transactions/:externalTransactionId")
  external(
    @Param("providerId") p: string,
    @Param("externalTransactionId") e: string,
  ) {
    return this.service.getExternal(p, e);
  }
  @Post("wallets/:walletId/reconciliation") reconcile(
    @Param("walletId") id: string,
  ) {
    return this.service.reconcile(id);
  }
}

@Controller("health")
export class HealthController {
  @Get("live") live() {
    return { status: "ok" };
  }
  @Get("ready") async ready() {
    try {
      await dataSource.query("SELECT 1");
      await checkSqs();
      return { status: "ready" };
    } catch {
      throw new ServiceUnavailableException({ status: "not_ready" });
    }
  }
}

@Controller()
export class MetricsController {
  @Get("metrics")
  @Header("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
  async metrics() {
    return registry.metrics();
  }
}
