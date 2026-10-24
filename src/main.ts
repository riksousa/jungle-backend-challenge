import "reflect-metadata";
import { NestFactory } from "@nestjs/core";
import { AppModule } from "./api/app.module";
import { ApiErrorFilter } from "./api/http";
import { connectDatabase, closeDatabase } from "./infrastructure/database";
import { ensureQueues } from "./infrastructure/queues";
import { Workers } from "./infrastructure/workers";
import { log } from "./infrastructure/telemetry";

async function bootstrap(): Promise<void> {
  await connectDatabase();
  await ensureQueues();
  const app = await NestFactory.create(AppModule, { bufferLogs: true });
  app.useGlobalFilters(new ApiErrorFilter());
  app.enableShutdownHooks();
  const workers = app.get(Workers);
  if (process.env.WORKERS_ENABLED !== "false") workers.start();
  app.getHttpAdapter().getInstance().set("trust proxy", 1);
  const port = Number(process.env.PORT ?? 3000);
  await app.listen(port, "0.0.0.0");
  log("application_started", { port });
  app.getHttpServer().once("close", async () => {
    await workers.stop();
    await closeDatabase();
  });
}
bootstrap().catch((error) => {
  log("application_start_failed", { error: String(error) });
  process.exitCode = 1;
});
