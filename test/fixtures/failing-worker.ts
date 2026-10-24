/// <reference types="node" />
/// <reference types="bun" />

import "reflect-metadata";
import { ChangeMessageVisibilityCommand } from "@aws-sdk/client-sqs";
import { envelopeSchema } from "../../src/application/contracts";
import { WageringService } from "../../src/application/wagering.service";
import {
  closeDatabase,
  connectDatabase,
} from "../../src/infrastructure/database";
import {
  deadLetterQueueName,
  ensureQueues,
  queueUrl,
  requestQueueName,
  sqs,
} from "../../src/infrastructure/queues";
import { Workers } from "../../src/infrastructure/workers";

function report(event: string, messageId?: string): void {
  console.log(JSON.stringify({ event, messageId }));
}

class FailingService extends WageringService {
  override async consumeEnvelope(input: unknown): Promise<never> {
    const envelope = envelopeSchema.parse(input);

    report("test_forced_failure", envelope.messageId);

    // Simula uma falha de processamento antes de confirmar qualquer alteração financeira.
    throw new Error("Controlled processing failure");
  }
}

async function start(): Promise<void> {
  if (!process.env.DATABASE_URL?.endsWith("_test")) {
    throw new Error("This fixture requires an isolated test database");
  }

  await connectDatabase();
  await ensureQueues();

  // Reduz o tempo de invisibilidade apenas nesta simulação para testar novas tentativas rapidamente.
  sqs.middlewareStack.add(
    (next, context) => async (args) => {
      const result = await next(args);

      if (context.commandName !== "ReceiveMessageCommand") {
        return result;
      }

      const input = args.input as { QueueUrl?: string };

      const output = result.output as {
        Messages?: {
          Body?: string;
          ReceiptHandle?: string;
        }[];
      };

      if (input.QueueUrl === queueUrl(requestQueueName)) {
        for (const message of output.Messages ?? []) {
          if (!message.ReceiptHandle) continue;

          await sqs.send(
            new ChangeMessageVisibilityCommand({
              QueueUrl: queueUrl(requestQueueName),
              ReceiptHandle: message.ReceiptHandle,
              VisibilityTimeout: 1,
            }),
          );
        }
      }

      if (input.QueueUrl === queueUrl(deadLetterQueueName)) {
        for (const message of output.Messages ?? []) {
          try {
            const envelope = envelopeSchema.parse(
              JSON.parse(message.Body ?? ""),
            );

            report("test_dlq_delivery", envelope.messageId);
          } catch {
            // O auditor padrão da DLQ trata os envelopes inválidos.
          }
        }
      }

      return result;
    },
    {
      step: "initialize",
      name: "testRetryVisibility",
    },
  );

  const workers = new Workers(new FailingService());
  workers.start();

  report("worker_ready");

  let stopping = false;

  const stop = async (): Promise<void> => {
    if (stopping) return;
    stopping = true;

    await workers.stop();
    await closeDatabase();
    process.exit(0);
  };

  process.once("SIGTERM", () => void stop());
  process.once("SIGINT", () => void stop());
}

start().catch((error) => {
  console.error(error);
  process.exit(1);
});
