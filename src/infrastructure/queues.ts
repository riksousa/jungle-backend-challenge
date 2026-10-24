import {
  CreateQueueCommand,
  GetQueueAttributesCommand,
  GetQueueUrlCommand,
  SQSClient,
} from "@aws-sdk/client-sqs";

export const requestQueueName =
  process.env.SQS_REQUEST_QUEUE ?? "wager-transactions.fifo";
export const deadLetterQueueName =
  process.env.SQS_DLQ ?? "wager-transactions-dlq.fifo";
export const eventQueueName =
  process.env.SQS_EVENT_QUEUE ?? "wager-events.fifo";
export const sqs = new SQSClient({
  region: process.env.AWS_REGION ?? "us-east-1",
  endpoint: process.env.SQS_ENDPOINT || undefined,
  credentials: {
    accessKeyId: process.env.AWS_ACCESS_KEY_ID ?? "test",
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY ?? "test",
  },
});
const urls = new Map<string, string>();

export async function ensureQueues(): Promise<void> {
  const dlq = (
    await sqs.send(
      new CreateQueueCommand({
        QueueName: deadLetterQueueName,
        Attributes: { FifoQueue: "true", ContentBasedDeduplication: "false" },
      }),
    )
  ).QueueUrl!;
  await sqs.send(
    new CreateQueueCommand({
      QueueName: requestQueueName,
      Attributes: {
        FifoQueue: "true",
        ContentBasedDeduplication: "false",
        VisibilityTimeout: "60",
        RedrivePolicy: JSON.stringify({
          deadLetterTargetArn: await queueArn(dlq),
          maxReceiveCount: "5",
        }),
      },
    }),
  );
  await sqs.send(
    new CreateQueueCommand({
      QueueName: eventQueueName,
      Attributes: { FifoQueue: "true", ContentBasedDeduplication: "false" },
    }),
  );
  urls.set(
    requestQueueName,
    (await sqs.send(new GetQueueUrlCommand({ QueueName: requestQueueName })))
      .QueueUrl!,
  );
  urls.set(
    eventQueueName,
    (await sqs.send(new GetQueueUrlCommand({ QueueName: eventQueueName })))
      .QueueUrl!,
  );
  urls.set(deadLetterQueueName, dlq);
}
async function queueArn(url: string): Promise<string> {
  const r = await sqs.send(
    new GetQueueAttributesCommand({
      QueueUrl: url,
      AttributeNames: ["QueueArn"],
    }),
  );
  if (!r.Attributes?.QueueArn) throw new Error("Could not resolve SQS DLQ ARN");
  return r.Attributes.QueueArn;
}
export function queueUrl(name: string): string {
  const v = urls.get(name);
  if (!v) throw new Error(`Queue ${name} has not been initialized`);
  return v;
}
export async function checkSqs(): Promise<void> {
  await sqs.send(new GetQueueUrlCommand({ QueueName: requestQueueName }));
}
