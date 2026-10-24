import { DeleteMessageCommand, GetQueueAttributesCommand, ReceiveMessageCommand, SendMessageCommand } from '@aws-sdk/client-sqs';
import { Injectable } from '@nestjs/common';
import { envelopeSchema, hash } from '../application/contracts';
import { WageringService } from '../application/wagering.service';
import { ConflictError, DomainError } from '../domain/errors';
import { dataSource, transaction } from './database';
import { deadLetterQueueName, eventQueueName, queueUrl, requestQueueName, sqs } from './queues';
import { count, dlqMessages, log, outboxLag } from './telemetry';

const sleep=(ms:number,signal:AbortSignal)=>new Promise<void>(resolve=>{if(signal.aborted)return resolve();const t=setTimeout(resolve,ms);signal.addEventListener('abort',()=>{clearTimeout(t);resolve();},{once:true});});

@Injectable()
export class Workers {
  private readonly aborter=new AbortController();
  private tasks:Promise<void>[]=[];
  constructor(private readonly service:WageringService) {}
  start():void {
    this.tasks=[this.consume(),this.publishOutbox(),this.retryReferences(),this.auditDeadLetters(),this.measureDlq()];
  }
  async stop():Promise<void> { this.aborter.abort(); await Promise.allSettled(this.tasks); }

  private async consume():Promise<void> {
    const signal=this.aborter.signal;
    while(!signal.aborted) {
      try {
        const response=await sqs.send(new ReceiveMessageCommand({QueueUrl:queueUrl(requestQueueName),WaitTimeSeconds:10,MaxNumberOfMessages:10,VisibilityTimeout:60,MessageSystemAttributeNames:['ApproximateReceiveCount']}));
        for(const message of response.Messages??[]) {
          if(!message.Body||!message.ReceiptHandle) continue;
          try {
            const parsed=envelopeSchema.parse(JSON.parse(message.Body));
            log('sqs_message_processing_start',{messageId:message.MessageId,envelopeMessageId:parsed.messageId});
            await this.service.consumeEnvelope(parsed);
            log('sqs_message_processing_commit',{messageId:message.MessageId,envelopeMessageId:parsed.messageId});
            // A transação no banco (estado financeiro + inbox + outbox) foi confirmada antes de confirmar o recebimento.
            await sqs.send(new DeleteMessageCommand({QueueUrl:queueUrl(requestQueueName),ReceiptHandle:message.ReceiptHandle}));
            count('message','acked');
          } catch(error) {
            if(error instanceof DomainError && !(error instanceof ConflictError)) {

              // Remove mensagens com erro de domínio não classificado como conflito.
              // Erros de validação do envelope seguem o fluxo de retry.
                
              await sqs.send(new DeleteMessageCommand({QueueUrl:queueUrl(requestQueueName),ReceiptHandle:message.ReceiptHandle}));
              count('message','invalid');
              log('sqs_message_invalid',{messageId:message.MessageId,code:error.code});
            } else {
              count('message','retry');
              log('sqs_message_retry',{messageId:message.MessageId,receiveCount:message.Attributes?.ApproximateReceiveCount,error:String(error)});
              // Mantém na fila. A expiração da invisibilidade permite nova tentativa; o SQS encaminha para a DLQ.
            }
          }
        }
      } catch(error) { if(!signal.aborted) { count('message','receive_error'); log('sqs_receive_error',{error:String(error)}); await sleep(1000,signal); } }
    }
  }

  private async publishOutbox():Promise<void> {
    const signal=this.aborter.signal;
    while(!signal.aborted) {
      try {
        const rows = await transaction(async q => {
  const result = await q.query(
    `WITH claimed AS (
      SELECT id
      FROM outbox_messages
      WHERE published_at IS NULL
        AND next_attempt_at <= now()
        AND (locked_until IS NULL OR locked_until < now())
      ORDER BY occurred_at
      LIMIT 20
      FOR UPDATE SKIP LOCKED
    )
    UPDATE outbox_messages o
    SET locked_until = now() + interval '45 seconds'
    FROM claimed c
    WHERE o.id = c.id
    RETURNING o.id, o.aggregate_id, o.event_type,
              o.payload, o.attempts,
              EXTRACT(EPOCH FROM (now() - o.occurred_at)) AS age`,
    [],
    true,
  );

  return result.records;
});

        if(!rows.length) { await sleep(500,signal); continue; }
        for(const row of rows) {
          try {
            const request=row.event_type==='WagerTransactionRequested';
            // O JSONB do PostgreSQL normalmente é retornado como objeto, mas mantém a
            // requisição SQS explícita e nunca permite que um valor indefinido chegue ao
            // validador de mensagens FIFO do LocalStack.
            const body=typeof row.payload==='string'?row.payload:JSON.stringify(row.payload??{});
            const aggregateId=String(row.aggregate_id??'');
            const eventId=String(row.id??'');
            if(!body || !aggregateId || !eventId) throw new Error('Invalid outbox message fields');
            await sqs.send(new SendMessageCommand({QueueUrl:queueUrl(request?requestQueueName:eventQueueName),MessageBody:body,MessageGroupId:aggregateId,MessageDeduplicationId:eventId}));
            await dataSource.query(`UPDATE outbox_messages SET published_at=now(),locked_until=NULL WHERE id=$1`,[row.id]);
            count('outbox','published');
          } catch(error) {
            // Mantém a contagem de tentativas no SQL: os drivers podem retornar colunas numéricas como
            // strings, e um campo inválido ou ausente nunca deve se tornar NaN.
            const attempts=Number.isFinite(Number(row.attempts))?Math.max(0,Number(row.attempts))+1:1;
            const delay=Math.min(1000*2**Math.min(attempts,10),60000);
            await dataSource.query(`UPDATE outbox_messages SET attempts=$2,next_attempt_at=now()+($3 || ' milliseconds')::interval,locked_until=NULL WHERE id=$1`,[row.id,attempts,delay]);
            count('outbox','retry'); log('outbox_publish_retry',{eventId:row.id,eventType:row.event_type,attempts,error:String(error)});
          }
        }
        const oldest=(await dataSource.query(`SELECT EXTRACT(EPOCH FROM (now()-MIN(occurred_at))) AS age FROM outbox_messages WHERE published_at IS NULL`))[0];
        outboxLag.set(Math.max(0,Number(oldest?.age??0)));
      } catch(error) { if(!signal.aborted) { log('outbox_poll_error',{error:String(error)}); await sleep(1000,signal); } }
    }
  }

  private async retryReferences():Promise<void> {
    const signal=this.aborter.signal;
    while(!signal.aborted) {
      try { await this.service.retryPendingReferences(); }
      catch(error) { log('reference_retry_error',{error:String(error)}); }
      await sleep(1000,signal);
    }
  }

  private async measureDlq():Promise<void> {
    const signal=this.aborter.signal;
    while(!signal.aborted) {
      try {
        const r=await sqs.send(new GetQueueAttributesCommand({QueueUrl:queueUrl(deadLetterQueueName),AttributeNames:['ApproximateNumberOfMessages','ApproximateNumberOfMessagesNotVisible']}));
        dlqMessages.set(Number(r.Attributes?.ApproximateNumberOfMessages??0)+Number(r.Attributes?.ApproximateNumberOfMessagesNotVisible??0));
      } catch(error) { log('dlq_metric_error',{error:String(error)}); }
      await sleep(10000,signal);
    }
  }

 private async auditDeadLetters(): Promise<void> {
  const signal = this.aborter.signal;

  while (!signal.aborted) {
    try {
      const response = await sqs.send(
        new ReceiveMessageCommand({
          QueueUrl: queueUrl(deadLetterQueueName),
          WaitTimeSeconds: 10,
          MaxNumberOfMessages: 10,
          VisibilityTimeout: 60,
        }),
      );

      for (const message of response.Messages ?? []) {
        if (!message.Body || !message.ReceiptHandle) {
          continue;
        }

        let parsed: ReturnType<typeof envelopeSchema.parse>;

        try {
          parsed = envelopeSchema.parse(JSON.parse(message.Body));
        } catch {
          log("dlq_message_invalid", {
            messageId: message.MessageId,
            reason: "INVALID_ENVELOPE",
          });

          // Preserva as mensagens malformadas na DLQ para inspeção.
          continue;
        }

        try {
          await this.service.failFromDeadLetter(parsed);

          // Confirma o recebimento somente após confirmar a transação de auditoria.
          await sqs.send(
            new DeleteMessageCommand({
              QueueUrl: queueUrl(deadLetterQueueName),
              ReceiptHandle: message.ReceiptHandle,
            }),
          );

          count("dlq", "audited");
        } catch (error) {
          log("dlq_audit_error", {
            messageId: parsed.messageId,
            code:
              error instanceof DomainError
                ? error.code
                : "DLQ_AUDIT_FAILED",
          });

          // Mantém a mensagem se a auditoria ou a confirmação de recebimento falhar.
          // Seu tempo de invisibilidade expira, permitindo outra tentativa.
        }
      }
    } catch (error) {
      if (!signal.aborted) {
        log("dlq_receive_error", {
          error: String(error),
        });

        await sleep(1000, signal);
      }
    }
  }
}
}
