# Roteiro para estudar o projeto

Vamos estudar o sistema seguindo uma aposta do começo ao fim. A ideia é entender por que cada parte existe e depois praticar como explicar as escolhas em uma entrevista.

## Ordem das aulas

1. **O caminho de uma requisição:** `src/api/controllers.ts` valida a entrada e chama `WageringService.submit`.
2. **Dinheiro e regras do domínio:** `src/domain/money.ts`, `wallet.ts` e `transaction.ts` protegem formato, saldo e transições.
3. **Persistência e idempotência:** `src/application/wagering.service.ts` e as constraints em `src/infrastructure/migrations/initial.ts` garantem replay seguro.
4. **Outbox e SQS:** a intenção e o evento são gravados no mesmo commit; `Workers.publishOutbox` entrega a mensagem depois.
5. **Consumer, inbox e ledger:** `Workers.consume` chama `process`, que grava inbox, saldo, ledger e novos eventos em uma única transação.
6. **Concorrência e falhas:** locks por wallet, `SKIP LOCKED`, DLQ e retomada após crash.
7. **Testes:** começar por `test/unit.test.ts`, seguir para PostgreSQL em `test/integration.test.ts` e terminar no teste de sistema com múltiplos workers.

## Perguntas para praticar

- Por que a API responde `202` antes de calcular o saldo final?
- O que impede que o mesmo evento debite a wallet duas vezes?
- O que acontece se o processo cair depois do commit e antes do ack do SQS?
- Por que o saldo e o ledger são atualizados na mesma transação?
- Como duas apostas simultâneas evitam saldo negativo sem bloquear todas as wallets?

## Preparação no Windows

Com Docker Desktop aberto, rode `docker compose up --build`. Em outro terminal, use `bun run test:unit`, `bun run test:integration` e `bun run test:system`. Os testes de integração e sistema usam o banco isolado `jungle_test` definido em `.env.test`.
