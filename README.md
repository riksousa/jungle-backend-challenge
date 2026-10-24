# Jungle Backend Challenge

Processador de transações de apostas com NestJS, Bun, PostgreSQL, migrations do TypeORM e SQS emulado pelo LocalStack.

Os valores monetários são representados como strings decimais na API e tratados pelo objeto de valor imutável `Money`, utilizando `decimal.js` no domínio.

## Pré-requisitos

- Docker Desktop em execução, com Docker Compose.
- Bun instalado para executar a aplicação ou os testes diretamente no Windows.
- Portas 3000, 5432 e 4566 disponíveis para os serviços do Compose.

Os comandos abaixo devem ser executados na pasta do projeto.

## Executar pelo Docker

Para construir a imagem e iniciar a aplicação, o PostgreSQL e o LocalStack:

```powershell
docker compose up -d --build
```

A aplicação executa as migrations, inicializa as filas FIFO e inicia os workers.

Confira o estado dos serviços:

```powershell
docker compose ps
```

Verifique a disponibilidade da aplicação:

```powershell
Invoke-RestMethod http://localhost:3000/health/ready
```

Resposta esperada:

```json
{
  "status": "ready"
}
```

Endpoints operacionais:

| Endpoint | Finalidade |
|---|---|
| `GET /health/live` | Verificar se a aplicação está em execução |
| `GET /health/ready` | Verificar a disponibilidade das dependências |
| `GET /metrics` | Consultar as métricas no formato Prometheus |

Para acompanhar os logs:

```powershell
docker compose logs -f app
```

Use `Ctrl+C` para sair do acompanhamento dos logs. Os containers continuam em execução.

Para parar e remover os containers:

```powershell
docker compose down
```

Os dados do PostgreSQL permanecem no volume. O comando `docker compose down -v` também remove o volume e apaga o banco local.

## Executar pelo Bun no Windows

Mantenha PostgreSQL e LocalStack no Docker. Se o serviço `app` estiver em execução, pare-o para liberar a porta 3000:

```powershell
docker compose stop app
docker compose up -d postgres localstack
bun install --frozen-lockfile
```

Crie o arquivo `.env` somente se ele ainda não existir:

```powershell
if (!(Test-Path .env)) {
  Copy-Item .env.example .env
}
```

As configurações locais utilizam PostgreSQL em `localhost:5432` e o endpoint SQS em `http://localhost:4566`.

Compile e inicie a aplicação:

```powershell
bun run build
bun run start
```

Use `Ctrl+C` para encerrar a aplicação executada nesse terminal.

## Testes automatizados

Os testes com infraestrutura exigem PostgreSQL e LocalStack em execução:

```powershell
docker compose up -d postgres localstack
```

Instale as dependências e confira os tipos da aplicação e dos testes:

```powershell
bun install --frozen-lockfile
bunx tsc --noEmit -p tsconfig.test.json
```

Execute a suíte completa:

```powershell
bun run test:all
```

Para executar cada etapa separadamente:

```powershell
bun run test:unit
bun run test:integration
bun run test:system
bun run test:dlq
```

| Etapa | Cobertura principal |
|---|---|
| Unitários | Dinheiro decimal, regras de Wallet e WagerTransaction e dependência do serviço no worker |
| Integração | PostgreSQL, idempotência, concorrência, regras financeiras, reconciliação e rollback quando a persistência de eventos falha |
| Sistema | Três processos worker, recuperação de reserva abandonada da outbox, entrada direta por SQS, auditoria da DLQ e reentrega após commit sem confirmação |
| Retry/DLQ | Novas tentativas após falhas de processamento e encaminhamento automático para a DLQ |

O comando `bun run test` executa somente os testes unitários. Para executar todas as etapas, utilize `bun run test:all`.

### Isolamento dos testes

Os testes com infraestrutura utilizam o banco `jungle_test` e filas separadas das utilizadas pela aplicação. A configuração está em `.env.test`.

Esses testes limpam suas tabelas de teste. Execute as suítes sequencialmente, como definido em `test:all`, sem outras execuções simultâneas utilizando os mesmos recursos.

Não configure `.env.test` para o banco de desenvolvimento.

### Criação do banco de testes

O banco `jungle_test` é criado pelo script de inicialização do PostgreSQL na primeira criação do volume.

Se o volume já existia antes da inclusão desse script, confira os bancos disponíveis:

```powershell
docker compose exec postgres psql -U jungle -d jungle -c "\l"
```

Se `jungle_test` não existir, crie-o:

```powershell
docker compose exec postgres psql -U jungle -d jungle -c "CREATE DATABASE jungle_test;"
```

As migrations são aplicadas durante a inicialização dos testes. Não é necessário apagar o volume para criar o banco de testes.

## Exemplo da API no Postman

Com a aplicação em execução, utilize a URL base:

```text
http://localhost:3000
```

### 1. Criar uma carteira

Configure a requisição:

```http
POST http://localhost:3000/wallets
Content-Type: application/json
```

Na aba **Body**, selecione **raw** e o formato **JSON**:

```json
{
  "playerId": "0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1",
  "initialBalance": {
    "amount": "100.00",
    "currency": "BRL"
  }
}
```

Guarde o campo `id` retornado. Ele será utilizado como `walletId` nas próximas requisições.

### 2. Enviar uma aposta

Configure a requisição e os headers:

```http
POST http://localhost:3000/wagering/transactions
Content-Type: application/json
Idempotency-Key: provider-a:bet-001
```

Substitua `WALLET_ID_AQUI` pelo `id` da carteira criada:

```json
{
  "providerId": "provider-a",
  "externalTransactionId": "bet-001",
  "playerId": "0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1",
  "walletId": "WALLET_ID_AQUI",
  "roundId": "round-1",
  "gameId": "game-1",
  "kind": "BET",
  "money": {
    "amount": "25.00",
    "currency": "BRL"
  }
}
```

Uma nova solicitação aceita retorna HTTP `202` com status `PENDING`. A API persistiu a intenção e a mensagem outbox na mesma transação SQL; o resultado financeiro será produzido pelo processamento assíncrono.

Guarde o `transactionId` retornado.

### 3. Consultar a transação

Substitua `TRANSACTION_ID_AQUI` pelo identificador retornado:

```http
GET http://localhost:3000/wagering/transactions/TRANSACTION_ID_AQUI
```

Os estados possíveis são:

| Estado | Significado |
|---|---|
| `PENDING` | Solicitação registrada e aguardando processamento |
| `PENDING_REFERENCE` | Operação aguardando uma referência necessária ao processamento |
| `PROCESSED` | Operação concluída com sucesso |
| `REJECTED` | Operação rejeitada por uma regra de negócio |
| `FAILED` | Falha registrada após o esgotamento das tentativas de infraestrutura |

Os estados terminais são `PROCESSED`, `REJECTED` e `FAILED`. Rejeições e falhas incluem um `failureCode`.

### 4. Consultar o saldo

```http
GET http://localhost:3000/wallets/WALLET_ID_AQUI
```

Após a aposta de `25.00` ser processada, o saldo esperado deste exemplo é `75.00`.

### 5. Verificar a idempotência

Reenvie a aposta com os mesmos headers e o mesmo corpo.

A repetição deve retornar a transação existente, sem aplicar outro débito. O saldo permanece em `75.00`.

Reutilizar a mesma chave de idempotência com um conteúdo de negócio diferente retorna conflito.

Para registrar uma nova operação, utilize novos identificadores de transação externa e de idempotência.

### 6. Consultar o extrato

```http
GET http://localhost:3000/wallets/WALLET_ID_AQUI/ledger?limit=50
```

### 7. Conferir a reconciliação

```http
POST http://localhost:3000/wallets/WALLET_ID_AQUI/reconciliation
```

A reconciliação compara o saldo armazenado com o saldo calculado a partir do ledger. Para uma carteira consistente, retorna `consistent: true` e diferença `0.00`.

Também é possível consultar uma transação pelos identificadores do provedor:

```http
GET http://localhost:3000/providers/provider-a/wagering/transactions/bet-001
```

## Fluxo de processamento

A API grava a intenção da transação e a outbox no PostgreSQL. O publisher envia a solicitação ao SQS FIFO.

O consumer registra a inbox e processa a operação dentro de uma transação SQL, utilizando locks para proteger a carteira. Saldo, ledger e eventos de saída são persistidos antes da confirmação de remoção da mensagem no SQS.

A entrega pode ocorrer mais de uma vez. A idempotência persistente impede que a mesma operação produza efeitos financeiros duplicados.

Consulte [ARCHITECTURE.md](ARCHITECTURE.md) para invariantes, decisões técnicas e tratamento de falhas.

Consulte [CHALLENGE.md](CHALLENGE.md) para o enunciado original.

## Escopo e limitações

- Os exemplos locais utilizam `BRL`. O objeto de valor `Money` também contempla `USD` e `EUR`.
- Autenticação não está implementada. O contrato `ProviderIdentityPort`, em `src/api/provider-identity.port.ts`, define um ponto de integração para uma implementação futura.
- A fila `wager-events.fifo` recebe os eventos de integração publicados pela outbox. O projeto não inclui consumidores downstream.
- A recuperação de referências pendentes utiliza backoff exponencial. Os valores padrão são dez tentativas e limite de 60 segundos entre tentativas, configuráveis por variáveis de ambiente.
- O LocalStack fornece a infraestrutura SQS do ambiente local. A configuração apresentada utiliza credenciais locais de teste.