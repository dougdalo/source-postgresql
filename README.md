# source-postgresql

[![Go Version](https://img.shields.io/badge/Go-1.25-00ADD8?logo=go&logoColor=white)](https://go.dev/doc/devel/release#go1.25.0)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-logical%20replication-4169E1?logo=postgresql&logoColor=white)](https://www.postgresql.org/docs/current/logical-replication.html)
[![Kafka](https://img.shields.io/badge/Kafka-producer-231F20?logo=apachekafka&logoColor=white)](https://kafka.apache.org/)

Source de CDC (*Change Data Capture*) para PostgreSQL, escrito em Go: faz o
snapshot inicial das tabelas/views configuradas e segue capturando mudanças
em tempo real via replicação lógica (slot + plugin `pgoutput`), publicando
tudo em um tópico Kafka. Construído como um node da plataforma **InthHub**.

## Sumário

- [Visão geral](#visão-geral)
- [Arquitetura do node](#arquitetura-do-node)
- [Deploy na InthHub](#deploy-na-inthhub)
- [Rodando localmente](#rodando-localmente)
- [Variáveis de ambiente](#variáveis-de-ambiente)
- [Formato da mensagem publicada](#formato-da-mensagem-publicada)
- [Como funciona o bootstrap](#como-funciona-o-bootstrap-primeira-execução)
- [Tuning de produção](#tuning-de-produção-tabelas-grandes)
- [Permissões necessárias no Postgres](#permissões-necessárias-no-postgres)
- [Reciclagem do slot / monitoramento](#reciclagem-do-slot--monitoramento)
- [Resiliência](#resiliência)
- [Licença](#licença)

## Visão geral

| Capacidade | Suporte |
|---|---|
| Tabelas normais | Snapshot + CDC contínuo |
| Tabelas particionadas (`PARTITION BY`) | Snapshot paralelo por partição + CDC contínuo, publicado sob o nome da tabela-raiz |
| Views / materialized views | Apenas snapshot (inicial e, opcionalmente, recorrente) — Postgres não gera WAL para views |
| Formato de saída | JSON flat (key = PK, value = linha completa, delete = tombstone) |
| Checkpoint / retomada | Tópico Kafka compactado, sem storage externo |
| Paralelismo do snapshot inicial | Configurável, por partição física |

## Arquitetura do node

Igual a qualquer outro node do InthHub: **[process.go](process.go) é o único
arquivo que sobe pra plataforma** — ela não permite modularizar em pacotes
internos, então toda a lógica (config, catálogo do Postgres, publication/slot,
snapshot, streaming de CDC, monitor) vive nesse arquivo só, em `package main`.

O `main.go` deste repositório **não é o que vai pra plataforma** — lá ele é
gerado automaticamente. Aqui ele é só um andaime para compilar e testar com
`go run .` localmente, reconstruindo o mínimo do contrato descrito na
documentação (`initVars()`, `process()`, `ProcessResult`, `sendDlq`,
`/healthz`, `/metrics`). Se o `main.go` real da plataforma tiver assinatura
diferente para esses hooks, ajuste as poucas linhas que os chamam no fim do
`process.go`.

Diferente dos nodes normais (consumer → transform → producer sobre uma
mensagem do `INPUT_TOPIC`), aqui a entrada é o próprio Postgres, não o Kafka.
Isso muda o uso dos hooks: como não existe mensagem de negócio chegando para
disparar trabalho, a pipeline inteira (bootstrap do slot, snapshot inicial,
streaming de CDC, monitor) é iniciada em background dentro de `initVars()`
— o único hook que a plataforma garante chamar uma vez antes do loop de
consumo começar — e roda sozinha até o processo morrer. `process()` fica
praticamente sem uso, existindo apenas para satisfazer o contrato exigido
pelo `main.go` gerado.

## Deploy na InthHub

Na plataforma, o `main.go` é **fixo e não pode ser alterado** — só é possível
mexer no `process.go` e nas variáveis de ambiente do deploy. É exatamente
assim que este projeto foi desenhado, e o contrato abaixo já foi conferido
contra a documentação oficial (guias *Pipelines*, *Criar uma pipeline*,
*Publicar uma pipeline*, *Gerenciar secrets* e *Secrets externos*):

- `process.go` **não sobe HTTP server, não abre porta, não faz nada de
  infraestrutura** — isso tudo (`/healthz`, `/metrics`, porta) já vem pronto
  do `main.go` padrão da plataforma.
- Ele declara as duas funções que o `main.go` padrão exige, com a assinatura
  confirmada no exemplo oficial de Go Function:
  ```go
  func initVars(ctx context.Context, log *zap.Logger) error
  func process(topicName, value, key string, headers map[string]string, log *zap.Logger) ([]ProcessResult, error)
  ```
  Todo o resto (config, catálogo do Postgres, publication/slot, snapshot,
  streaming, monitor) são funções privadas do próprio arquivo — não
  interferem em nada fora dele.
- Toda a configuração vem de `os.Getenv` dentro do `process.go`. As
  variáveis são declaradas na aba **Deploy → Variáveis** da pipeline, cada
  uma como **Valor plain** (host, porta, nome de tabela — nada sensível) ou
  **Secret** (aponta para um Secret cadastrado em *Workspace → Secrets* + o
  nome do campo dentro dele). `PG_DSN` deve ser Secret, já que carrega
  usuário e senha do Postgres — veja
  [Variáveis de ambiente](#variáveis-de-ambiente).

### Um Go Function precisa de pelo menos um Input ligado

Este é o ponto que **não é óbvio de início**: a documentação de *Pipelines*
é explícita — *"um Transform ou um Output consomem sempre de pelo menos um
tópico. Sem isso, a publicação é recusada."* Um Go Function é sempre da
família **Transform**, e a plataforma bloqueia a publicação de qualquer
Transform sem nenhum Input conectado.

Isso importa aqui porque este node não tem, por natureza, uma mensagem de
negócio disparando o trabalho — a captura roda em background a partir do
`initVars()`, direto do Postgres. Para a pipeline passar na validação da
plataforma, conecte um **Input do tipo Cron** (baixa frequência, ex: a cada
1-5 minutos) na entrada deste Go Function — o conteúdo do tick é irrelevante
e ignorado por `process()` (que já devolve `nil, nil` sempre); ele só existe
para satisfazer a exigência de "todo Transform tem um Input". O trabalho de
verdade continua 100% dirigido pelo `initVars()`.

### O tópico de saída é criado pela plataforma, não por você

Segundo a documentação de *Pipelines*: *"Toda peça capaz de produzir dado —
Input ou Transform — ganha automaticamente o seu tópico de saída quando é
criada."* Ou seja, `OUTPUT_TOPIC` não é um nome que você inventa e declara à
mão — a plataforma cria o tópico (padrão
`{pipeline}-n{número-do-node}-{sufixo}`) e injeta o valor no ambiente do
node sozinha. `STATE_TOPIC`, por outro lado, **não existe nesse mecanismo**
— é uma criação própria deste projeto para guardar o checkpoint, então
precisa ser criado manualmente (compactado) e declarado como variável plain
apontando pro nome escolhido.

> **Ponto de atenção:** o que está documentado aqui é a melhor leitura
> possível da documentação oficial disponível no momento em que este
> projeto foi escrito. Se o comportamento real da plataforma divergir em
> algum detalhe (nome exato de alguma variável injetada automaticamente,
> por exemplo), ajuste conforme o erro que a plataforma reportar no deploy.

O `main.go`, `.env`/`.env.example`, `docker-compose.homelab.yaml` e a pasta
`testdata/` deste repositório **não vão para a plataforma** — são apenas
andaime para desenvolver e testar localmente antes do deploy (veja
[Rodando localmente](#rodando-localmente) e o `.gitignore`).

## Rodando localmente

```bash
cp .env.example .env   # preencha com os dados do seu ambiente de teste
go run .
```

`/healthz` e `/metrics` (Prometheus) sobem na porta 9999, no mesmo padrão dos
outros nodes do InthHub — servidos pelo `main.go` (gerado na plataforma,
andaime local aqui). As métricas customizadas do pipeline (`pgsource_*`,
definidas em `process.go`) aparecem em `/metrics` porque usam
`promauto`/registry default do `prometheus/client_golang`, o mesmo que
qualquer handler `/metrics` padrão da plataforma serve.

A pasta [`testdata/`](testdata) traz scripts prontos para testar contra um
Postgres/Kafka locais: criação de tabela particionada de exemplo, carga em
massa (testa a paginação do snapshot) e geração contínua de insert/update/
delete (testa o streaming de CDC).

## Variáveis de ambiente

| Var | Obrigatória | Fonte na InthHub | Descrição |
|---|---|---|---|
| `KAFKA_BROKERS` | sim | Plain (ou `INTHUB_KAFKA_CONNECTION`, se usar Kafka → Conexões) | Lista separada por vírgula |
| `OUTPUT_TOPIC` | sim | **Auto-injetada pela plataforma** | Tópico de dados — não invente o nome, a plataforma cria e injeta ao criar o node |
| `STATE_TOPIC` | não | Plain | Default `{OUTPUT_TOPIC}-state`. **Precisa ser criado manualmente com `cleanup.policy=compact`** (não é um tópico auto-gerenciado pela plataforma) — é onde o node guarda o checkpoint (LSN confirmado + progresso do snapshot por tabela) para sobreviver a restarts |
| `PG_DSN` | sim | **Secret** | Connection string do Postgres, formato URL (`postgres://user:pass@host:5432/db?sslmode=require`) — contém credencial, nunca declare como plain |
| `SOURCE_TABLES` | sim | Plain | Lista `schema.tabela` separada por vírgula. Tipo de cada objeto (tabela/view/particionada) é descoberto em runtime |
| `SLOT_NAME` | sim | Plain | Nome do replication slot — **único por deploy/instância do node**, senão dois nodes competem pelo mesmo slot |
| `PUBLICATION_NAME` | não | Plain | Default = `SLOT_NAME` |
| `SNAPSHOT_FETCH_SIZE` | não | Plain | Default `5000`. Tamanho do lote de paginação do snapshot |
| `SNAPSHOT_WORKERS` | não | Plain | Default `4`. Partições-filha lidas em paralelo no snapshot inicial de uma tabela particionada |
| `PG_POOL_MAX_CONNS` | não | Plain | Default `SNAPSHOT_WORKERS + 6`. Tamanho do pool de conexões do Postgres |
| `KAFKA_BATCH_SIZE` | não | Plain | Default `1000`. Linhas por lote no producer Kafka |
| `KAFKA_BATCH_BYTES` | não | Plain | Default `5MB`. Bytes por lote no producer Kafka |
| `STATUS_UPDATE_INTERVAL_SECONDS` | não | Plain | Default `10`. Frequência do feedback ao Postgres (avança `confirmed_flush_lsn`, o que permite reciclar WAL) |
| `SLOT_MONITOR_INTERVAL_SECONDS` | não | Plain | Default `30`. Frequência da checagem de lag do slot |
| `SLOT_LAG_WARN_BYTES` | não | Plain | Default `512MB`. Acima disso, loga warning + métrica de WAL retido |
| `VIEW_REFRESH_INTERVAL_SECONDS` | não | Plain | Default `0` (só o snapshot inicial). Intervalo de re-snapshot de views |

`KAFKA_BROKERS` também pode ser resolvido automaticamente pela plataforma se
a pipeline usar uma conexão cadastrada em **Kafka → Conexões** — nesse caso a
variável `INTHUB_KAFKA_CONNECTION` (ou `INTHUB_KAFKA_CONNECTION_N{n}` para um
node Kafka específico) já entrega o endereço resolvido, sem precisar
declarar `KAFKA_BROKERS` manualmente. Ajuste conforme o que fizer mais
sentido no seu ambiente.

## Formato da mensagem publicada

- **Key**: JSON flat só com as colunas de chave primária. Ex: `{"id": 123}`.
  Views não têm PK confiável, então saem com key vazia `{}`.
- **Value** (insert/update/snapshot): JSON flat com todas as colunas da
  linha, tipado (number/bool/timestamp/etc — não tudo string).
- **Value** (delete): vazio — tombstone, mesma convenção de "delete via CDC"
  que os consumidores desses tópicos já esperam (`value == ""` → delete).
- **Headers**: `table` (schema.tabela), `op` (`r`=snapshot, `c`=insert,
  `u`=update, `d`=delete), `ts`, `lsn` (quando aplicável). Ficam fora do
  value de propósito, para manter o value 100% flat para o sink connector
  que vai transacionar isso para outro banco.

## Como funciona o bootstrap (primeira execução)

1. Resolve cada entrada de `SOURCE_TABLES` contra `pg_class` (tabela normal,
   particionada ou view).
2. Cria a publication (se não existir) cobrindo as tabelas CDC-capable.
3. Cria o replication slot exportando um snapshot consistente
   (`CREATE_REPLICATION_SLOT ... EXPORT_SNAPSHOT`) — isso dá um LSN exato
   (`ConsistentPoint`) e um nome de snapshot reutilizável em outra transação
   via `SET TRANSACTION SNAPSHOT`.
4. Roda o snapshot inicial de tabelas e views **usando esse snapshot
   exportado**, garantindo que não existe gap nem overlap entre o que o
   snapshot leu e de onde o streaming de CDC vai começar.
5. Começa o streaming a partir do `ConsistentPoint`.

Em execuções seguintes, pula os passos 2-4 (slot e publication já existem) e
retoma o streaming do `confirmed_lsn` salvo no `STATE_TOPIC`. Se o node cair
no meio do snapshot de uma tabela grande, retoma de onde parou (cursor salvo
por chave primária, não do zero).

## Tuning de produção (tabelas grandes)

Pensado para cenários como "dezenas de milhões de linhas numa tabela
particionada em várias partições, Kafka com vários brokers e boa capacidade
de máquina":

**1. O snapshot lê partição por partição, em paralelo.** Para uma tabela
`PARTITION BY`, o node descobre as partições-filha físicas (`pg_inherits`) e
lê `SNAPSHOT_WORKERS` delas ao mesmo tempo, cada uma na sua própria
conexão/transação — todas importando o **mesmo snapshot exportado do slot**,
permanecendo consistentes entre si (a mesma técnica usada pelo
`pg_dump --jobs`). O paralelismo real do snapshot é limitado pelo **número de
partições**, não por configuração alguma: com 12 partições,
`SNAPSHOT_WORKERS=8` processa 8 de uma vez e enfileira o resto;
`SNAPSHOT_WORKERS=12` processa todas simultaneamente (se o Postgres
aguentar). Comece perto do número de partições e ajuste para baixo se o
Postgres começar a sofrer com I/O/CPU.

**2. Publicação em lote, não linha a linha.** Cada página de
`SNAPSHOT_FETCH_SIZE` linhas lidas do Postgres vira **uma única chamada**
`WriteMessages` com todas as mensagens daquele lote — não uma chamada por
linha. Isso é o que permite o producer Kafka agrupar e comprimir de fato
antes de mandar para a rede, em vez de fazer um round-trip por linha.

**3. Dimensionamento de referência** (83M linhas / 12 partições, Kafka com 5
brokers de 32GB/8vCPU, tópico com 6 partições):

```dotenv
SNAPSHOT_WORKERS=8          # entre 6 e 12; comece em 8 e observe carga no Postgres
SNAPSHOT_FETCH_SIZE=20000   # lotes maiores = menos round-trips (default 5000 é conservador)
PG_POOL_MAX_CONNS=16        # SNAPSHOT_WORKERS + folga para monitor/refresh de views
KAFKA_BATCH_SIZE=2000
KAFKA_BATCH_BYTES=8388608   # 8MB — máquinas robustas de Kafka aguentam tranquilo
```

Com o tópico de dados tendo poucas partições, mais workers escrevendo ao
mesmo tempo não ganha muito em paralelismo *na escrita* (todos competem pelas
mesmas partições Kafka via hash da key) — o ganho de mais workers vem
principalmente do lado da *leitura* no Postgres (I/O paralelo entre
partições físicas diferentes). Para explorar um cluster Kafka maior ao
máximo, considere subir o número de partições do tópico (alinhado com o
número de partições do Postgres) antes de ir para produção: mais partições
Kafka = mais paralelismo possível, tanto na escrita quanto no consumo
posterior.

**4. Lado do Postgres.** O gargalo real de tabelas muito grandes costuma ser
I/O de disco, não CPU do node:
- confira `effective_io_concurrency` e `shared_buffers` compatíveis com
  leitura paralela de várias partições ao mesmo tempo;
- se o storage é compartilhado entre as partições, paralelismo alto pode
  saturar IOPS em vez de acelerar — teste incrementalmente
  (`SNAPSHOT_WORKERS=4` → `8` → `12`) observando `iostat`/latência de query,
  em vez de assumir que mais é sempre melhor;
- `GRANT SELECT` continua sendo dado só na tabela-raiz, não em cada partição
  individualmente.

**5. O streaming de CDC não é afetado por esse tuning** — ele processa um
evento de cada vez, na ordem em que chegam do WAL (não é possível paralelizar
sem quebrar a ordem de commit). O tuning acima é só para o snapshot inicial,
a parte pesada de carregar um volume grande de dados de uma vez.

## Permissões necessárias no Postgres

O usuário de `PG_DSN` precisa de:

```sql
ALTER ROLE meu_usuario REPLICATION;
GRANT SELECT ON <tabelas e views listadas em SOURCE_TABLES> TO meu_usuario;
```

O Postgres precisa estar com `wal_level = logical` (requer restart do
servidor se estiver como `replica`/`minimal`).

Toda tabela em `SOURCE_TABLES` precisa ter **chave primária** — usada tanto
para a key das mensagens quanto porque replicação lógica exige
`REPLICA IDENTITY DEFAULT` (usa a PK) ou `FULL` no mínimo. Sem PK, o node
recusa subir.

Se alguma coluna TOAST grande (texto/bytea grande) precisar sempre vir
completa em updates que não a alteraram, configure `REPLICA IDENTITY FULL`
nessa tabela — com `DEFAULT`, uma coluna TOAST não tocada num UPDATE não é
reenviada pelo WAL e o campo sai omitido do JSON daquele evento.

## Reciclagem do slot / monitoramento

A cada `STATUS_UPDATE_INTERVAL_SECONDS`, o node envia um *Standby Status
Update* ao Postgres informando até onde já processou (e publicou com
sucesso no Kafka) — isso é o que permite o Postgres avançar o `restart_lsn`
do slot e reciclar segmentos de WAL antigos. Se o node cair ou ficar para
trás, o WAL retido cresce (`pgsource_replication_lag_bytes`), e ao ultrapassar
`SLOT_LAG_WARN_BYTES` isso vira warning no log — sinal de que o disco do
Postgres pode estar enchendo e algo rio abaixo (Kafka, rede, o próprio node)
precisa de atenção.

## Resiliência

Como não existe "reprocessar mensagem" aqui (não há consumer group nem
offset por mensagem — é um loop de background), a própria pipeline se
reinicia sozinha com backoff exponencial (5s a 2min) se cair por qualquer
motivo (conexão derrubada, erro do Postgres, etc.), retomando do último
`confirmed_lsn`/progresso de snapshot salvo no `STATE_TOPIC`.

## Licença

Distribuído sob a licença [MIT](LICENSE).
