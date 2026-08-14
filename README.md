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

- [source-postgresql](#source-postgresql)
  - [Sumário](#sumário)
  - [Visão geral](#visão-geral)
  - [Arquitetura do node](#arquitetura-do-node)
  - [Deploy na InthHub](#deploy-na-inthhub)
    - [Um Go Function precisa de pelo menos um Input ligado](#um-go-function-precisa-de-pelo-menos-um-input-ligado)
    - [O tópico de saída é criado pela plataforma, não por você](#o-tópico-de-saída-é-criado-pela-plataforma-não-por-você)
  - [Rodando localmente](#rodando-localmente)
  - [Variáveis de ambiente](#variáveis-de-ambiente)
  - [Formato da mensagem publicada](#formato-da-mensagem-publicada)
  - [Como funciona o bootstrap (primeira execução)](#como-funciona-o-bootstrap-primeira-execução)
  - [Tuning de produção (tabelas grandes)](#tuning-de-produção-tabelas-grandes)
  - [Permissões necessárias no Postgres](#permissões-necessárias-no-postgres)
    - [Checklist — tabela normal ou view](#checklist--tabela-normal-ou-view)
    - [Checklist — tabela particionada (`PARTITION BY`)](#checklist--tabela-particionada-partition-by)
    - [Coluna TOAST grande](#coluna-toast-grande)
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
  nome do campo dentro dele). A conexão com o Postgres segue o mesmo padrão
  usado nos outros nodes Go Function da empresa: `PG_HOST`/`PG_PORT`/
  `PG_DATABASE`/`PG_SSLMODE` como plain, e `PG_USER`/`PG_PASSWORD` como
  Secret — veja [Variáveis de ambiente](#variáveis-de-ambiente).

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
| `PG_HOST` | sim | Plain | Host do Postgres |
| `PG_PORT` | não | Plain | Default `5432` |
| `PG_DATABASE` | sim | Plain | Nome do banco |
| `PG_SSLMODE` | não | Plain | Default `require`. Use `disable` só em ambiente de teste sem TLS |
| `PG_USER` | sim | **Secret** | Usuário do Postgres |
| `PG_PASSWORD` | sim | **Secret** | Senha do Postgres — nunca declare como plain |
| `SOURCE_TABLES` | sim | Plain | Lista `schema.tabela` separada por vírgula. Tipo de cada objeto (tabela/view/particionada) é descoberto em runtime |
| `SLOT_NAME` | sim | Plain | Nome do replication slot — **único por deploy/instância do node**, senão dois nodes competem pelo mesmo slot |
| `PUBLICATION_NAME` | não | Plain | Default = `SLOT_NAME` |
| `SNAPSHOT_FETCH_SIZE` | não | Plain | Default `1000`. Tamanho do lote de paginação do snapshot. Default conservador de propósito — cada worker mantém um lote inteiro na memória, então isso escala junto com `SNAPSHOT_WORKERS` (veja [Tuning](#tuning-de-produção-tabelas-grandes)) |
| `SNAPSHOT_WORKERS` | não | Plain | Default `2`. Partições-filha lidas em paralelo no snapshot inicial de uma tabela particionada. Default conservador — só suba se o pod tiver folga de memória |
| `PG_POOL_MAX_CONNS` | não | Plain | Default `SNAPSHOT_WORKERS + 6`. Tamanho do pool de conexões do Postgres |
| `KAFKA_BATCH_SIZE` | não | Plain | Default `1000`. Linhas por lote no producer Kafka |
| `KAFKA_BATCH_BYTES` | não | Plain | Default `5MB`. Bytes por lote no producer Kafka |
| `STATUS_UPDATE_INTERVAL_SECONDS` | não | Plain | Default `10`. Frequência do feedback ao Postgres (avança `confirmed_flush_lsn`, o que permite reciclar WAL) |
| `SLOT_MONITOR_INTERVAL_SECONDS` | não | Plain | Default `30`. Frequência da checagem de lag do slot |
| `SLOT_LAG_WARN_BYTES` | não | Plain | Default `512MB`. Acima disso, loga warning + métrica de WAL retido |
| `VIEW_REFRESH_INTERVAL_SECONDS` | não | Plain | Default `0` (só o snapshot inicial). Intervalo de re-snapshot de views |

Se a pipeline usar uma conexão cadastrada em **Kafka → Conexões** em vez de
um endereço digitado à mão, a plataforma injeta o endereço resolvido em
`INTHUB_KAFKA_CONNECTION` — o código já trata isso: `KAFKA_BROKERS` tem
prioridade se estiver setada, caindo para `INTHUB_KAFKA_CONNECTION`
automaticamente quando não estiver (função `resolveKafkaBrokers` em
`process.go`). Não precisa declarar as duas.

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

Os defaults (`SNAPSHOT_WORKERS=2`, `SNAPSHOT_FETCH_SIZE=1000`) são
propositalmente conservadores de memória — pensados pra rodar sem estourar
limite em pods pequenos, mesmo sem controle sobre o `MEM REQ → LIM` do
deploy. Esta seção é **opt-in**: só suba esses valores se o pod tiver
memória de sobra e a tabela for grande o suficiente pra o ganho de
velocidade valer a pena. Pensado para cenários como "dezenas de milhões de
linhas numa tabela particionada em várias partições, Kafka com vários
brokers e boa capacidade de máquina":

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
SNAPSHOT_WORKERS=8          # entre 6 e 12; comece em 8 e observe carga no Postgres E memória do pod
SNAPSHOT_FETCH_SIZE=20000   # lotes maiores = menos round-trips, mas mais memória por worker (default 1000 é o modo econômico)
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

O servidor precisa estar com `wal_level = logical` (requer restart se
estiver como `replica`/`minimal`). O usuário configurado em
`PG_USER`/`PG_PASSWORD` precisa das permissões abaixo — **o conjunto muda
dependendo se o objeto é tabela normal/view ou tabela particionada**, então
as duas situações estão separadas.

### Checklist — tabela normal ou view

```sql
ALTER ROLE meu_usuario REPLICATION;
GRANT SELECT ON schema.tabela_normal TO meu_usuario;
GRANT SELECT ON schema.minha_view TO meu_usuario;
```

- `REPLICATION`: exigido pra abrir a conexão de replicação lógica (streaming de CDC) e criar/gerenciar o slot.
- `SELECT` na própria tabela/view: exigido pro snapshot inicial, que faz `SELECT * FROM schema.tabela_normal` direto.
- A tabela precisa ter **chave primária** — usada tanto pra key das mensagens quanto porque replicação lógica exige `REPLICA IDENTITY DEFAULT` (usa a PK) ou `FULL` no mínimo. Sem PK, o node recusa subir. Views não precisam de PK (não têm CDC, só snapshot).

### Checklist — tabela particionada (`PARTITION BY`)

O ponto que costuma pegar gente de surpresa: **o `GRANT` na tabela-raiz não
é suficiente, e também não é o que falta** — são coisas diferentes.

| Onde dar o GRANT | Precisa? | Por quê |
|---|---|---|
| `SELECT` na tabela-**raiz** (`schema.tabela_particionada`) | **Não precisa** | O código nunca faz `SELECT` direto na raiz — nem o snapshot nem o CDC leem por ali |
| `SELECT` em **cada partição-filha** (`schema.tabela_particionada_2025_04`, etc.) | **Precisa, uma por uma** | O [snapshot paralelo por partição](#tuning-de-produção-tabelas-grandes) lê cada partição diretamente pelo nome físico (`SELECT * FROM tabela_particionada_2025_04 ...`), pra poder paralelizar. Quando a consulta nomeia a partição diretamente, o Postgres checa a permissão **daquela partição**, não da raiz — e uma partição nova **não herda automaticamente** nenhum `GRANT` dado na raiz |
| `REPLICATION` no usuário | Precisa (igual tabela normal) | Streaming de CDC |
| Chave primária na raiz, incluindo a coluna de partição | Precisa (exigência do próprio Postgres pra tabela particionada) | Postgres propaga a constraint pra cada partição automaticamente — não precisa recriar PK partição por partição |
| Ser dono da publication, **se ela já existir criada por fora** (ex: por um DBA) | Só se o node precisar adicionar a tabela numa publication pré-existente | Ver aviso abaixo |

Sem o `SELECT` em cada partição, o snapshot falha assim (o streaming de CDC
**não** é afetado por isso — ele lê do WAL, não faz `SELECT`):

```
ERROR: permission denied for table sicsimulacao_2025_04 (SQLSTATE 42501)
```

A correção tem duas partes — cobre as partições de hoje e as que ainda vão
ser criadas:

```sql
-- 1. Partições que já existem
GRANT SELECT ON ALL TABLES IN SCHEMA public TO meu_usuario;

-- 2. Partições futuras — roda como o role que normalmente CRIA as
--    partições novas (rotina de manutenção de partição), senão o
--    default privilege não pega
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO meu_usuario;

-- Se quem cria as partições novas é outro role, especifique:
-- ALTER DEFAULT PRIVILEGES FOR ROLE role_que_cria_particoes IN SCHEMA public
--     GRANT SELECT ON TABLES TO meu_usuario;
```

Sem o passo 2, toda vez que uma partição nova for criada (mensal, por
exemplo) o snapshot dela volta a falhar até alguém lembrar de rodar o
`GRANT` de novo.

> **Publication pré-existente (criada por fora, ex: por um DBA):** o node
> tenta automaticamente incluir a tabela na publication se ela ainda não
> for membro (`ALTER PUBLICATION ... ADD TABLE`), e isso **exige ser dono
> da publication** — só dá erro se a tabela genuinamente ainda não estiver
> lá. Se a publication já foi criada cobrindo sua tabela (direto ou via
> `FOR TABLE`), o node detecta isso e não tenta alterar nada, não
> precisando de posse da publication nesse caso.

### Coluna TOAST grande

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
