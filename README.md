# source-postgresql

[![Go Version](https://img.shields.io/badge/Go-1.23-00ADD8?logo=go&logoColor=white)](https://go.dev/doc/devel/release#go1.23.0)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-logical%20replication-4169E1?logo=postgresql&logoColor=white)](https://www.postgresql.org/docs/current/logical-replication.html)
[![Kafka](https://img.shields.io/badge/Kafka-producer-231F20?logo=apachekafka&logoColor=white)](https://kafka.apache.org/)

Source de CDC pra Postgres: faz o snapshot inicial das tabelas/views
configuradas e depois segue capturando mudanças via replicação lógica (slot +
plugin `pgoutput`), publicando tudo em um tópico Kafka.

## Estrutura do node

Igual a qualquer outro node do InthHub: **[process.go](process.go) é o único
arquivo que sobe pra plataforma** — a plataforma não deixa modularizar em
pacotes internos, então toda a lógica (config, catálogo do Postgres,
publication/slot, snapshot, streaming de CDC, monitor) vive nesse arquivo só,
em `package main`.

O `main.go` deste repositório **não é o que vai pra plataforma** — lá ele é
gerado automaticamente. Aqui ele é só um andaime pra dar pra compilar e testar
com `go run .` localmente, reconstruindo o mínimo do contrato descrito na doc
(`initVars()`, `process()`, `ProcessResult`, `sendDlq`, `/healthz`,
`/metrics`). Se o `main.go` real do seu projeto tiver assinatura diferente pra
esses hooks, ajuste as poucas linhas que chamam eles no fim do `process.go`.

Diferente dos nodes normais (consumer → transform → producer sobre uma
mensagem do `INPUT_TOPIC`), aqui a entrada é o próprio Postgres, não o Kafka.
Isso muda o uso dos hooks: como não existe mensagem de negócio chegando pra
disparar trabalho, a pipeline inteira (bootstrap do slot, snapshot inicial,
streaming de CDC, monitor) é iniciada em background dentro de `initVars()`
— o único hook que a plataforma garante chamar uma vez antes do loop de
consumo começar — e roda sozinha até o processo morrer. `process()` fica
praticamente sem uso, só existe pra satisfazer o contrato exigido pelo
`main.go` gerado.

## Deploy na InthHub

Na plataforma, o `main.go` é **fixo e não pode ser alterado** — só dá pra
mexer no `process.go` e nas variáveis de ambiente do deploy. É exatamente
assim que este projeto foi desenhado:

- `process.go` **não sobe HTTP server, não abre porta, não faz nada de
  infraestrutura** — isso tudo (`/healthz`, `/metrics`, porta) já vem pronto
  do `main.go` padrão da plataforma.
- Ele só declara duas coisas que o `main.go` padrão espera encontrar:
  `func initVars()` e
  `func process(topicName, value, key string, headers map[string]string, log *zap.Logger) ([]ProcessResult, error)`.
  Todo o resto (config, catálogo do Postgres, publication/slot, snapshot,
  streaming, monitor) são funções privadas do próprio arquivo — não
  interferem em nada fora dele.
- Toda a configuração vem de `os.Getenv` dentro do `process.go` — as
  variáveis que você declarar no deploy da plataforma chegam do jeito normal,
  sem precisar de `.env` nem de nenhum arquivo. Não existe passo extra: é só
  declarar as variáveis da seção [Variáveis de ambiente](#variáveis-de-ambiente) — as 5 obrigatórias (`KAFKA_BROKERS`, `OUTPUT_TOPIC`, `PG_DSN`,
  `SOURCE_TABLES`, `SLOT_NAME`) e, se quiser, as de tuning.
- **Único ponto de atenção**: a assinatura exata de `initVars()`/`process()`
  no `process.go` precisa bater com o que o `main.go` real da plataforma
  chama. O que está aqui é a melhor leitura da documentação disponível no
  momento em que este projeto foi escrito — se o build da plataforma reclamar
  de assinatura (`does not implement`, número de argumentos, etc.), ajuste só
  essas duas funções no fim do `process.go`; o resto do arquivo não muda.

O `main.go`, `.env`/`.env.example`, `docker-compose.homelab.yaml` e a pasta
`testdata/` deste repositório **não vão pra plataforma** — são só andaime
pra desenvolver e testar localmente antes do deploy (veja
[Rodando localmente](#rodando-localmente) e o `.gitignore`).

## O que ele cobre

- **Tabelas normais e particionadas**: snapshot inicial + CDC contínuo via
  replicação lógica. Publication criada com `publish_via_partition_root =
  true`, então mudanças em qualquer partição-filha chegam no Kafka com o nome
  da tabela-raiz — quem consome não precisa saber que a tabela é particionada.
- **Views e materialized views**: só snapshot (inicial e, opcionalmente,
  recorrente via `VIEW_REFRESH_INTERVAL_SECONDS`). O Postgres não gera WAL
  para views — não existe CDC de view, só de tabela. Isso é limitação do
  próprio Postgres, não do código.

## Formato da mensagem publicada

- **Key**: JSON flat só com as colunas de chave primária. Ex: `{"id": 123}`.
  Views não têm PK confiável, então saem com key vazia `{}`.
- **Value** (insert/update/snapshot): JSON flat com todas as colunas da linha,
  tipado (number/bool/timestamp/etc, não tudo string).
- **Value** (delete): vazio — tombstone, mesma convenção de "delete via CDC"
  que os consumidores desses tópicos já esperam (`value == ""` → delete).
- **Headers**: `table` (schema.tabela), `op` (`r`=snapshot, `c`=insert,
  `u`=update, `d`=delete), `ts`, `lsn` (quando aplicável). Ficam fora do value
  de propósito, pra manter o value 100% flat pro sink connector que vai
  transacionar isso pra outro banco.

## Variáveis de ambiente

| Var | Obrigatória | Descrição |
|---|---|---|
| `KAFKA_BROKERS` | sim | Lista separada por vírgula |
| `OUTPUT_TOPIC` | sim | Tópico de dados |
| `STATE_TOPIC` | não | Default `{OUTPUT_TOPIC}-state`. **Precisa ser criado com `cleanup.policy=compact`** — é onde o node guarda o checkpoint (LSN confirmado + progresso do snapshot por tabela) pra sobreviver a restart |
| `PG_DSN` | sim | Connection string do Postgres, formato URL (`postgres://user:pass@host:5432/db?sslmode=require`) |
| `SOURCE_TABLES` | sim | Lista `schema.tabela` separada por vírgula. Tipo de cada objeto (tabela/view/particionada) é descoberto em runtime |
| `SLOT_NAME` | sim | Nome do replication slot — **único por deploy/instância do node**, senão dois nodes competem pelo mesmo slot |
| `PUBLICATION_NAME` | não | Default = `SLOT_NAME` |
| `SNAPSHOT_FETCH_SIZE` | não | Default 5000. Tamanho do batch de paginação do snapshot |
| `STATUS_UPDATE_INTERVAL_SECONDS` | não | Default 10. Frequência do feedback ao Postgres (avança `confirmed_flush_lsn`, o que permite reciclar WAL) |
| `SLOT_MONITOR_INTERVAL_SECONDS` | não | Default 30. Frequência da checagem de lag do slot |
| `SLOT_LAG_WARN_BYTES` | não | Default 512MB. Acima disso, loga warning + métrica de WAL retido |
| `VIEW_REFRESH_INTERVAL_SECONDS` | não | Default 0 (só o snapshot inicial). Intervalo de re-snapshot de views |
| `SNAPSHOT_WORKERS` | não | Default 4. Quantas partições-filha são lidas em paralelo no snapshot inicial de uma tabela particionada |
| `PG_POOL_MAX_CONNS` | não | Default `SNAPSHOT_WORKERS + 6`. Tamanho do pool de conexões do Postgres |
| `KAFKA_BATCH_SIZE` | não | Default 1000. Linhas por lote no producer Kafka |
| `KAFKA_BATCH_BYTES` | não | Default 5MB. Bytes por lote no producer Kafka |

## Tuning de produção (tabelas grandes)

Testado pensando em cenários tipo "83M de linhas numa tabela particionada em
~12 partições, Kafka com vários brokers e boa capacidade de máquina":

**1) O snapshot lê partição por partição, em paralelo.** Pra uma tabela
`PARTITION BY`, o node descobre as partições-filha físicas (`pg_inherits`) e
lê `SNAPSHOT_WORKERS` delas ao mesmo tempo, cada uma na sua própria
conexão/transação — todas importando o **mesmo snapshot exportado do slot**,
então continuam consistentes entre si (é a mesma técnica do `pg_dump --jobs`).
Isso significa que o paralelismo real do snapshot é limitado pelo **número de
partições**, não por config alguma: com 12 partições, `SNAPSHOT_WORKERS=8`
processa 8 de uma vez e enfileira o resto; `SNAPSHOT_WORKERS=12` processa
todas simultaneamente (se o Postgres aguentar). Comece perto do número de
partições e ajuste pra baixo se o Postgres começar a sofrer com I/O/CPU.

**2) Publicação em lote, não linha a linha.** Cada página de
`SNAPSHOT_FETCH_SIZE` linhas lidas do Postgres vira **uma única chamada**
`WriteMessages` com todas as mensagens daquele lote — não uma chamada por
linha. Isso é o que deixa o producer Kafka realmente agrupar e comprimir
antes de mandar pra rede, em vez de fazer um round-trip por linha.

**3) Dimensionamento sugerido pro seu ambiente** (83M linhas / 12 partições,
Kafka com 5 brokers de 32GB/8vCPU, tópico com 6 partições):

```
SNAPSHOT_WORKERS=8          # entre 6 e 12; comece em 8 e observe carga no Postgres
SNAPSHOT_FETCH_SIZE=20000   # lotes maiores = menos round-trips (default 5000 é conservador)
PG_POOL_MAX_CONNS=16        # SNAPSHOT_WORKERS + folga pro monitor/refresh de views
KAFKA_BATCH_SIZE=2000
KAFKA_BATCH_BYTES=8388608   # 8MB — as máquinas do Kafka aguentam tranquilo
```

Com o tópico de dados tendo só 6 partições, mais de ~6 workers escrevendo ao
mesmo tempo não ganha muito em paralelismo *na escrita* (todo mundo compete
pelas mesmas 6 partições Kafka pra fazer o hash da key) — o ganho de mais
workers vem principalmente do lado da *leitura* no Postgres (I/O paralelo
entre partições físicas diferentes). Se quiser explorar o Kafka de 5 brokers
ao máximo, considere subir o tópico pra mais partições (ex: 12, alinhado com
o número de partições do Postgres) antes de ir pra produção — mais partições
Kafka = mais paralelismo possível tanto na escrita quanto depois no consumo.

**4) Lado do Postgres**: o gargalo real de 83M linhas costuma ser I/O de
disco, não CPU do node. Verifique:
- `effective_io_concurrency` e `shared_buffers` do Postgres compatíveis com
  leitura paralela de várias partições ao mesmo tempo;
- se o storage é SSD/NVMe compartilhado entre as partições, paralelismo alto
  pode saturar IOPS em vez de acelerar — teste incrementalmente
  (`SNAPSHOT_WORKERS=4` → `8` → `12`) observando `iostat`/latência de query,
  não assuma que mais é sempre melhor;
- `GRANT SELECT` continua só na tabela-raiz, não precisa dar grant em cada
  partição individualmente.

**5) Isso não afeta o streaming de CDC** — o streaming processa um evento de
cada vez, na ordem em que chegam do WAL (não dá pra paralelizar sem quebrar
ordem de commit). O tuning acima é só pro snapshot inicial, que é a parte
pesada de carregar 83M linhas de uma vez.

## Permissões necessárias no Postgres

O usuário de `PG_DSN` precisa de:

```sql
ALTER ROLE meu_usuario REPLICATION;
GRANT SELECT ON <tabelas e views listadas em SOURCE_TABLES> TO meu_usuario;
```

E o Postgres precisa estar com `wal_level = logical` (requer restart do
servidor se estiver como `replica`/`minimal`).

Toda tabela em `SOURCE_TABLES` precisa ter **chave primária** — é usada tanto
pra key das mensagens quanto porque replicação lógica exige `REPLICA IDENTITY
DEFAULT` (usa a PK) ou `FULL` no mínimo. Sem PK, o node recusa subir.

Se alguma coluna TOAST grande (texto/bytea grande) precisar sempre vir
completa em updates que não a alteraram, configure `REPLICA IDENTITY FULL`
nessa tabela — com `DEFAULT`, coluna TOAST não tocada num UPDATE não é
reenviada pelo WAL e o campo sai omitido do JSON daquele evento.

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

## Reciclagem do slot / monitoramento

A cada `STATUS_UPDATE_INTERVAL_SECONDS`, o node manda um *Standby Status
Update* pro Postgres informando até onde já processou (e publicou com sucesso
no Kafka) — é isso que permite o Postgres avançar o `restart_lsn` do slot e
reciclar segmentos de WAL antigos. Se o node cair ou ficar pra trás, o WAL
retido cresce (`pgsource_replication_lag_bytes`), e passado
`SLOT_LAG_WARN_BYTES` isso vira warning no log — sinal de que o disco do
Postgres pode estar enchendo e algo rio abaixo (Kafka, rede, o próprio node)
precisa de atenção.

## Rodando localmente

```bash
go run .
```

`/healthz` e `/metrics` (Prometheus) sobem na porta 9999, no mesmo padrão dos
outros nodes do InthHub — servidos pelo `main.go` (gerado na plataforma,
stand-in local aqui). As métricas customizadas do pipeline (`pgsource_*`,
definidas em `process.go`) aparecem em `/metrics` porque usam
`promauto`/registry default do `prometheus/client_golang`, o mesmo que
qualquer handler `/metrics` padrão da plataforma serve.

## Resiliência

Como não existe "reprocessar mensagem" aqui (não tem consumer group nem
offset por mensagem — é um loop de background), a própria pipeline se
reinicia sozinha com backoff exponencial (5s a 2min) se cair por qualquer
motivo (conexão derrubada, erro do Postgres, etc.), retomando do último
`confirmed_lsn`/progresso de snapshot salvo no `STATE_TOPIC`.

## Licença

Distribuído sob a licença [MIT](LICENSE).
