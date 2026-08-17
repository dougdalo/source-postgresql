// process.go — ÚNICO arquivo que sobe pra plataforma. Tudo (config, catálogo,
// publication/slot, snapshot, replicação lógica, monitor de slot) vive aqui
// dentro porque o node não permite pacotes internos: main.go é gerado pela
// plataforma e só enxerga process.go no mesmo pacote.
//
// Esse node é diferente dos nodes normais (consumer -> transform -> producer
// sobre uma mensagem do INPUT_TOPIC): aqui a origem dos dados é o próprio
// Postgres, não o Kafka. Por isso:
//   - initVars() não só inicializa o pool de conexão: é onde a pipeline
//     inteira (snapshot inicial + streaming de CDC) é disparada em background,
//     já que é o único hook que a plataforma chama uma vez antes do loop
//     de consumo começar.
//   - process() fica praticamente sem uso (não há mensagens de negócio
//     chegando pra transformar) — só existe pra satisfazer o contrato exigido
//     pelo main.go gerado.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	kafka "github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

// =============================================================================
// Métricas — registradas no registry default do prometheus/client_golang, que
// é o mesmo que o handler /metrics já exposto pelo main.go da plataforma serve.
// =============================================================================

var (
	metricSnapshotRows = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "pgsource_snapshot_rows_published_total",
		Help: "Linhas publicadas durante o snapshot inicial/periódico, por tabela.",
	}, []string{"table"})

	metricCDCEvents = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "pgsource_cdc_events_published_total",
		Help: "Eventos de CDC publicados (insert/update/delete), por tabela e operação.",
	}, []string{"table", "op"})

	metricPublishErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "pgsource_publish_errors_total",
		Help: "Erros ao publicar no Kafka.",
	}, []string{"stage"})

	metricReplicationLagBytes = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "pgsource_replication_lag_bytes",
		Help: "Diferença entre pg_current_wal_lsn() e o restart_lsn do slot de replicação.",
	})

	metricSlotActive = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "pgsource_slot_active",
		Help: "1 se o replication slot está ativo (conectado), 0 caso contrário.",
	})

	metricLastConfirmedLSN = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "pgsource_last_confirmed_lsn",
		Help: "Último LSN confirmado ao Postgres (valor numérico do LSN).",
	})

	metricSnapshotInProgress = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "pgsource_snapshot_in_progress",
		Help: "1 enquanto o snapshot inicial está rodando, 0 depois de concluído.",
	})
)

// =============================================================================
// Config — lida direto das env vars, igual ao padrão dos outros nodes.
// =============================================================================

type sourceConfig struct {
	KafkaBrokers []string
	OutputTopic  string
	StateTopic   string

	// Conexão com o Postgres montada a partir de campos separados, não de um
	// PG_DSN único: host/porta/banco/sslmode são variáveis plain, usuário e
	// senha vêm de Secret — mesmo padrão já usado nos outros nodes Go
	// Function da empresa (host/porta plain, credencial via secret).
	PGHost     string
	PGPort     string
	PGDatabase string
	PGUser     string
	PGPassword string
	PGSSLMode  string

	SourceTables []string

	SlotName        string
	PublicationName string

	SnapshotFetchSize int
	// SnapshotWorkers controla o paralelismo do snapshot inicial: quantas
	// partições-filha (de uma tabela particionada) são lidas ao mesmo tempo,
	// cada uma na sua própria conexão/transação, todas importando o mesmo
	// snapshot exportado do slot pra manter consistência entre si.
	SnapshotWorkers int
	// PGPoolMaxConns dimensiona o pool do pgx. Precisa comportar
	// SnapshotWorkers conexões simultâneas + a do monitor de slot + a do
	// refresh de views, com folga.
	PGPoolMaxConns int32

	KafkaBatchSize  int
	KafkaBatchBytes int64
	// KafkaCompression: "lz4" (default — bate com o padrão do cluster),
	// "snappy", "gzip", "zstd" ou "none".
	KafkaCompression string

	StatusUpdateInterval time.Duration
	SlotMonitorInterval  time.Duration
	SlotLagWarnBytes     int64
	ViewRefreshInterval  time.Duration
}

// maxSnapshotWorkers é um teto absoluto pro paralelismo do snapshot inicial,
// independente do que SNAPSHOT_WORKERS pedir. Existe pra cobrir o caso de
// alguém setar o valor igual (ou perto) do número de partições de uma tabela
// grande — ex.: 24 partições, SNAPSHOT_WORKERS=24 pra "processar tudo de uma
// vez" — o que sobe memória rápido demais e derruba o pod com OOMKilled no
// meio do bootstrap (cada worker segura um lote inteiro de linhas na
// memória). Alinhado ao teto já documentado no README "Tuning de produção".
const maxSnapshotWorkers = 12

// maxSnapshotFetchSize é o teto absoluto de linhas por lote do snapshot,
// pelo mesmo motivo do teto de workers: memória de pico escala com
// SnapshotWorkers × SnapshotFetchSize × largura da linha, e nada limitava o
// segundo fator até aqui. 20000 é o próprio valor de referência documentado
// no README pra tabela grande com pod de sobra — acima disso já não é mais
// tuning, é risco.
const maxSnapshotFetchSize = 20000

// maxSnapshotBatchRows é um teto sobre o PRODUTO SnapshotWorkers ×
// SnapshotFetchSize. Os dois tetos acima, cada um aplicado isoladamente,
// ainda permitem a pior combinação dos dois ao mesmo tempo (12 workers ×
// 20000 linhas = 240000 linhas em memória simultaneamente, uma por worker
// paralelo) — exatamente o cenário de OOMKilled que os comentários deles já
// descreviam mas nenhum dos dois de fato bloqueava sozinho. 160000 é o
// próprio produto do cenário de referência documentado no README (8
// workers × 20000 linhas para 83M linhas/12 partições) — pedir mais que
// isso ao mesmo tempo já não é mais o "modo turbo" documentado, é risco não
// coberto. Quando o produto estoura, reduz SNAPSHOT_FETCH_SIZE (não
// SNAPSHOT_WORKERS): o paralelismo entre partições é o que dá o ganho real
// de I/O no Postgres — o lote por worker é o que sobra pra cortar.
const maxSnapshotBatchRows = 160_000

func loadSourceConfig(log *zap.Logger) (*sourceConfig, error) {
	// Defaults conservadores de propósito: o pico de memória do snapshot
	// escala com SnapshotWorkers × SnapshotFetchSize (cada worker segura um
	// lote inteiro de linhas na memória). Sem saber de antemão o limite de
	// memória do pod (em muitos ambientes ele é automático, não configurável
	// pelo deploy da pipeline), é mais seguro começar pequeno e deixar quem
	// tiver folga de memória subir via env var do que estourar OOM por padrão.
	snapshotWorkers := envInt("SNAPSHOT_WORKERS", 2)
	if snapshotWorkers > maxSnapshotWorkers {
		log.Warn("SNAPSHOT_WORKERS acima do teto de segurança, limitando",
			zap.Int("solicitado", snapshotWorkers), zap.Int("teto", maxSnapshotWorkers))
		snapshotWorkers = maxSnapshotWorkers
	}

	snapshotFetchSize := envInt("SNAPSHOT_FETCH_SIZE", 1000)
	if snapshotFetchSize > maxSnapshotFetchSize {
		log.Warn("SNAPSHOT_FETCH_SIZE acima do teto de segurança, limitando",
			zap.Int("solicitado", snapshotFetchSize), zap.Int("teto", maxSnapshotFetchSize))
		snapshotFetchSize = maxSnapshotFetchSize
	}

	if product := snapshotWorkers * snapshotFetchSize; product > maxSnapshotBatchRows {
		adjusted := maxSnapshotBatchRows / snapshotWorkers
		log.Warn("SNAPSHOT_WORKERS × SNAPSHOT_FETCH_SIZE acima do teto combinado de segurança, reduzindo SNAPSHOT_FETCH_SIZE",
			zap.Int("snapshot_workers", snapshotWorkers),
			zap.Int("snapshot_fetch_size_solicitado", snapshotFetchSize),
			zap.Int("snapshot_fetch_size_ajustado", adjusted),
			zap.Int("teto_combinado", maxSnapshotBatchRows))
		snapshotFetchSize = adjusted
	}

	cfg := &sourceConfig{
		KafkaBrokers:         resolveKafkaBrokers(),
		OutputTopic:          os.Getenv("OUTPUT_TOPIC"),
		StateTopic:           os.Getenv("STATE_TOPIC"),
		PGHost:               strings.TrimSpace(os.Getenv("PG_HOST")),
		PGPort:               envOrDefault("PG_PORT", "5432"),
		PGDatabase:           strings.TrimSpace(os.Getenv("PG_DATABASE")),
		PGUser:               strings.TrimSpace(os.Getenv("PG_USER")),
		PGPassword:           os.Getenv("PG_PASSWORD"),
		PGSSLMode:            envOrDefault("PG_SSLMODE", "require"),
		SourceTables:         splitCSV(os.Getenv("SOURCE_TABLES")),
		SlotName:             os.Getenv("SLOT_NAME"),
		PublicationName:      os.Getenv("PUBLICATION_NAME"),
		SnapshotFetchSize:    snapshotFetchSize,
		SnapshotWorkers:      snapshotWorkers,
		PGPoolMaxConns:       int32(envInt("PG_POOL_MAX_CONNS", snapshotWorkers+6)),
		KafkaBatchSize:       envInt("KAFKA_BATCH_SIZE", 1000),
		KafkaBatchBytes:      envInt64("KAFKA_BATCH_BYTES", 5*1024*1024),
		KafkaCompression:     strings.ToLower(envOrDefault("KAFKA_COMPRESSION", "lz4")),
		StatusUpdateInterval: time.Duration(envInt("STATUS_UPDATE_INTERVAL_SECONDS", 10)) * time.Second,
		SlotMonitorInterval:  time.Duration(envInt("SLOT_MONITOR_INTERVAL_SECONDS", 30)) * time.Second,
		SlotLagWarnBytes:     envInt64("SLOT_LAG_WARN_BYTES", 512*1024*1024),
		ViewRefreshInterval:  time.Duration(envInt("VIEW_REFRESH_INTERVAL_SECONDS", 0)) * time.Second,
	}

	if cfg.StateTopic == "" {
		cfg.StateTopic = cfg.OutputTopic + "-state"
	}
	if cfg.PublicationName == "" {
		cfg.PublicationName = cfg.SlotName
	}

	switch {
	case len(cfg.KafkaBrokers) == 0:
		return nil, fmt.Errorf("KAFKA_BROKERS (ou INTHUB_KAFKA_CONNECTION) é obrigatório")
	case cfg.OutputTopic == "":
		return nil, fmt.Errorf("OUTPUT_TOPIC é obrigatório")
	case cfg.PGHost == "":
		return nil, fmt.Errorf("PG_HOST é obrigatório")
	case cfg.PGDatabase == "":
		return nil, fmt.Errorf("PG_DATABASE é obrigatório")
	case cfg.PGUser == "":
		return nil, fmt.Errorf("PG_USER é obrigatório")
	case cfg.PGPassword == "":
		return nil, fmt.Errorf("PG_PASSWORD é obrigatório (configure como Secret)")
	case len(cfg.SourceTables) == 0:
		return nil, fmt.Errorf("SOURCE_TABLES é obrigatório (lista schema.tabela separada por vírgula)")
	case cfg.SlotName == "":
		return nil, fmt.Errorf("SLOT_NAME é obrigatório (precisa ser único por node/deploy)")
	}

	// Loga os knobs que mais pesam na memória do snapshot — sem isso, um OOM
	// em produção não dá pra diagnosticar via log: os valores efetivos vêm de
	// env vars do deploy (fora deste repo) e nunca aparecem em lugar nenhum.
	log.Info("config efetiva de tuning carregada",
		zap.Int("snapshot_workers", cfg.SnapshotWorkers),
		zap.Int("snapshot_fetch_size", cfg.SnapshotFetchSize),
		zap.Int32("pg_pool_max_conns", cfg.PGPoolMaxConns),
		zap.Int("kafka_batch_size", cfg.KafkaBatchSize),
		zap.Int64("kafka_batch_bytes", cfg.KafkaBatchBytes))

	return cfg, nil
}

// resolveKafkaBrokers prioriza KAFKA_BROKERS (declarada explicitamente como
// variável plain) e cai para INTHUB_KAFKA_CONNECTION se ela não estiver
// setada — é a variável que a InthHub injeta automaticamente quando a
// pipeline usa uma conexão cadastrada em Kafka → Conexões em vez de um valor
// digitado à mão (ver "Conexões Kafka por cluster" na doc de deploy).
func resolveKafkaBrokers() []string {
	if brokers := splitCSV(os.Getenv("KAFKA_BROKERS")); len(brokers) > 0 {
		return brokers
	}
	return splitCSV(os.Getenv("INTHUB_KAFKA_CONNECTION"))
}

// resolveKafkaCompression mapeia KAFKA_COMPRESSION pro codec do kafka-go.
// Default é lz4 pra já sair alinhado com o padrão do cluster (menor uso de
// disco no broker); nome desconhecido cai pra lz4 também, com um warning,
// em vez de derrubar o node por causa de um typo na env var.
func resolveKafkaCompression(name string, log *zap.Logger) kafka.Compression {
	switch name {
	case "lz4":
		return kafka.Lz4
	case "snappy":
		return kafka.Snappy
	case "gzip":
		return kafka.Gzip
	case "zstd":
		return kafka.Zstd
	case "none":
		return kafka.Compression(0)
	default:
		log.Warn("KAFKA_COMPRESSION desconhecido, usando lz4", zap.String("valor_recebido", name))
		return kafka.Lz4
	}
}

func splitCSV(s string) []string {
	if strings.TrimSpace(s) == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func envInt64(key string, def int64) int64 {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			return n
		}
	}
	return def
}

func envOrDefault(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
}

// buildPGConnString monta a connection string do Postgres a partir dos
// campos separados (host/porta/banco plain, usuário/senha secret) — usuário
// e senha passam por url.UserPassword pra escapar corretamente caracteres
// especiais (@, :, /, etc.) que apareçam na senha.
func buildPGConnString(cfg *sourceConfig) string {
	u := url.URL{
		Scheme: "postgres",
		User:   url.UserPassword(cfg.PGUser, cfg.PGPassword),
		Host:   fmt.Sprintf("%s:%s", cfg.PGHost, cfg.PGPort),
		Path:   "/" + cfg.PGDatabase,
	}
	q := u.Query()
	q.Set("sslmode", cfg.PGSSLMode)
	u.RawQuery = q.Encode()
	return u.String()
}

// =============================================================================
// Estado / checkpoint — persistido num tópico Kafka compactado (STATE_TOPIC),
// pra sobreviver a restart sem precisar de nenhum storage fora do Kafka.
// =============================================================================

const stateCheckpointKey = "checkpoint"

type tableSnapshotState struct {
	Done       bool            `json:"done"`
	LastPKJSON json.RawMessage `json:"last_pk,omitempty"`
}

type pipelineState struct {
	ConfirmedLSN string                        `json:"confirmed_lsn,omitempty"`
	Snapshots    map[string]tableSnapshotState `json:"snapshots"`
	UpdatedAt    time.Time                     `json:"updated_at"`
}

func newEmptyState() *pipelineState {
	return &pipelineState{Snapshots: map[string]tableSnapshotState{}}
}

func loadState(ctx context.Context, brokers []string, topic string, log *zap.Logger) (*pipelineState, error) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: brokers,
		Topic:   topic,
		// Sempre partição 0: saveState/stateWriter escreve sem balancer,
		// forçando a mesma partição — se o tópico tiver mais de uma (comum
		// se foi criado com o partition count default do cluster em vez de
		// 1), ler sem fixar a partição pegaria só a 0 por padrão do kafka-go
		// enquanto a escrita podia cair em qualquer uma via hash da key,
		// fazendo o checkpoint nunca ser encontrado na releitura.
		Partition: 0,
		MinBytes:  1,
		MaxBytes:  10e6,
	})
	defer reader.Close()

	state := newEmptyState()
	for {
		readCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		msg, err := reader.ReadMessage(readCtx)
		cancel()
		if err != nil {
			if !errors.Is(err, context.DeadlineExceeded) {
				log.Warn("erro lendo checkpoint do STATE_TOPIC, seguindo com estado vazio (snapshot pode reiniciar do zero)", zap.Error(err))
			}
			break // timeout == chegamos no fim do tópico (ou ele está vazio)
		}
		applyStateRecord(state, msg)
		if msg.Offset >= msg.HighWaterMark-1 {
			break
		}
	}

	log.Info("checkpoint carregado", zap.String("confirmed_lsn", state.ConfirmedLSN), zap.Int("tabelas_com_progresso", len(state.Snapshots)))
	return state, nil
}

func applyStateRecord(state *pipelineState, msg kafka.Message) {
	if len(msg.Value) == 0 || string(msg.Key) != stateCheckpointKey {
		return
	}
	var loaded pipelineState
	if err := json.Unmarshal(msg.Value, &loaded); err == nil {
		if loaded.Snapshots == nil {
			loaded.Snapshots = map[string]tableSnapshotState{}
		}
		*state = loaded
	}
}

// singlePartitionBalancer força toda mensagem pra partição 0, sempre — é o
// que garante que o checkpoint escrito bata com a partição 0 fixa que
// loadState lê, não importa quantas partições o STATE_TOPIC realmente tenha.
type singlePartitionBalancer struct{}

func (singlePartitionBalancer) Balance(_ kafka.Message, _ ...int) int { return 0 }

// kafkaWriteRetryAttempts/kafkaWriteRetryBackoff cobrem o caso do
// OUTPUT_TOPIC/STATE_TOPIC ainda não estar visível pra esse Writer no
// momento do primeiro write — o platform cria o tópico de forma assíncrona,
// e o Writer resolve o número de partições via metadata antes de cada
// WriteMessages, sem retry próprio pra esse passo (só o publish em si tem
// retry interno). Sem isso, um "Unknown Topic Or Partition" transitório
// derruba o pipeline inteiro e força reprocessar snapshot sem ponto
// consistente exportado (cenário de recuperação).
const (
	kafkaWriteRetryAttempts = 5
	kafkaWriteRetryBackoff  = 2 * time.Second
)

func isRetryableKafkaError(err error) bool {
	return errors.Is(err, kafka.UnknownTopicOrPartition) ||
		errors.Is(err, kafka.LeaderNotAvailable) ||
		errors.Is(err, kafka.NotLeaderForPartition) ||
		errors.Is(err, kafka.RequestTimedOut)
}

// writeMessagesWithRetry envolve Writer.WriteMessages com algumas
// re-tentativas pros erros transitórios de metadata acima — pra qualquer
// outro erro (ex: mensagem grande demais, contexto cancelado), devolve na
// primeira tentativa, sem mascarar erro real.
func writeMessagesWithRetry(ctx context.Context, writer *kafka.Writer, msgs ...kafka.Message) error {
	var err error
	for attempt := 0; attempt < kafkaWriteRetryAttempts; attempt++ {
		if err = writer.WriteMessages(ctx, msgs...); err == nil || !isRetryableKafkaError(err) {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(kafkaWriteRetryBackoff):
		}
	}
	return err
}

func saveState(ctx context.Context, writer *kafka.Writer, state *pipelineState) error {
	state.UpdatedAt = time.Now().UTC()
	value, err := json.Marshal(state)
	if err != nil {
		return err
	}
	return writeMessagesWithRetry(ctx, writer, kafka.Message{Key: []byte(stateCheckpointKey), Value: value})
}

// =============================================================================
// Publicação dos eventos de dados no OUTPUT_TOPIC.
//
// Convenção de mensagem:
//   - Key: JSON flat só com as colunas de PK, ex: {"id":123}.
//   - Value (insert/update/snapshot): JSON flat com todas as colunas da linha.
//   - Value (delete): vazio (tombstone) — mesma convenção de "delete via CDC"
//     que os consumidores desses tópicos já tratam.
//   - Headers: metadados fora do value (table/op/ts/lsn), pra manter o value
//     100% flat pro sink connector que vai transacionar isso pra outro banco.
// =============================================================================

type cdcOp string

const (
	opSnapshot cdcOp = "r"
	opInsert   cdcOp = "c"
	opUpdate   cdcOp = "u"
	opDelete   cdcOp = "d"
)

// encodeMessage monta a mensagem Kafka na convenção usada por todo o
// pipeline (snapshot e CDC): key = colunas de PK/replica identity, value =
// linha completa (nil no delete), headers = table/op/ts/lsn. Única
// implementação de encoding — antes existiam três (publishRow,
// publishTombstone, publishRowsBatch) fazendo o mesmo par de json.Marshal.
func encodeMessage(table string, keyValues, row map[string]any, op cdcOp, lsn string) (kafka.Message, error) {
	key, err := json.Marshal(keyValues)
	if err != nil {
		return kafka.Message{}, fmt.Errorf("encode key: %w", err)
	}
	var value []byte
	if row != nil {
		value, err = json.Marshal(flattenRow(row))
		if err != nil {
			return kafka.Message{}, fmt.Errorf("encode value: %w", err)
		}
	}
	return kafka.Message{Key: key, Value: value, Headers: buildKafkaHeaders(table, op, lsn)}, nil
}

// flattenKeySep separa nível de aninhamento nas chaves achatadas (ex.:
// "config_limite_valor"). "_" em vez de "." de propósito: o value flat
// existe pra um sink connector transacionar isso em outra tabela de banco
// (ver comentário da seção acima), e "." não é um caractere seguro em nome
// de coluna na maioria dos bancos (Postgres incluso, sem quoting) nem em
// nome de campo Avro/Kafka Connect — "_" chega direto como coluna válida do
// outro lado sem tratamento extra.
const flattenKeySep = "_"

// flattenRow achata colunas json/jsonb com objeto aninhado em chaves
// "coluna_subchave" no nível raiz do value — o pgx decodifica json/jsonb
// automaticamente em map[string]any, então sem isso uma coluna desse tipo
// vira JSON dentro de JSON na mensagem, quebrando a convenção de "value
// 100% flat" que os sink connectors downstream esperam. Roda
// incondicionalmente pra qualquer tabela/coluna, sem depender de env var:
// não faz sentido essa normalização ser opt-in, já que o formato "flat" é a
// garantia que este node dá pra quem consome o tópico.
// Array não é expandido por índice — vira uma string com o JSON do array
// serializado (ex.: "parcelas": "[{...},{...}]") em vez de ficar como valor
// nativo aninhado. Expandir por índice (ex.: "parcelas_0_valor",
// "parcelas_1_valor") deixaria o formato instável, com o número de colunas
// mudando conforme o tamanho do array varia entre linhas (uma lista de
// parcelas de financiamento pode ter dezenas de itens) — inviável pra um
// sink JDBC/schema fixo do outro lado. Virar string mantém o value
// estruturalmente 100% flat (todo valor é escalar) sem gerar esse
// problema, ao custo de o consumidor ter que decodificar esse campo
// específico se precisar dos itens individualmente.
func flattenRow(row map[string]any) map[string]any {
	flat := make(map[string]any, len(row))
	// Ordena as chaves de nível raiz antes de achatar pra que, no caso raro
	// de colisão (coluna "a.b" literal + coluna jsonb "a" com subchave "b"),
	// o resultado seja determinístico entre execuções, já que a ordem de
	// iteração de um map em Go não é.
	keys := make([]string, 0, len(row))
	for k := range row {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		flattenInto(flat, k, row[k])
	}
	return flat
}

func flattenInto(dst map[string]any, prefix string, v any) {
	if arr, ok := v.([]any); ok {
		if b, err := json.Marshal(arr); err == nil {
			dst[prefix] = string(b)
		} else {
			dst[prefix] = v
		}
		return
	}
	nested, ok := v.(map[string]any)
	if !ok || len(nested) == 0 {
		dst[prefix] = v
		return
	}
	subKeys := make([]string, 0, len(nested))
	for k := range nested {
		subKeys = append(subKeys, k)
	}
	sort.Strings(subKeys)
	for _, k := range subKeys {
		flattenInto(dst, prefix+flattenKeySep+k, nested[k])
	}
}

func publishRow(ctx context.Context, writer *kafka.Writer, table string, keyValues, row map[string]any, op cdcOp, lsn string) error {
	msg, err := encodeMessage(table, keyValues, row, op, lsn)
	if err != nil {
		return err
	}
	return writeMessagesWithRetry(ctx, writer, msg)
}

func publishTombstone(ctx context.Context, writer *kafka.Writer, table string, keyValues map[string]any, lsn string) error {
	msg, err := encodeMessage(table, keyValues, nil, opDelete, lsn)
	if err != nil {
		return err
	}
	return writeMessagesWithRetry(ctx, writer, msg)
}

func buildKafkaHeaders(table string, op cdcOp, lsn string) []kafka.Header {
	h := []kafka.Header{
		{Key: "table", Value: []byte(table)},
		{Key: "op", Value: []byte(op)},
		{Key: "ts", Value: []byte(strconv.FormatInt(time.Now().UTC().UnixMilli(), 10))},
	}
	if lsn != "" {
		h = append(h, kafka.Header{Key: "lsn", Value: []byte(lsn)})
	}
	return h
}

// =============================================================================
// Catálogo — resolve cada "schema.nome" de SOURCE_TABLES contra pg_class,
// classificando tabela normal / particionada / view, e descobre a PK.
// =============================================================================

type relKind string

const (
	kindTable    relKind = "table"
	kindPartRoot relKind = "part_root"
	kindView     relKind = "view"
	kindMatView  relKind = "matview"
)

type tableInfo struct {
	Schema    string
	Name      string
	Kind      relKind
	PKColumns []string
}

func (t tableInfo) qualified() string { return t.Schema + "." + t.Name }

// cdcCapable indica se o objeto pode ser capturado via replicação lógica —
// só tabelas normais e particionadas têm WAL; views não.
func (t tableInfo) cdcCapable() bool { return t.Kind == kindTable || t.Kind == kindPartRoot }

func inspectTables(ctx context.Context, pool *pgxpool.Pool, qualifiedNames []string) ([]tableInfo, error) {
	out := make([]tableInfo, 0, len(qualifiedNames))
	for _, qn := range qualifiedNames {
		schema, name, err := splitQualified(qn)
		if err != nil {
			return nil, err
		}

		var kind string
		err = pool.QueryRow(ctx, `
			SELECT c.relkind::text FROM pg_class c
			JOIN pg_namespace n ON n.oid = c.relnamespace
			WHERE n.nspname = $1 AND c.relname = $2
		`, schema, name).Scan(&kind)
		if err != nil {
			if err == pgx.ErrNoRows {
				return nil, fmt.Errorf("objeto %s não encontrado (schema/nome errado ou sem permissão)", qn)
			}
			return nil, fmt.Errorf("inspecionar %s: %w", qn, err)
		}

		info := tableInfo{Schema: schema, Name: name}
		switch kind {
		case "r":
			info.Kind = kindTable
		case "p":
			info.Kind = kindPartRoot
		case "v":
			info.Kind = kindView
		case "m":
			info.Kind = kindMatView
		default:
			return nil, fmt.Errorf("objeto %s tem relkind %q não suportado", qn, kind)
		}

		if info.cdcCapable() {
			pk, err := primaryKeyColumns(ctx, pool, qn)
			if err != nil {
				return nil, err
			}
			if len(pk) == 0 {
				return nil, fmt.Errorf("tabela %s não tem chave primária: obrigatório pra CDC (REPLICA IDENTITY DEFAULT ou FULL)", qn)
			}
			info.PKColumns = pk
		}
		out = append(out, info)
	}
	return out, nil
}

func primaryKeyColumns(ctx context.Context, pool *pgxpool.Pool, qualified string) ([]string, error) {
	rows, err := pool.Query(ctx, `
		SELECT a.attname
		FROM pg_index i
		JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
		WHERE i.indrelid = $1::regclass AND i.indisprimary
		ORDER BY array_position(i.indkey, a.attnum)
	`, qualified)
	if err != nil {
		return nil, fmt.Errorf("buscar PK de %s: %w", qualified, err)
	}
	defer rows.Close()

	var cols []string
	for rows.Next() {
		var col string
		if err := rows.Scan(&col); err != nil {
			return nil, err
		}
		cols = append(cols, col)
	}
	return cols, rows.Err()
}

func splitQualified(qn string) (schema, name string, err error) {
	parts := strings.SplitN(qn, ".", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", "", fmt.Errorf("nome %q inválido em SOURCE_TABLES, esperado schema.tabela", qn)
	}
	return parts[0], parts[1], nil
}

func qualifiedIdent(t tableInfo) string {
	return pgx.Identifier{t.Schema, t.Name}.Sanitize()
}

// discoverPartitions lista as partições-filha físicas de uma tabela
// particionada (pg_inherits). É o que permite ler cada partição em paralelo
// no snapshot em vez de escanear a tabela-raiz inteira sequencialmente — em
// tabela particionada de dezenas de milhões de linhas, isso é o maior ganho
// de tempo de carga inicial. As colunas de PK são herdadas da raiz.
func discoverPartitions(ctx context.Context, pool *pgxpool.Pool, root tableInfo) ([]tableInfo, error) {
	rows, err := pool.Query(ctx, `
		SELECT n.nspname, c.relname
		FROM pg_inherits i
		JOIN pg_class c ON c.oid = i.inhrelid
		JOIN pg_class p ON p.oid = i.inhparent
		JOIN pg_namespace n ON n.oid = c.relnamespace
		JOIN pg_namespace pn ON pn.oid = p.relnamespace
		WHERE pn.nspname = $1 AND p.relname = $2
		ORDER BY c.relname
	`, root.Schema, root.Name)
	if err != nil {
		return nil, fmt.Errorf("descobrir partições de %s: %w", root.qualified(), err)
	}
	defer rows.Close()

	var partitions []tableInfo
	for rows.Next() {
		var schema, name string
		if err := rows.Scan(&schema, &name); err != nil {
			return nil, err
		}
		partitions = append(partitions, tableInfo{Schema: schema, Name: name, Kind: kindTable, PKColumns: root.PKColumns})
	}
	return partitions, rows.Err()
}

// =============================================================================
// Publication + replication slot.
// =============================================================================

// isTableInPublication checa a associação direto no catálogo (pg_publication_rel,
// por OID via ::regclass) em vez da view pg_publication_tables. A view expande
// tabela particionada em partições-folha ou na raiz dependendo da flag
// publish_via_partition_root da publication — se uma publication pré-existente
// (criada por fora, ex: por um DBA) não tiver essa flag, a view pode listar as
// partições-folha em vez da tabela-raiz configurada em SOURCE_TABLES, dando um
// falso "não está na publication" e disparando um ALTER desnecessário (que
// falha se o usuário do app não for dono da publication). Checar por OID
// direto no catálogo evita essa ambiguidade — é exatamente o que
// CREATE/ALTER PUBLICATION ... FOR TABLE registra, sem expansão nenhuma.
func isTableInPublication(ctx context.Context, pool *pgxpool.Pool, pubName, qualifiedTable string) (bool, error) {
	var member bool
	err := pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM pg_publication_rel pr
			JOIN pg_publication p ON p.oid = pr.prpubid
			WHERE p.pubname = $1 AND pr.prrelid = $2::regclass
		)
	`, pubName, qualifiedTable).Scan(&member)
	return member, err
}

// ensurePublication cria a publication se não existir e garante que todas as
// tabelas CDC-capable estejam nela. publish_via_partition_root=true faz o
// Postgres publicar mudanças de qualquer partição-folha como se fossem da
// tabela particionada raiz — trata tabela particionada e normal da mesma forma.
func ensurePublication(ctx context.Context, pool *pgxpool.Pool, pubName string, tables []tableInfo, log *zap.Logger) error {
	cdcTables := make([]tableInfo, 0, len(tables))
	for _, t := range tables {
		if t.cdcCapable() {
			cdcTables = append(cdcTables, t)
		}
	}
	if len(cdcTables) == 0 {
		return fmt.Errorf("nenhuma tabela CDC-capable em SOURCE_TABLES (só views/materialized views não bastam)")
	}

	var exists bool
	if err := pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = $1)`, pubName).Scan(&exists); err != nil {
		return fmt.Errorf("checar publication: %w", err)
	}

	if !exists {
		list := make([]string, len(cdcTables))
		for i, t := range cdcTables {
			list[i] = qualifiedIdent(t)
		}
		stmt := fmt.Sprintf(`CREATE PUBLICATION %s FOR TABLE %s WITH (publish_via_partition_root = true)`,
			pgx.Identifier{pubName}.Sanitize(), strings.Join(list, ", "))
		if _, err := pool.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("criar publication %s: %w", pubName, err)
		}
		log.Info("publication criada", zap.String("publication", pubName), zap.Int("tabelas", len(cdcTables)))
		return nil
	}

	var missing []string
	for _, t := range cdcTables {
		member, err := isTableInPublication(ctx, pool, pubName, t.qualified())
		if err != nil {
			return fmt.Errorf("checar se %s já está na publication: %w", t.qualified(), err)
		}
		if !member {
			missing = append(missing, qualifiedIdent(t))
		}
	}
	if len(missing) > 0 {
		stmt := fmt.Sprintf(`ALTER PUBLICATION %s ADD TABLE %s`, pgx.Identifier{pubName}.Sanitize(), strings.Join(missing, ", "))
		if _, err := pool.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("adicionar tabelas na publication %s: %w", pubName, err)
		}
		log.Info("tabelas adicionadas na publication existente", zap.String("publication", pubName), zap.Strings("tabelas", missing))
	}
	return nil
}

type slotBootstrap struct {
	Created         bool
	ConsistentPoint pglogrepl.LSN
	SnapshotName    string
}

// ensureReplicationSlot cria o slot lógico (plugin pgoutput) se ainda não
// existir, exportando um snapshot consistente — técnica padrão pra não ter
// gap nem overlap entre o snapshot inicial e o streaming de CDC.
func ensureReplicationSlot(ctx context.Context, pool *pgxpool.Pool, replConn *pgconn.PgConn, slotName string) (*slotBootstrap, error) {
	var exists bool
	if err := pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)`, slotName).Scan(&exists); err != nil {
		return nil, fmt.Errorf("checar replication slot: %w", err)
	}
	if exists {
		return &slotBootstrap{Created: false}, nil
	}

	result, err := pglogrepl.CreateReplicationSlot(ctx, replConn, slotName, "pgoutput", pglogrepl.CreateReplicationSlotOptions{
		Temporary:      false,
		SnapshotAction: "EXPORT_SNAPSHOT",
	})
	if err != nil {
		return nil, fmt.Errorf("criar replication slot %s: %w", slotName, err)
	}
	lsn, err := pglogrepl.ParseLSN(result.ConsistentPoint)
	if err != nil {
		return nil, fmt.Errorf("parsear consistent point do slot: %w", err)
	}
	return &slotBootstrap{Created: true, ConsistentPoint: lsn, SnapshotName: result.SnapshotName}, nil
}

func replicationConnString(dsn string) (string, error) {
	u, err := url.Parse(dsn)
	if err != nil {
		return "", fmt.Errorf("connection string do Postgres inválida: %w", err)
	}
	q := u.Query()
	q.Set("replication", "database")
	u.RawQuery = q.Encode()
	return u.String(), nil
}

// =============================================================================
// Snapshot inicial (+ refresh periódico de views).
// =============================================================================

// runInitialSnapshot processa uma tabela/view de cada vez, mas dentro de uma
// tabela particionada, paraleliza a leitura entre as partições-filha
// (snapshotWorkers concorrentes) — é o que faz o snapshot de dezenas de
// milhões de linhas em poucas partições grandes escalar em vez de ficar preso
// num único scan sequencial. Toda unidade de trabalho (tabela inteira,
// partição-filha, ou view) roda na sua própria conexão/transação, todas
// importando o mesmo snapshot exportado do slot pra ler no mesmo ponto
// consistente — é a mesma técnica que pg_dump --jobs usa.
func runInitialSnapshot(ctx context.Context, pool *pgxpool.Pool, producer, stateWriter *kafka.Writer, fetchSize, snapshotWorkers int, tables []tableInfo, snapshotName string, st *pipelineState, log *zap.Logger) error {
	metricSnapshotInProgress.Set(1)
	defer metricSnapshotInProgress.Set(0)

	if snapshotName == "" {
		log.Warn("snapshot sem ponto consistente exportado do slot — pequena janela de overlap/gap possível (cenário de recuperação após falha no meio do bootstrap)")
	}

	var stateMu sync.Mutex

	for _, t := range tables {
		stateMu.Lock()
		done := st.Snapshots[t.qualified()].Done
		stateMu.Unlock()
		if done {
			continue
		}

		if t.Kind == kindPartRoot {
			if err := snapshotPartitionedRoot(ctx, pool, producer, stateWriter, fetchSize, snapshotWorkers, t, snapshotName, st, &stateMu, log); err != nil {
				return fmt.Errorf("snapshot de %s: %w", t.qualified(), err)
			}
			continue
		}

		tlog := log.With(zap.String("table", t.qualified()))
		tlog.Info("iniciando snapshot")
		n, err := snapshotUnit(ctx, pool, producer, stateWriter, fetchSize, qualifiedIdent(t), t.qualified(), t.qualified(), t.PKColumns, t.cdcCapable(), snapshotName, st, &stateMu, tlog)
		if err != nil {
			return fmt.Errorf("snapshot de %s: %w", t.qualified(), err)
		}
		tlog.Info("snapshot concluído", zap.Int("total_linhas", n))
	}

	return nil
}

// snapshotPartitionedRoot descobre as partições-filha físicas e as lê em
// paralelo (bounded por snapshotWorkers), cada uma na sua própria
// conexão/transação. As mensagens saem no Kafka com o nome da tabela-raiz
// (publishAs), nunca o nome físico da partição — quem consome não precisa
// saber que a tabela é particionada. O checkpoint rastreia progresso por
// partição individualmente, então se cair no meio só retoma as que faltam.
func snapshotPartitionedRoot(ctx context.Context, pool *pgxpool.Pool, producer, stateWriter *kafka.Writer, fetchSize, snapshotWorkers int, root tableInfo, snapshotName string, st *pipelineState, stateMu *sync.Mutex, log *zap.Logger) error {
	partitions, err := discoverPartitions(ctx, pool, root)
	if err != nil {
		return err
	}
	if len(partitions) == 0 {
		// Tabela particionada sem nenhuma partição-filha criada ainda: não
		// tem de onde ler (a raiz de uma tabela PARTITION BY não guarda
		// linhas). Marca como concluída — não há dado a capturar.
		log.Warn("tabela particionada sem partições-filha, nada pra ler no snapshot", zap.String("table", root.qualified()))
		stateMu.Lock()
		st.Snapshots[root.qualified()] = tableSnapshotState{Done: true}
		err := saveState(ctx, stateWriter, st)
		stateMu.Unlock()
		return err
	}

	rlog := log.With(zap.String("table", root.qualified()), zap.Int("particoes", len(partitions)))
	rlog.Info("iniciando snapshot paralelo por partição")

	if snapshotWorkers < 1 {
		snapshotWorkers = 1
	}
	sem := make(chan struct{}, snapshotWorkers)
	var wg sync.WaitGroup
	errCh := make(chan error, len(partitions))

	for _, p := range partitions {
		stateMu.Lock()
		done := st.Snapshots[p.qualified()].Done
		stateMu.Unlock()
		if done {
			continue
		}

		wg.Add(1)
		sem <- struct{}{}
		go func(p tableInfo) {
			defer wg.Done()
			defer func() { <-sem }()

			plog := rlog.With(zap.String("particao", p.qualified()))
			plog.Info("iniciando snapshot da partição")
			n, err := snapshotUnit(ctx, pool, producer, stateWriter, fetchSize, qualifiedIdent(p), root.qualified(), p.qualified(), root.PKColumns, true, snapshotName, st, stateMu, plog)
			if err != nil {
				errCh <- fmt.Errorf("partição %s: %w", p.qualified(), err)
				return
			}
			plog.Info("snapshot da partição concluído", zap.Int("total_linhas", n))
		}(p)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			return err
		}
	}

	stateMu.Lock()
	st.Snapshots[root.qualified()] = tableSnapshotState{Done: true}
	err = saveState(ctx, stateWriter, st)
	stateMu.Unlock()
	return err
}

// snapshotUnit lê UM objeto físico (tabela normal, partição-filha ou view) do
// início ao fim, paginando por keyset quando há PK (tabelas/partições) ou
// numa leitura só (views, sem PK confiável pra paginar). readFromIdent é de
// onde ler; publishAs é o nome que vai nas mensagens (a tabela-raiz, mesmo
// lendo de uma partição-filha); stateKey é a chave do checkpoint (física —
// permite retomar partição por partição).
func snapshotUnit(ctx context.Context, pool *pgxpool.Pool, producer, stateWriter *kafka.Writer, fetchSize int, readFromIdent, publishAs, stateKey string, pkCols []string, hasPK bool, snapshotName string, st *pipelineState, stateMu *sync.Mutex, log *zap.Logger) (int, error) {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return 0, fmt.Errorf("adquirir conexão pro snapshot: %w", err)
	}
	defer conn.Release()

	tx, err := conn.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return 0, fmt.Errorf("abrir transação de snapshot: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	if snapshotName != "" {
		if _, err := tx.Exec(ctx, fmt.Sprintf("SET TRANSACTION SNAPSHOT '%s'", snapshotName)); err != nil {
			return 0, fmt.Errorf("aplicar snapshot exportado do slot: %w", err)
		}
	}

	rowCount := 0

	if hasPK {
		stateMu.Lock()
		var lastPK []any
		if raw := st.Snapshots[stateKey].LastPKJSON; len(raw) > 0 {
			if err := json.Unmarshal(raw, &lastPK); err != nil {
				stateMu.Unlock()
				return 0, fmt.Errorf("decodificar cursor salvo: %w", err)
			}
		}
		stateMu.Unlock()

		for {
			msgs, next, err := fetchAndEncodeBatch(ctx, tx, fetchSize, readFromIdent, publishAs, pkCols, lastPK)
			if err != nil {
				return rowCount, err
			}
			if len(msgs) > 0 {
				if err := writeMessagesWithRetry(ctx, producer, msgs...); err != nil {
					return rowCount, fmt.Errorf("publicar batch: %w", err)
				}
			}
			rowCount += len(msgs)
			metricSnapshotRows.WithLabelValues(publishAs).Add(float64(len(msgs)))

			if len(msgs) < fetchSize {
				stateMu.Lock()
				st.Snapshots[stateKey] = tableSnapshotState{Done: true}
				stateMu.Unlock()
				break
			}
			lastPK = next
			pkJSON, _ := json.Marshal(lastPK)
			stateMu.Lock()
			st.Snapshots[stateKey] = tableSnapshotState{Done: false, LastPKJSON: pkJSON}
			saveErr := saveState(ctx, stateWriter, st)
			stateMu.Unlock()
			if saveErr != nil {
				log.Warn("falha ao salvar checkpoint de progresso do snapshot", zap.Error(saveErr))
			}
			log.Info("progresso do snapshot", zap.Int("linhas_ate_agora", rowCount))
		}
	} else {
		rows, err := tx.Query(ctx, fmt.Sprintf("SELECT * FROM %s", readFromIdent))
		if err != nil {
			return 0, fmt.Errorf("query na view: %w", err)
		}
		// Sem PK não dá pra paginar por cursor como no caminho de cima, mas
		// isso não significa segurar a view inteira em memória — publica em
		// lotes de fetchSize conforme lê, do mesmo jeito que o caminho com PK
		// faz. Sem isso, uma view grande estoura memória de um jeito que
		// nenhum SNAPSHOT_WORKERS/SNAPSHOT_FETCH_SIZE consegue conter, porque
		// o limite nunca era respeitado aqui pra começo de conversa.
		n, err := streamEncodedBatches(ctx, rows, producer, publishAs, fetchSize)
		rows.Close()
		if err != nil {
			return rowCount, fmt.Errorf("snapshot da view: %w", err)
		}
		rowCount = n
		stateMu.Lock()
		st.Snapshots[stateKey] = tableSnapshotState{Done: true}
		stateMu.Unlock()
	}

	stateMu.Lock()
	saveErr := saveState(ctx, stateWriter, st)
	stateMu.Unlock()
	if saveErr != nil {
		return rowCount, saveErr
	}
	return rowCount, tx.Commit(ctx)
}

// fetchAndEncodeBatch pagina por keyset (WHERE pk > último visto) em vez de
// OFFSET — mais barato e estável em tabelas grandes — e já codifica cada
// linha como kafka.Message durante a leitura, em vez de montar o lote
// decodificado (map[string]any) inteiro e só depois convertê-lo pra
// mensagem numa segunda passada: manter as duas representações da mesma
// linha vivas ao mesmo tempo dobra à toa o pico de memória de um lote
// grande, que é justamente o cenário que derruba o pod com OOMKilled.
// readFromIdent já vem sanitizado (schema.tabela ou schema.partição),
// pkCols só os nomes das colunas.
func fetchAndEncodeBatch(ctx context.Context, tx pgx.Tx, fetchSize int, readFromIdent, publishAs string, pkCols []string, lastPK []any) ([]kafka.Message, []any, error) {
	orderCols := make([]string, len(pkCols))
	for i, c := range pkCols {
		orderCols[i] = pgx.Identifier{c}.Sanitize()
	}
	orderBy := strings.Join(orderCols, ", ")

	query := fmt.Sprintf("SELECT * FROM %s", readFromIdent)
	var args []any
	if len(lastPK) > 0 {
		placeholders := make([]string, len(orderCols))
		for i := range orderCols {
			placeholders[i] = fmt.Sprintf("$%d", i+1)
			args = append(args, lastPK[i])
		}
		query += fmt.Sprintf(" WHERE (%s) > (%s)", orderBy, strings.Join(placeholders, ", "))
	}
	query += fmt.Sprintf(" ORDER BY %s LIMIT %d", orderBy, fetchSize)

	rows, err := tx.Query(ctx, query, args...)
	if err != nil {
		return nil, nil, fmt.Errorf("query de batch: %w", err)
	}
	defer rows.Close()

	msgs := make([]kafka.Message, 0, fetchSize)
	var next []any
	for rows.Next() {
		row, err := rowToMap(rows)
		if err != nil {
			return nil, nil, err
		}
		msg, err := encodeMessage(publishAs, extractKey(row, pkCols), row, opSnapshot, "")
		if err != nil {
			return nil, nil, fmt.Errorf("codificar linha: %w", err)
		}
		msgs = append(msgs, msg)

		next = make([]any, len(pkCols))
		for i, c := range pkCols {
			next[i] = row[c]
		}
	}
	if err := rows.Err(); err != nil {
		return nil, nil, err
	}
	return msgs, next, nil
}

// streamEncodedBatches lê linhas sem paginação por PK (usado por view/
// materialized view, que não tem cursor confiável) e publica em lotes de
// fetchSize conforme lê, codificando cada linha assim que é decodificada —
// nunca segura mais que um lote em memória, e nunca duas representações da
// mesma linha ao mesmo tempo. Reusada pelo snapshot inicial de view e pelo
// refresh periódico, que tinham essa lógica de acumular+publicar duplicada.
func streamEncodedBatches(ctx context.Context, rows pgx.Rows, producer *kafka.Writer, publishAs string, fetchSize int) (int, error) {
	msgs := make([]kafka.Message, 0, fetchSize)
	total := 0
	flush := func() error {
		if len(msgs) == 0 {
			return nil
		}
		if err := writeMessagesWithRetry(ctx, producer, msgs...); err != nil {
			return fmt.Errorf("publicar lote: %w", err)
		}
		total += len(msgs)
		metricSnapshotRows.WithLabelValues(publishAs).Add(float64(len(msgs)))
		msgs = msgs[:0]
		return nil
	}

	for rows.Next() {
		row, err := rowToMap(rows)
		if err != nil {
			return total, err
		}
		msg, err := encodeMessage(publishAs, extractKey(row, nil), row, opSnapshot, "")
		if err != nil {
			return total, fmt.Errorf("codificar linha: %w", err)
		}
		msgs = append(msgs, msg)
		if len(msgs) >= fetchSize {
			if err := flush(); err != nil {
				return total, err
			}
		}
	}
	if err := rows.Err(); err != nil {
		return total, err
	}
	if err := flush(); err != nil {
		return total, err
	}
	return total, nil
}

func rowToMap(rows pgx.Rows) (map[string]any, error) {
	values, err := rows.Values()
	if err != nil {
		return nil, err
	}
	fields := rows.FieldDescriptions()
	row := make(map[string]any, len(fields))
	for i, f := range fields {
		row[string(f.Name)] = values[i]
	}
	return row, nil
}

func extractKey(row map[string]any, pkCols []string) map[string]any {
	key := make(map[string]any, len(pkCols))
	for _, c := range pkCols {
		key[c] = row[c]
	}
	return key
}

// refreshViews relê as views inteiras e republica, sem depender de checkpoint
// (views não são paginadas por PK — cada ciclo é uma leitura completa nova).
func refreshViews(ctx context.Context, pool *pgxpool.Pool, producer *kafka.Writer, views []tableInfo, fetchSize int, log *zap.Logger) error {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("adquirir conexão pro refresh de views: %w", err)
	}
	defer conn.Release()

	tx, err := conn.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	for _, v := range views {
		vlog := log.With(zap.String("view", v.qualified()))
		rows, err := tx.Query(ctx, fmt.Sprintf("SELECT * FROM %s", qualifiedIdent(v)))
		if err != nil {
			return fmt.Errorf("query na view %s: %w", v.qualified(), err)
		}
		total, err := streamEncodedBatches(ctx, rows, producer, v.qualified(), fetchSize)
		rows.Close()
		if err != nil {
			return fmt.Errorf("refresh da view %s: %w", v.qualified(), err)
		}
		vlog.Info("refresh periódico da view concluído", zap.Int("linhas", total))
	}
	return tx.Commit(ctx)
}

// =============================================================================
// Streaming de CDC via replicação lógica (pgoutput).
// =============================================================================

type replicator struct {
	slotName        string
	publicationName string
	producer        *kafka.Writer
	stateWriter     *kafka.Writer
	statusInterval  time.Duration
	log             *zap.Logger

	typeMap   *pgtype.Map
	relations map[uint32]*pglogrepl.RelationMessage
}

func newReplicator(slotName, publicationName string, producer, stateWriter *kafka.Writer, statusInterval time.Duration, log *zap.Logger) *replicator {
	return &replicator{
		slotName:        slotName,
		publicationName: publicationName,
		producer:        producer,
		stateWriter:     stateWriter,
		statusInterval:  statusInterval,
		log:             log,
		typeMap:         pgtype.NewMap(),
		relations:       map[uint32]*pglogrepl.RelationMessage{},
	}
}

// run consome o WAL a partir da conexão de replicação já aberta (a mesma
// usada pro bootstrap do slot, pra não perder a validade do snapshot
// exportado) até o ctx ser cancelado.
func (r *replicator) run(ctx context.Context, conn *pgconn.PgConn, st *pipelineState, startLSN pglogrepl.LSN) error {
	pluginArgs := []string{"proto_version '1'", fmt.Sprintf("publication_names '%s'", r.publicationName)}
	if err := pglogrepl.StartReplication(ctx, conn, r.slotName, startLSN, pglogrepl.StartReplicationOptions{PluginArgs: pluginArgs}); err != nil {
		return fmt.Errorf("start replication: %w", err)
	}
	r.log.Info("streaming de CDC iniciado", zap.String("slot", r.slotName), zap.String("from_lsn", startLSN.String()))

	clientXLogPos := startLSN
	lastCommitLSN := startLSN
	nextStandby := time.Now().Add(r.statusInterval)

	for {
		if time.Now().After(nextStandby) {
			if err := pglogrepl.SendStandbyStatusUpdate(ctx, conn, pglogrepl.StandbyStatusUpdate{
				WALWritePosition: clientXLogPos,
				WALFlushPosition: clientXLogPos,
				WALApplyPosition: clientXLogPos,
			}); err != nil {
				return fmt.Errorf("enviar standby status update: %w", err)
			}
			metricLastConfirmedLSN.Set(float64(clientXLogPos))
			st.ConfirmedLSN = clientXLogPos.String()
			if err := saveState(ctx, r.stateWriter, st); err != nil {
				r.log.Warn("falha ao salvar checkpoint de LSN", zap.Error(err))
			}
			nextStandby = time.Now().Add(r.statusInterval)
		}

		recvCtx, cancel := context.WithDeadline(ctx, nextStandby)
		rawMsg, err := conn.ReceiveMessage(recvCtx)
		cancel()
		if err != nil {
			if pgconn.Timeout(err) {
				continue
			}
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("receber mensagem de replicação: %w", err)
		}

		if errMsg, ok := rawMsg.(*pgproto3.ErrorResponse); ok {
			return fmt.Errorf("erro do servidor durante replicação: %+v", errMsg)
		}

		copyData, ok := rawMsg.(*pgproto3.CopyData)
		if !ok || len(copyData.Data) == 0 {
			continue
		}

		switch copyData.Data[0] {
		case pglogrepl.PrimaryKeepaliveMessageByteID:
			pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(copyData.Data[1:])
			if err != nil {
				return fmt.Errorf("parsear keepalive: %w", err)
			}
			// ServerWALEnd é até onde o servidor já processou. Avança a
			// posição mesmo sem XLogData: se não chegou Insert/Update/Delete
			// é porque não havia nada relevante pra nossa publication nesse
			// trecho (o pgoutput já filtra no servidor) — sem isso, tabelas
			// com pouca escrita numa instância com outras tabelas ativas
			// ficariam com o slot represando WAL de trechos que já foram
			// processados e descartados como irrelevantes.
			if pkm.ServerWALEnd > clientXLogPos {
				clientXLogPos = pkm.ServerWALEnd
			}
			if pkm.ReplyRequested {
				nextStandby = time.Time{}
			}

		case pglogrepl.XLogDataByteID:
			xld, err := pglogrepl.ParseXLogData(copyData.Data[1:])
			if err != nil {
				return fmt.Errorf("parsear XLogData: %w", err)
			}
			commitLSN, err := r.handleWALMessage(ctx, xld.WALData)
			if err != nil {
				return err
			}
			if xld.WALStart > clientXLogPos {
				clientXLogPos = xld.WALStart
			}
			if commitLSN > lastCommitLSN {
				lastCommitLSN = commitLSN
				clientXLogPos = commitLSN
			}
		}
	}
}

// handleWALMessage decodifica uma mensagem lógica do pgoutput e publica no
// Kafka quando for insert/update/delete. Devolve o CommitLSN quando a
// mensagem for um Commit (0 caso contrário), sinalizando até onde é seguro
// avançar o confirmed_flush_lsn do slot (recicla WAL antigo).
func (r *replicator) handleWALMessage(ctx context.Context, walData []byte) (pglogrepl.LSN, error) {
	msg, err := pglogrepl.Parse(walData)
	if err != nil {
		return 0, fmt.Errorf("parsear mensagem pgoutput: %w", err)
	}

	switch v := msg.(type) {
	case *pglogrepl.RelationMessage:
		r.relations[v.RelationID] = v

	case *pglogrepl.InsertMessage:
		rel, ok := r.relations[v.RelationID]
		if !ok {
			return 0, fmt.Errorf("insert de relação desconhecida (id %d)", v.RelationID)
		}
		table := rel.Namespace + "." + rel.RelationName
		row := r.decodeTuple(rel, v.Tuple)
		key := extractKeyFromRelation(rel, row)
		if err := publishRow(ctx, r.producer, table, key, row, opInsert, ""); err != nil {
			metricPublishErrors.WithLabelValues("insert").Inc()
			return 0, err
		}
		metricCDCEvents.WithLabelValues(table, string(opInsert)).Inc()

	case *pglogrepl.UpdateMessage:
		rel, ok := r.relations[v.RelationID]
		if !ok {
			return 0, fmt.Errorf("update de relação desconhecida (id %d)", v.RelationID)
		}
		table := rel.Namespace + "." + rel.RelationName
		row := r.decodeTuple(rel, v.NewTuple)
		key := extractKeyFromRelation(rel, row)
		if err := publishRow(ctx, r.producer, table, key, row, opUpdate, ""); err != nil {
			metricPublishErrors.WithLabelValues("update").Inc()
			return 0, err
		}
		metricCDCEvents.WithLabelValues(table, string(opUpdate)).Inc()

	case *pglogrepl.DeleteMessage:
		rel, ok := r.relations[v.RelationID]
		if !ok {
			return 0, fmt.Errorf("delete de relação desconhecida (id %d)", v.RelationID)
		}
		table := rel.Namespace + "." + rel.RelationName
		if v.OldTuple == nil {
			r.log.Warn("delete sem old tuple — tabela provavelmente com REPLICA IDENTITY NOTHING, não dá pra montar a key; evento descartado", zap.String("table", table))
			return 0, nil
		}
		row := r.decodeTuple(rel, v.OldTuple)
		key := extractKeyFromRelation(rel, row)
		if err := publishTombstone(ctx, r.producer, table, key, ""); err != nil {
			metricPublishErrors.WithLabelValues("delete").Inc()
			return 0, err
		}
		metricCDCEvents.WithLabelValues(table, string(opDelete)).Inc()

	case *pglogrepl.TruncateMessage:
		for _, relID := range v.RelationIDs {
			if rel, ok := r.relations[relID]; ok {
				r.log.Warn("TRUNCATE recebido — não é propagado como eventos individuais de delete", zap.String("table", rel.Namespace+"."+rel.RelationName))
			}
		}

	case *pglogrepl.CommitMessage:
		return v.CommitLSN, nil
	}

	return 0, nil
}

func (r *replicator) decodeTuple(rel *pglogrepl.RelationMessage, tuple *pglogrepl.TupleData) map[string]any {
	row := make(map[string]any, len(tuple.Columns))
	for i, col := range tuple.Columns {
		if i >= len(rel.Columns) {
			break
		}
		relCol := rel.Columns[i]
		switch col.DataType {
		case 'n':
			row[relCol.Name] = nil
		case 'u':
			continue // TOAST não alterado, não veio no WAL — omite o campo
		case 't', 'b':
			row[relCol.Name] = r.decodeColumnValue(relCol.DataType, col.Data)
		}
	}
	return row
}

// decodeColumnValue usa o mesmo codec que o pgx usa internamente pra
// decodificar resultado de query, então o JSON publicado sai com
// number/bool/timestamp tipados de verdade, não tudo string.
func (r *replicator) decodeColumnValue(oid uint32, data []byte) any {
	typ, ok := r.typeMap.TypeForOID(oid)
	if !ok {
		return string(data)
	}
	value, err := typ.Codec.DecodeValue(r.typeMap, oid, pgtype.TextFormatCode, data)
	if err != nil {
		return string(data)
	}
	return value
}

func extractKeyFromRelation(rel *pglogrepl.RelationMessage, row map[string]any) map[string]any {
	key := make(map[string]any)
	for _, c := range rel.Columns {
		if c.Flags&1 != 0 { // bit 1 = coluna faz parte da replica identity (chave)
			key[c.Name] = row[c.Name]
		}
	}
	return key
}

// =============================================================================
// Monitor do slot — garante boa reciclagem de WAL, alertando se o lag crescer
// além do configurado (SLOT_LAG_WARN_BYTES).
// =============================================================================

func runSlotMonitor(ctx context.Context, pool *pgxpool.Pool, slotName string, warnBytes int64, interval time.Duration, log *zap.Logger) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			checkSlot(ctx, pool, slotName, warnBytes, log)
		}
	}
}

func checkSlot(ctx context.Context, pool *pgxpool.Pool, slotName string, warnBytes int64, log *zap.Logger) {
	var active bool
	var restartLSN, confirmedLSN *string
	var retainedBytes int64

	err := pool.QueryRow(ctx, `
		SELECT active, restart_lsn::text, confirmed_flush_lsn::text,
		       COALESCE(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn), 0)::bigint
		FROM pg_replication_slots WHERE slot_name = $1
	`, slotName).Scan(&active, &restartLSN, &confirmedLSN, &retainedBytes)
	if err != nil {
		if err == pgx.ErrNoRows {
			log.Error("replication slot não existe mais — foi dropado externamente?", zap.String("slot", slotName))
			metricSlotActive.Set(0)
			return
		}
		log.Warn("falha ao checar status do slot", zap.Error(err))
		return
	}

	metricSlotActive.Set(boolToFloat(active))
	metricReplicationLagBytes.Set(float64(retainedBytes))

	fields := []zap.Field{zap.Bool("active", active), zap.Int64("wal_retido_bytes", retainedBytes)}
	if restartLSN != nil {
		fields = append(fields, zap.String("restart_lsn", *restartLSN))
	}
	if confirmedLSN != nil {
		fields = append(fields, zap.String("confirmed_flush_lsn", *confirmedLSN))
	}
	if retainedBytes > warnBytes {
		log.Warn("slot com WAL retido acima do limite (SLOT_LAG_WARN_BYTES) — risco de disco crescendo no Postgres", fields...)
	} else {
		log.Debug("status do slot", fields...)
	}
}

func boolToFloat(b bool) float64 {
	if b {
		return 1
	}
	return 0
}

func runViewRefreshLoop(ctx context.Context, pool *pgxpool.Pool, producer *kafka.Writer, views []tableInfo, interval time.Duration, fetchSize int, log *zap.Logger) {
	if interval <= 0 || len(views) == 0 {
		return
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := refreshViews(ctx, pool, producer, views, fetchSize, log); err != nil {
				log.Error("falha ao atualizar snapshot das views", zap.Error(err))
			}
		}
	}
}

// =============================================================================
// Lifecycle exigido pela plataforma: initVars() + process().
// =============================================================================

var pipelineOnce sync.Once

// initVars é chamado uma vez pelo main.go gerado, antes do loop de consumo
// começar. Esse node não tem trabalho de negócio disparado por mensagem — a
// pipeline inteira (bootstrap do slot, snapshot inicial, streaming de CDC,
// monitor) é iniciada aqui em background e roda até o processo morrer.
func initVars(ctx context.Context, log *zap.Logger) error {
	applyContainerMemoryLimit(log)
	pipelineOnce.Do(func() {
		// Usa um context próprio, não o recebido aqui: o ctx de initVars pode
		// ter escopo só da inicialização, e a pipeline precisa sobreviver por
		// todo o tempo de vida do processo, não só até initVars retornar.
		go runPipelineForever(context.Background(), log)
	})
	return nil
}

// applyContainerMemoryLimit dá ao GC do Go um teto suave (runtime/debug.
// SetMemoryLimit) baseado no limite real do cgroup do pod. Sem isso, o
// runtime só reage ao crescimento do heap pela heurística padrão (GOGC=100),
// sem saber que existe um limite duro do Kubernetes logo ali — num pico de
// alocação (lote grande de snapshot rodando em paralelo, por exemplo) isso
// pode passar do MEM LIM do pod antes do GC ter motivo pra agir, resultando
// em OOMKilled mesmo com uso médio de memória baixo. Se GOMEMLIMIT já
// estiver setado explicitamente no ambiente, respeita e não mexe — o
// runtime do Go já lê essa env var sozinho, isso aqui é só o fallback pra
// quando ninguém configurou nada.
func applyContainerMemoryLimit(log *zap.Logger) {
	if _, set := os.LookupEnv("GOMEMLIMIT"); set {
		return
	}
	limit, err := readCgroupMemoryLimit()
	if err != nil {
		return
	}
	// 90%: deixa folga pra memória que o Go não conta no heap (stacks de
	// goroutine, buffers de rede/cgo) sem abrir mão da maior parte do teto.
	soft := int64(float64(limit) * 0.9)
	debug.SetMemoryLimit(soft)
	log.Info("GOMEMLIMIT aplicado automaticamente a partir do limite do cgroup",
		zap.Int64("cgroup_limit_bytes", limit), zap.Int64("soft_limit_bytes", soft))
}

// readCgroupMemoryLimit lê o limite de memória do container: cgroup v2
// primeiro (padrão em kernels/K8s recentes), com fallback pra v1. Retorna
// erro se não achar nenhum dos dois arquivos ou se o cgroup não tiver limite
// definido (rodando fora de container, por exemplo).
func readCgroupMemoryLimit() (int64, error) {
	if raw, err := os.ReadFile("/sys/fs/cgroup/memory.max"); err == nil {
		s := strings.TrimSpace(string(raw))
		if s == "max" {
			return 0, fmt.Errorf("cgroup v2 sem limite definido")
		}
		return strconv.ParseInt(s, 10, 64)
	}
	if raw, err := os.ReadFile("/sys/fs/cgroup/memory/memory.limit_in_bytes"); err == nil {
		v, err := strconv.ParseInt(strings.TrimSpace(string(raw)), 10, 64)
		if err != nil {
			return 0, err
		}
		// cgroup v1 sem limite reporta um valor gigante (perto do teto do
		// tipo) em vez de "max" como o v2 — filtra esse caso.
		if v > 1<<62 {
			return 0, fmt.Errorf("cgroup v1 sem limite definido")
		}
		return v, nil
	}
	return 0, fmt.Errorf("nenhum arquivo de limite de cgroup encontrado")
}

// runPipelineForever faz bootstrap + streaming, e se cair por erro (conexão
// derrubada, etc.) espera um backoff e tenta de novo — não tem "reprocessar
// mensagem" aqui como no fluxo normal de process(), então quem garante
// resiliência é esse loop.
func runPipelineForever(ctx context.Context, log *zap.Logger) {
	backoff := 5 * time.Second
	const maxBackoff = 2 * time.Minute
	for {
		if err := runPipelineOnce(ctx, log); err != nil {
			log.Error("pipeline de CDC caiu, tentando de novo com backoff", zap.Error(err), zap.Duration("backoff", backoff))
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
			continue
		}
		return // ctx cancelado, encerramento normal
	}
}

func runPipelineOnce(ctx context.Context, log *zap.Logger) error {
	cfg, err := loadSourceConfig(log)
	if err != nil {
		return err
	}

	pgConnString := buildPGConnString(cfg)
	poolConfig, err := pgxpool.ParseConfig(pgConnString)
	if err != nil {
		return err
	}
	// Precisa comportar SnapshotWorkers conexões simultâneas (uma por
	// partição em paralelo) + monitor de slot + refresh de views, com folga.
	poolConfig.MaxConns = cfg.PGPoolMaxConns
	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return err
	}
	defer pool.Close()

	tables, err := inspectTables(ctx, pool, cfg.SourceTables)
	if err != nil {
		return err
	}
	var views []tableInfo
	for _, t := range tables {
		if !t.cdcCapable() {
			views = append(views, t)
		}
	}
	log.Info("objetos de origem resolvidos", zap.Int("tabelas_cdc", len(tables)-len(views)), zap.Int("views_snapshot_only", len(views)))

	if err := ensurePublication(ctx, pool, cfg.PublicationName, tables, log); err != nil {
		return err
	}

	stateWriter := &kafka.Writer{Addr: kafka.TCP(cfg.KafkaBrokers...), Topic: cfg.StateTopic, Balancer: singlePartitionBalancer{}, RequiredAcks: kafka.RequireAll}
	defer stateWriter.Close() //nolint:errcheck

	st, err := loadState(ctx, cfg.KafkaBrokers, cfg.StateTopic, log)
	if err != nil {
		return err
	}

	producer := &kafka.Writer{
		Addr:         kafka.TCP(cfg.KafkaBrokers...),
		Topic:        cfg.OutputTopic,
		Balancer:     &kafka.Hash{}, // mesma key sempre na mesma partição -> ordem preservada por linha
		RequiredAcks: kafka.RequireAll,
		BatchSize:    cfg.KafkaBatchSize,
		BatchBytes:   cfg.KafkaBatchBytes,
		BatchTimeout: 100 * time.Millisecond,
		Compression:  resolveKafkaCompression(cfg.KafkaCompression, log),
	}
	defer producer.Close() //nolint:errcheck

	replConnString, err := replicationConnString(pgConnString)
	if err != nil {
		return err
	}
	replConn, err := pgconn.Connect(ctx, replConnString)
	if err != nil {
		return err
	}
	defer replConn.Close(ctx)

	bootstrap, err := ensureReplicationSlot(ctx, pool, replConn, cfg.SlotName)
	if err != nil {
		return err
	}

	startLSN, err := resolveStartLSN(ctx, pool, cfg.SlotName, st, bootstrap)
	if err != nil {
		return err
	}

	if bootstrap.Created {
		log.Info("slot recém-criado, rodando snapshot inicial no ponto consistente", zap.String("consistent_point", bootstrap.ConsistentPoint.String()))
		if err := runInitialSnapshot(ctx, pool, producer, stateWriter, cfg.SnapshotFetchSize, cfg.SnapshotWorkers, tables, bootstrap.SnapshotName, st, log); err != nil {
			return err
		}
	} else if !allTablesSnapshotted(tables, st) {
		log.Warn("slot já existia mas nem toda tabela tem snapshot concluído no checkpoint — rodando o que falta sem ponto consistente exportado (cenário de recuperação)")
		if err := runInitialSnapshot(ctx, pool, producer, stateWriter, cfg.SnapshotFetchSize, cfg.SnapshotWorkers, tables, "", st, log); err != nil {
			return err
		}
	}

	go runSlotMonitor(ctx, pool, cfg.SlotName, cfg.SlotLagWarnBytes, cfg.SlotMonitorInterval, log)
	go runViewRefreshLoop(ctx, pool, producer, views, cfg.ViewRefreshInterval, cfg.SnapshotFetchSize, log)

	rep := newReplicator(cfg.SlotName, cfg.PublicationName, producer, stateWriter, cfg.StatusUpdateInterval, log)
	err = rep.run(ctx, replConn, st, startLSN)
	if ctx.Err() != nil {
		return nil
	}
	return err
}

func resolveStartLSN(ctx context.Context, pool *pgxpool.Pool, slotName string, st *pipelineState, bootstrap *slotBootstrap) (pglogrepl.LSN, error) {
	if st.ConfirmedLSN != "" {
		return pglogrepl.ParseLSN(st.ConfirmedLSN)
	}
	if bootstrap.Created {
		return bootstrap.ConsistentPoint, nil
	}
	var confirmed string
	if err := pool.QueryRow(ctx, `SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = $1`, slotName).Scan(&confirmed); err != nil {
		return 0, err
	}
	return pglogrepl.ParseLSN(confirmed)
}

func allTablesSnapshotted(tables []tableInfo, st *pipelineState) bool {
	for _, t := range tables {
		if !st.Snapshots[t.qualified()].Done {
			return false
		}
	}
	return true
}

// process satisfaz o contrato exigido pelo main.go gerado, mas não tem
// trabalho de negócio real pra fazer: esse node não consome nada de
// INPUT_TOPIC, a pipeline inteira já roda em background desde initVars().
func process(topicName, value, key string, headers map[string]string, log *zap.Logger) ([]ProcessResult, error) {
	return nil, nil
}
