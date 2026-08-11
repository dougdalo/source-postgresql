// main.go — NÃO faz parte do que sobe pra plataforma. Lá, esse arquivo é
// gerado automaticamente e eu não mexo nele; aqui é só um stand-in fiel o
// suficiente ao que a documentação descreve (chama initVars() antes de
// qualquer coisa, expõe /healthz e /metrics na porta 9999, declara
// ProcessResult e sendDlq) pra dar pra compilar e testar process.go
// localmente com `go run .`.
//
// Se o main.go real da plataforma tiver uma assinatura diferente pra
// initVars()/process()/ProcessResult, ajuste process.go de acordo — o que
// importa de verdade é o process.go, esse arquivo é só andaime local.
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	kafka "github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

// loadDotEnv só existe nesse stand-in local — na plataforma de verdade as env
// vars já vêm setadas no ambiente do container, não tem .env. Não sobrescreve
// nada que já esteja setado de verdade no ambiente (env real sempre ganha).
func loadDotEnv(path string) {
	f, err := os.Open(path)
	if err != nil {
		return // sem .env, segue só com o que já tiver no ambiente
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, value, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		key = strings.TrimSpace(key)
		value = strings.Trim(strings.TrimSpace(value), `"'`)
		if _, alreadySet := os.LookupEnv(key); !alreadySet {
			os.Setenv(key, value)
		}
	}
}

type ProcessResult struct {
	Value   string
	Key     string
	Headers map[string]string
}

func sendDlq(value, key string, headers map[string]string) error {
	dlqTopic := os.Getenv("OUTPUT_TOPIC") + "-dlq"
	writer := &kafka.Writer{Addr: kafka.TCP(splitCSV(os.Getenv("KAFKA_BROKERS"))...), Topic: dlqTopic}
	defer writer.Close() //nolint:errcheck

	kh := make([]kafka.Header, 0, len(headers))
	for k, v := range headers {
		kh = append(kh, kafka.Header{Key: k, Value: []byte(v)})
	}
	return writer.WriteMessages(context.Background(), kafka.Message{Key: []byte(key), Value: []byte(value), Headers: kh})
}

func main() {
	loadDotEnv(".env")

	log, err := zap.NewProduction()
	if err != nil {
		panic(err)
	}
	defer log.Sync() //nolint:errcheck

	initVars()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	})
	mux.Handle("/metrics", promhttp.Handler())

	port := os.Getenv("HTTP_PORT")
	if port == "" {
		port = "9999"
	}
	srv := &http.Server{Addr: ":" + port, Handler: mux}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()

	log.Info("http server no ar", zap.String("port", port))
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatal("http server caiu", zap.Error(err))
	}
}
