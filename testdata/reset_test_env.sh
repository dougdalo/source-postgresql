#!/usr/bin/env bash
# Zera slot, publication e tópicos pra um teste limpo (contagem tabela == tópico).
# Rode NO HOST do docker compose (dougdalo@docker:~/kafka), com o `go run .`
# PARADO (Ctrl+C) antes de rodar isto.
set -euo pipefail

SLOT_NAME="${1:-pgsource_teste_local}"
OUTPUT_TOPIC="${2:-pg.homelab.teste}"
STATE_TOPIC="${3:-${OUTPUT_TOPIC}-state}"
BROKER="${4:-kafka:9092}"

PSQL="docker compose exec -T postgres-test psql -U postgres -d testdb -v ON_ERROR_STOP=1 -q"

echo "==> Derrubando replication slot ${SLOT_NAME} (se existir)"
$PSQL -c "SELECT pg_drop_replication_slot('${SLOT_NAME}') WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = '${SLOT_NAME}');"

echo "==> Derrubando publication (mesmo nome do slot, no seu .env)"
$PSQL -c "DROP PUBLICATION IF EXISTS ${SLOT_NAME};"

echo "==> Apagando e recriando os tópicos Kafka"
docker compose exec -T kafka /kafka/bin/kafka-topics.sh --delete --topic "${OUTPUT_TOPIC}" --bootstrap-server "${BROKER}" || true
docker compose exec -T kafka /kafka/bin/kafka-topics.sh --delete --topic "${STATE_TOPIC}" --bootstrap-server "${BROKER}" || true
sleep 2
docker compose exec -T kafka /kafka/bin/kafka-topics.sh --create --topic "${OUTPUT_TOPIC}" --bootstrap-server "${BROKER}" --partitions 3 --replication-factor 1
docker compose exec -T kafka /kafka/bin/kafka-topics.sh --create --topic "${STATE_TOPIC}" --bootstrap-server "${BROKER}" --partitions 1 --replication-factor 1 --config cleanup.policy=compact

echo "==> Truncando a tabela de teste (opcional — comente se quiser manter os dados)"
$PSQL -c "TRUNCATE TABLE pedidos_particionado;"

echo "Pronto. Roda 'go run .' de novo pra um snapshot limpo."
