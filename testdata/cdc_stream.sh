#!/usr/bin/env bash
# Gera insert/update/delete continuamente no Postgres pra testar o caminho de
# streaming de CDC (não o snapshot). Rode isso NO HOST do docker compose
# (dougdalo@docker:~/kafka), com o node (go run .) já rodando e o snapshot
# inicial já concluído.
#
# Uso: ./cdc_stream.sh [intervalo_em_segundos]   (default: 3)
set -euo pipefail

INTERVAL="${1:-3}"
PSQL="docker compose exec -T postgres-test psql -U postgres -d testdb -v ON_ERROR_STOP=1 -q"

echo "Gerando eventos de CDC a cada ${INTERVAL}s em pedidos_particionado (Ctrl+C pra parar)..."

i=0
while true; do
    i=$((i + 1))
    cliente="Stream $i"
    valor=$(awk 'BEGIN{srand(); printf "%.2f", rand()*300}')

    $PSQL -c "INSERT INTO pedidos_particionado (cliente, valor, criado_em) VALUES ('${cliente}', ${valor}, now());"
    echo "[$i] insert -> ${cliente} = ${valor}"

    # a cada 4 inserts, atualiza um registro de alguns ciclos atrás
    if (( i % 4 == 0 && i - 2 > 0 )); then
        alvo="Stream $((i - 2))"
        $PSQL -c "UPDATE pedidos_particionado SET valor = valor + 10 WHERE cliente = '${alvo}';"
        echo "[$i] update -> ${alvo}"
    fi

    # a cada 6 inserts, apaga um registro mais antigo ainda (testa tombstone)
    if (( i % 6 == 0 && i - 4 > 0 )); then
        alvo="Stream $((i - 4))"
        $PSQL -c "DELETE FROM pedidos_particionado WHERE cliente = '${alvo}';"
        echo "[$i] delete -> ${alvo}"
    fi

    sleep "$INTERVAL"
done
