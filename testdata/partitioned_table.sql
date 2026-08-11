-- Cenário de teste: tabela particionada por RANGE (data), o padrão mais comum
-- (particionamento por mês). PK precisa incluir a coluna de partição — é
-- exigência do Postgres pra tabela particionada, não invenção do node.

CREATE TABLE pedidos_particionado (
    id         serial,
    cliente    text,
    valor      numeric,
    criado_em  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (id, criado_em)
) PARTITION BY RANGE (criado_em);

CREATE TABLE pedidos_particionado_2026_01 PARTITION OF pedidos_particionado
    FOR VALUES FROM ('2026-01-01') TO ('2026-02-01');

CREATE TABLE pedidos_particionado_2026_02 PARTITION OF pedidos_particionado
    FOR VALUES FROM ('2026-02-01') TO ('2026-03-01');

-- Pega tudo que não cair nas faixas acima (ex: linhas com criado_em fora do
-- range mapeado) — sem isso, um INSERT fora das faixas dá erro.
CREATE TABLE pedidos_particionado_default PARTITION OF pedidos_particionado DEFAULT;

-- Linhas em partições diferentes, pra confirmar que o snapshot e o CDC pegam
-- as duas (e que tudo sai no Kafka com o nome da tabela-raiz, não da partição).
INSERT INTO pedidos_particionado (cliente, valor, criado_em) VALUES
    ('Ana',   100.50, '2026-01-15 10:00:00-03'),
    ('Beto',   42.00, '2026-01-20 14:30:00-03'),
    ('Carla', 250.00, '2026-02-05 09:15:00-03'),
    ('Davi',   17.90, '2026-02-28 23:59:00-03');

-- Confirma que cada linha caiu na partição esperada:
-- SELECT tableoid::regclass AS partição, * FROM pedidos_particionado ORDER BY criado_em;
