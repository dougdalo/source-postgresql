-- Carga em massa pra testar a PAGINAÇÃO DO SNAPSHOT (SNAPSHOT_FETCH_SIZE
-- default é 5000 — com 12000 linhas o snapshot precisa de 3 batches, então
-- dá pra ver o checkpoint de progresso sendo salvo no meio do processo, não
-- só no fim). As datas caem espalhadas entre as duas partições existentes
-- (2026-01 e 2026-02).
INSERT INTO pedidos_particionado (cliente, valor, criado_em)
SELECT
    'Cliente ' || i,
    round((random() * 500)::numeric, 2),
    '2026-01-01'::timestamptz + (random() * interval '58 days')
FROM generate_series(1, 12000) AS s(i);

-- Confere quantas linhas foram pra cada partição:
-- SELECT tableoid::regclass AS particao, count(*) FROM pedidos_particionado GROUP BY 1;
