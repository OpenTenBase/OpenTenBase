\set query_id random(1, :query_count)
SELECT id
FROM vector_bench.items
ORDER BY embedding <=> (
	SELECT embedding FROM vector_bench.queries WHERE id = :query_id
)
LIMIT :k;
