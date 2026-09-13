\set query_id random(1, :query_count)

SELECT item.id
FROM vector_bench.items AS item
ORDER BY item.embedding <=> (
	SELECT query_vector.embedding
	FROM vector_bench.queries AS query_vector
	WHERE query_vector.id = :query_id
)
LIMIT :top_k;
