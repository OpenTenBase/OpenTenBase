# Minimal example: `opentenbase_ai` + `pgvector` (embedding + similarity search)

This walkthrough shows the smallest end-to-end setup that combines the AI
functions of this extension with the bundled `contrib/pgvector` extension
(`vector`): register an OpenAI-compatible embedding model, store embeddings
in a table, and run a nearest-neighbour query.

All signatures follow `opentenbase_ai--1.0.sql`. Steps that were not executed
against a live model endpoint are marked **[expected result]**.


## 1. Prerequisites

- An OpenAI-compatible embeddings endpoint (URL + API token).
- Both dependencies ship in this repository:
  - `http` (`contrib/pgsql-http`) — required by this extension
    (`opentenbase_ai.control` declares `requires = 'http'`);
  - `vector` (`contrib/pgvector`).

```sql
CREATE EXTENSION http;
CREATE EXTENSION opentenbase_ai;
CREATE EXTENSION vector;
```

## 2. Register an embedding model

```sql
SELECT ai.add_embedding_model(
    'text-embedding-3-small',
    'https://api.openai.com/v1/embeddings',
    '{"model": "text-embedding-3-small"}'::jsonb,
    token => 'sk-...'
);
```

Signature (from `opentenbase_ai--1.0.sql`):

```sql
ai.add_embedding_model(model_name text, uri text, default_args jsonb,
                       token text = NULL, model_provider text = NULL)
RETURNS boolean
```

The token is sent as an `Authorization: Bearer <token>` header. Confirm the
registration (see the `ai.models` view):

```sql
SELECT model_name, uri FROM ai.models;
-- [expected result] one row for the model just added
```

## 3. Generate an embedding

`ai.embedding()` **returns text** — the JSON array extracted from the HTTP
response body — so cast it into a `vector`:

```sql
SET ai.embedding_model = 'text-embedding-3-small';
SELECT ai.embedding('hello world')::vector;
-- [expected result] e.g. [0.010031,-0.013487,...] (1536 dims for
-- text-embedding-3-small)
```

Two ways to hit the same model:

```sql
-- explicit model name (no session setting needed)
SELECT ai.embedding('hello world', 'text-embedding-3-small')::vector;
```

If neither is given, the function raises `Embedding model name is not set`
(the `ai.embedding` body uses `current_setting('ai.embedding_model', true)`).

## 4. Store embeddings and search

```sql
CREATE TABLE docs (
    id        serial PRIMARY KEY,
    body      text,
    embedding vector(1536)
);

INSERT INTO docs (body, embedding)
VALUES ('hello world', ai.embedding('hello world')::vector);

-- cosine-distance nearest neighbour
SELECT id, body,
       embedding <=> ai.embedding('greetings')::vector AS distance
FROM docs
ORDER BY embedding <=> ai.embedding('greetings')::vector
LIMIT 3;
-- [expected result] the row just inserted ranked first
```

`<=>` (cosine distance) comes from `contrib/pgvector`; see that extension's
documentation for `<->` (L2) and `<#>` (negative inner product).

## 5. Notes and limits

- Each `ai.embedding()` call is one synchronous HTTP `POST`; this example
  does no batching or caching.
- Errors from the HTTP call are surfaced by `ai.invoke_model` rather than
  silently swallowed; a `NULL` input returns `NULL`.
- Only endpoints that accept `{"input": ...}` and answer with an
  OpenAI-style `{"data": [{"embedding": [...]}]}` body work with
  `ai.add_embedding_model` as-is. Other response shapes can be handled with
  `ai.add_model` and a custom `json_path` extraction expression
  (`ai.add_embedding_model` fixes it to `data->0->embedding`).
- The `::vector` cast works because pgvector's text input format is exactly
  the bracketed array `ai.embedding()` returns; no intermediate parsing is
  needed.
