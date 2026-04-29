-- Run once in Supabase SQL editor
CREATE TABLE trades (
    id          BIGSERIAL PRIMARY KEY,
    symbol      TEXT      NOT NULL,
    qty         FLOAT     NOT NULL,
    price       FLOAT,
    ts          TIMESTAMPTZ NOT NULL,
    inserted_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE trades_analytics (
    id            BIGSERIAL PRIMARY KEY,
    symbol        TEXT        NOT NULL,
    minute_bucket TIMESTAMPTZ NOT NULL,
    volume        FLOAT       NOT NULL,
    inserted_at   TIMESTAMPTZ DEFAULT now(),
    UNIQUE (symbol, minute_bucket)   -- prevent duplicate windows
);