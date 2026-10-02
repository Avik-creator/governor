-- +goose Up
CREATE TABLE events (
    seq  BIGINT      PRIMARY KEY,
    kind TEXT        NOT NULL,
    at   TIMESTAMPTZ NOT NULL,
    data JSONB       NOT NULL
);

-- +goose Down
DROP TABLE events;
