-- +goose Up
CREATE TABLE snapshot (
    singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
    seq       BIGINT  NOT NULL,
    data      JSONB   NOT NULL
);

-- +goose Down
DROP TABLE snapshot;
