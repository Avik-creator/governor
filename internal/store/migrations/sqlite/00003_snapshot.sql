-- +goose Up
CREATE TABLE snapshot (
    singleton INTEGER PRIMARY KEY DEFAULT 1 CHECK (singleton = 1),
    seq       INTEGER NOT NULL,
    data      TEXT    NOT NULL
);

-- +goose Down
DROP TABLE snapshot;
