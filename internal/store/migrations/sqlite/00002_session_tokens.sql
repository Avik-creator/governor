-- +goose Up
CREATE TABLE session_tokens (
    token_hash BLOB    PRIMARY KEY,
    session_id INTEGER NOT NULL
);

-- +goose Down
DROP TABLE session_tokens;
