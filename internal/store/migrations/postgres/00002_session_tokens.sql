-- +goose Up
CREATE TABLE session_tokens (
    token_hash BYTEA  PRIMARY KEY,
    session_id BIGINT NOT NULL
);

-- +goose Down
DROP TABLE session_tokens;
