-- +goose Up
CREATE TABLE events (
    seq  INTEGER PRIMARY KEY,
    kind TEXT    NOT NULL,
    at   TEXT    NOT NULL,
    data TEXT    NOT NULL
);

-- +goose Down
DROP TABLE events;
