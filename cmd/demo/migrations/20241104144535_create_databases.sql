-- +goose NO TRANSACTION

-- +goose Up
CREATE DATABASE IF NOT EXISTS sink;

-- +goose Down
DROP DATABASE sink;
DROP DATABASE source;
