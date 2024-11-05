-- +goose Up
-- +goose StatementBegin
CREATE TABLE simple (
    id int NOT NULL AUTO_INCREMENT,
    title text,
    body text,
    PRIMARY KEY(id)
);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE simple;
-- +goose StatementEnd
