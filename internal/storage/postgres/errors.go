package postgres

import (
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/boundflow/boundflow/internal/storage"
)

func handleError(err error, entity string) error {
	if err == nil {
		return nil
	}

	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "23505" {
		return storage.ErrAlreadyExists
	}

	if errors.Is(err, pgx.ErrNoRows) {
		return storage.ErrNotFound
	}

	return err
}
