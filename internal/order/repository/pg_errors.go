package repository

import (
	"errors"

	"github.com/jackc/pgx/v5/pgconn"
)

// ErrDuplicateClientOrder 表示 client_order_id 幂等键冲突。
var ErrDuplicateClientOrder = errors.New("duplicate client_order_id")

func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}
