package store

import (
	"context"
	"crypto/sha256"
	"fmt"

	"github.com/doug-martin/goqu/v9"

	"github.com/avikmukherjee/governor/internal/core"
)

// tokensTable holds one row per session token, keyed by the token's hash.
const tokensTable = "session_tokens"

// tokenRow is one row of tokensTable.
type tokenRow struct {
	Hash    []byte `db:"token_hash"`
	Session uint64 `db:"session_id"`
}

// SaveToken records that the token with this hash names the session.
func (s *Store) SaveToken(ctx context.Context, hash [sha256.Size]byte, sid core.SessionID) error {
	insert := s.db.Insert(tokensTable).Prepared(true).Rows(tokenRow{Hash: hash[:], Session: uint64(sid)})
	if _, err := insert.Executor().ExecContext(ctx); err != nil {
		return fmt.Errorf("store: save token: %w", err)
	}
	return nil
}

// DeleteToken forgets a token; deleting an unknown token is not an error.
func (s *Store) DeleteToken(ctx context.Context, hash [sha256.Size]byte) error {
	del := s.db.Delete(tokensTable).Prepared(true).Where(goqu.C("token_hash").Eq(hash[:]))
	if _, err := del.Executor().ExecContext(ctx); err != nil {
		return fmt.Errorf("store: delete token: %w", err)
	}
	return nil
}

// LoadTokens returns every recorded token hash and the session it names.
func (s *Store) LoadTokens(ctx context.Context) (map[[sha256.Size]byte]core.SessionID, error) {
	var rows []tokenRow
	if err := s.db.From(tokensTable).ScanStructsContext(ctx, &rows); err != nil {
		return nil, fmt.Errorf("store: load tokens: %w", err)
	}
	tokens := make(map[[sha256.Size]byte]core.SessionID, len(rows))
	for _, row := range rows {
		if len(row.Hash) != sha256.Size {
			return nil, fmt.Errorf("store: load tokens: hash of %d bytes", len(row.Hash))
		}
		tokens[[sha256.Size]byte(row.Hash)] = core.SessionID(row.Session)
	}
	return tokens, nil
}
