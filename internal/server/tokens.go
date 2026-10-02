package server

import (
	"context"
	"crypto/sha256"
	"fmt"
	"log/slog"
	"time"

	"github.com/Avik-creator/governor/internal/core"
)

// tokenTimeout bounds the removal of a token after its session has ended.
const tokenTimeout = 5 * time.Second

// TokenStore keeps session token hashes, so sessions stay usable after a restart.
type TokenStore interface {
	// SaveToken records that the token with this hash names the session.
	SaveToken(ctx context.Context, hash [sha256.Size]byte, sid core.SessionID) error

	// DeleteToken forgets a token; deleting an unknown token is not an error.
	DeleteToken(ctx context.Context, hash [sha256.Size]byte) error

	// LoadTokens returns every recorded token hash and the session it names.
	LoadTokens(ctx context.Context) (map[[sha256.Size]byte]core.SessionID, error)
}

// NopTokenStore keeps nothing, so every session token dies with the process.
type NopTokenStore struct{}

// SaveToken does nothing.
func (NopTokenStore) SaveToken(context.Context, [sha256.Size]byte, core.SessionID) error {
	return nil
}

// DeleteToken does nothing.
func (NopTokenStore) DeleteToken(context.Context, [sha256.Size]byte) error {
	return nil
}

// LoadTokens returns no tokens.
func (NopTokenStore) LoadTokens(context.Context) (map[[sha256.Size]byte]core.SessionID, error) {
	return nil, nil
}

// adopt takes over the sessions of a restored engine and closes the unreachable ones.
func (s *Server) adopt(ctx context.Context) error {
	stored, err := s.tokens.LoadTokens(ctx)
	if err != nil {
		return fmt.Errorf("server: load tokens: %w", err)
	}
	reachable := make(map[core.SessionID]bool, len(stored))
	for key, sid := range stored {
		reachable[sid] = true
		// A token whose session is gone is dropped again as soon as it is issued.
		s.issue(key, sid)
	}
	for _, sid := range s.engine.Sessions() {
		if reachable[sid] {
			continue
		}
		// Nobody holds a token for this session, so it could never be used or closed.
		if _, err := s.engine.CloseSession(sid); err != nil {
			return fmt.Errorf("server: close session %d: %w", sid, err)
		}
	}
	return nil
}

// stand opens one session without a TTL per API key, for the calls a key may make directly.
func (s *Server) stand(ctx context.Context) error {
	var last uint64
	for key, scope := range s.keys {
		sid, _, seq, err := s.engine.OpenSession(scope, 0)
		if err != nil {
			return fmt.Errorf("server: open standing session on node %d: %w", scope, err)
		}
		s.standing[key], last = sid, seq
	}
	// These sessions hold no token, so the next start closes them and opens new ones.
	return s.durable(ctx, last)
}

// issue maps a token hash to its session until the session ends.
func (s *Server) issue(key digest, sid core.SessionID) {
	ended := s.engine.SessionDone(sid)
	s.mu.Lock()
	s.sessions[key] = sid
	s.mu.Unlock()
	s.wg.Go(func() {
		select {
		case <-ended:
		case <-s.closed:
			return
		}
		s.mu.Lock()
		delete(s.sessions, key)
		s.mu.Unlock()
		s.seen.forget(sid)
		ctx, cancel := context.WithTimeout(context.Background(), tokenTimeout)
		defer cancel()
		// A token left behind is harmless: adopt drops it at the next start.
		if err := s.tokens.DeleteToken(ctx, key); err != nil {
			slog.ErrorContext(ctx, "governor: delete token", "session", sid, "err", err)
		}
	})
}
