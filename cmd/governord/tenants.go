package main

import (
	"fmt"

	"github.com/avikmukherjee/governor/internal/config"
	"github.com/avikmukherjee/governor/internal/core"
)

// reconcile makes the tree match the configuration and returns each API key's scope.
func reconcile(engine *core.Engine, cfg *config.Config) (map[string]core.NodeID, uint64, error) {
	sid, _, _, err := engine.OpenSession(core.RootID, 0)
	if err != nil {
		return nil, 0, fmt.Errorf("open bootstrap session: %w", err)
	}
	keys, err := applyConfig(engine, sid, cfg)
	// The bootstrap session is closed even on failure, so it never outlives the start.
	seq, closeErr := engine.CloseSession(sid)
	if err == nil {
		err = closeErr
	}
	return keys, seq, err
}

// applyConfig creates missing tenants and applies the configured limits as sid.
func applyConfig(engine *core.Engine, sid core.SessionID, cfg *config.Config) (map[string]core.NodeID, error) {
	if err := setLimits(engine, core.RootID, cfg.Root.Limits); err != nil {
		return nil, fmt.Errorf("root: %w", err)
	}
	children, err := engine.Children(sid, core.RootID)
	if err != nil {
		return nil, fmt.Errorf("list tenants: %w", err)
	}
	// A tenant that has ended is not reused; the tenant gets a fresh node.
	existing := make(map[string]core.NodeID, len(children))
	for _, c := range children {
		if c.State == core.StateActive {
			existing[c.Name] = c.ID
		}
	}

	keys := make(map[string]core.NodeID, len(cfg.Tenants)+1)
	if cfg.AdminKey != "" {
		keys[cfg.AdminKey] = core.RootID
	}
	for _, t := range cfg.Tenants {
		id, ok := existing[t.Name]
		if ok {
			err = setLimits(engine, id, t.Limits)
		} else {
			id, _, err = engine.CreateNode(sid, core.RootID, t.Spec())
		}
		if err != nil {
			return nil, fmt.Errorf("tenant %q: %w", t.Name, err)
		}
		keys[t.APIKey] = id
	}
	return keys, nil
}

// setLimits applies configured limits to a node; unchanged limits record nothing.
func setLimits(engine *core.Engine, id core.NodeID, limits map[core.Class]int) error {
	for class, limit := range limits {
		if _, err := engine.SetLimit(id, class, limit); err != nil {
			return fmt.Errorf("limit %q: %w", class, err)
		}
	}
	return nil
}
