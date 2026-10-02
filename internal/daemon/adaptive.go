package daemon

import (
	"github.com/Avik-creator/governor/internal/adaptive"
	"github.com/Avik-creator/governor/internal/config"
	"github.com/Avik-creator/governor/internal/core"
)

// controllers builds one controller per adaptive class, keyed by that class.
func controllers(cfg *config.Config) (map[core.Class]*adaptive.Controller, error) {
	byClass := make(map[core.Class]*adaptive.Controller, len(cfg.Adaptive))
	for _, a := range cfg.Adaptive {
		c, err := adaptive.New(adaptive.Config{
			Node:        core.RootID,
			Class:       a.Class,
			Initial:     cfg.Root.Limits[a.Class],
			MinLimit:    a.MinLimit,
			MaxLimit:    a.MaxLimit,
			Interval:    a.Interval,
			TargetP95:   a.TargetP95,
			MaxOverload: a.MaxOverload,
			MinSamples:  a.MinSamples,
		})
		if err != nil {
			return nil, err
		}
		byClass[a.Class] = c
	}
	return byClass, nil
}
