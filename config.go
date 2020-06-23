package beelog

import "errors"

// ReduceInterval ...
type ReduceInterval int8

const (
	// Immediately ...
	Immediately ReduceInterval = iota

	// Delayed ...
	Delayed

	// Interval ...
	Interval
)

// LogConfig ...
type LogConfig struct {
	alg   Reducer
	tick  ReduceInterval
	inmem bool
	fname string
}

// DefaultLogConfig ...
func DefaultLogConfig() *LogConfig {
	return &LogConfig{
		alg:   IterDFSAvl,
		tick:  Delayed,
		inmem: true,
	}
}

// ValidateConfig ...
func (lc *LogConfig) ValidateConfig() error {
	if (lc.alg != GreedyAvl) && (lc.alg != IterBFSAvl) && (lc.alg != IterDFSAvl) {
		return errors.New("invalid config: unknow reduce algorithm provided")
	}
	if (lc.tick != Immediately) && (lc.tick != Delayed) && (lc.tick != Interval) {
		return errors.New("invalid config: unknow reduce interval provided")
	}
	if !lc.inmem && lc.fname == "" {
		return errors.New("invalid config: if persistent storage (i.e. inmem == false), config.fname must be provided")
	}
	return nil
}
