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
	Alg    Reducer
	Tick   ReduceInterval
	Inmem  bool
	Fname  string
	Period uint32
}

// DefaultLogConfig ...
func DefaultLogConfig() *LogConfig {
	return &LogConfig{
		Alg:   IterDFSAvl,
		Tick:  Delayed,
		Inmem: true,
	}
}

// ValidateConfig ...
func (lc *LogConfig) ValidateConfig() error {
	if (lc.Alg != GreedyAvl) && (lc.Alg != IterBFSAvl) && (lc.Alg != IterDFSAvl) {
		return errors.New("invalid config: unknow reduce algorithm provided")
	}
	if (lc.Tick != Immediately) && (lc.Tick != Delayed) && (lc.Tick != Interval) {
		return errors.New("invalid config: unknow reduce interval provided")
	}
	if !lc.Inmem && lc.Fname == "" {
		return errors.New("invalid config: if persistent storage (i.e. Inmem == false), config.Fname must be provided")
	}
	if lc.Tick == Interval && lc.Period == 0 {
		return errors.New("invalid config: if periodic reduce is set (i.e. Tick == Interval), a config.Period must be provided")
	}
	return nil
}
