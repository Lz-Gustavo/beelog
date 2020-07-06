package beelog

import "errors"

// ReduceInterval ...
type ReduceInterval int8

const (
	// Immediately log reduce takes place after each insertion on the log
	// structure. Not every 'Log()' call triggers an reduce, only those that
	// result in a state change (e.g. write operations).
	Immediately ReduceInterval = iota

	// Delayed log reduce executes the configured reduce algorithm only during
	// recovery, when a 'Recov()' call is invoked. This approach provides minimal
	// overhead during application's execution, but can incur in a longer recovery
	// on catastrophic fault scenarios (i.e. when all replicas fail together).
	Delayed

	// Interval log reduce acts similar to a checkpoint procedure, triggering
	// a reduce event after 'Period' commands. If a log interval is requested
	// (i.e. through 'Recov()' calls), the last reduced state is informed. If
	// no prior state is found (i.e. didnt reach 'Period' commands yet), a new
	// one is immediately executed.
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
	if (lc.Alg != GreedyLt) && (lc.Alg != GreedyArray) && (lc.Alg != GreedyAvl) && (lc.Alg != IterBFSAvl) && (lc.Alg != IterDFSAvl) {
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
