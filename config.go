package beelog

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
