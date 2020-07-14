package beelog

import (
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/Lz-Gustavo/beelog/pb"
)

const (
	defaultCap = 2000
)

// buffEntry (ies) are equivalent to list entries without any "shortcut" ptr
// to the stateTable.
type buffEntry struct {
	ind uint64
	key string
}

// minStateTable is a minimal format of the ordinary stateTable, storing only
// the lates state for each key.
type minStateTable map[string]State

// CircBuffHT ...
type CircBuffHT struct {
	buff *[]buffEntry
	aux  *minStateTable
	mu   sync.Mutex

	cur, cap, len int
	logData
}

// NewCircBuffHT ...
func NewCircBuffHT() *CircBuffHT {
	ht := make(minStateTable, 0)
	sl := make([]buffEntry, 0, defaultCap)
	return &CircBuffHT{
		logData: logData{config: DefaultLogConfig()},
		buff:    &sl,
		aux:     &ht,
		cap:     defaultCap,
	}
}

// NewCircBuffHTWithConfig ...
func NewCircBuffHTWithConfig(cfg *LogConfig, cap int) (*CircBuffHT, error) {
	err := cfg.ValidateConfig()
	if err != nil {
		return nil, err
	}

	ht := make(minStateTable, 0)
	sl := make([]buffEntry, 0, cap)
	return &CircBuffHT{
		logData: logData{config: cfg},
		buff:    &sl,
		aux:     &ht,
		cap:     cap,
	}, nil
}

// Str returns a string representation of the list state, used for debug purposes.
func (cb *CircBuffHT) Str() string {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	var strs []string
	i := 0
	for i < cb.len {
		// negative values already account circular reference
		pos := (cb.cur - cb.len + i) % cb.cap
		v := (*cb.buff)[pos]
		strs = append(strs, fmt.Sprintf("%v->", v))
		i++
	}
	return strings.Join(strs, " ")
}

// Len returns the list length.
func (cb *CircBuffHT) Len() uint64 {
	return uint64(cb.len)
}

// Log records the occurence of command 'cmd' on the provided index. Writes are as
// a new node on the underlying liked list,  with a pointer to the newly inserted
// state update on the update list for its particular key..
func (cb *CircBuffHT) Log(index uint64, cmd pb.Command) error {

	cb.mu.Lock()
	var wrt bool
	if cmd.Op != pb.Command_SET {
		// TODO: treat 'ar.first' attribution on GETs
		cb.last = index

	} else {
		wrt = true

		// TODO: Ensure same index for now. Log API will change in time
		cmd.Id = index

		entry := buffEntry{
			ind: index,
			key: cmd.Key,
		}

		// update current state for that particular key
		st := (*cb.aux)[cmd.Key]
		st.ind = index
		st.cmd = cmd

		// adjust first structure index
		if cb.Len() == 0 {
			cb.first = entry.ind
		}

		// insert new entry
		(*cb.buff)[cb.cur] = entry

		// update insert cursor
		cb.cur = (cb.cur + 1) % cb.cap

		// adjust last index and len once inserted
		cb.last = index
		cb.len++
	}

	// Pessimistic approach, create a copy on every update for a POSSIBLE reduce.
	// Optimize later.
	cp, f, l := cb.createStateCopy()
	cb.mu.Unlock()

	// immediately recovery entirely reduces the log to its minimal format
	if wrt && cb.config.Tick == Immediately {
		return cb.ReduceLog(cp, f, l)
	}
	return cb.mayTriggerReduce(cp, f, l)
}

// Recov ... mention that when 'inmem' is true the persistent way is ineficient,
// considering use RecovBytes instead ...
// NOTE: [p, n] indexes are ignored on CircBuff structures ...
func (cb *CircBuffHT) Recov(p, n uint64) ([]pb.Command, error) {
	if n < p {
		return nil, errors.New("invalid interval request, 'n' must be >= 'p'")
	}
	cb.mu.Lock()
	cp, f, l := cb.createStateCopy()
	cb.mu.Unlock()

	if err := cb.mayExecuteLazyReduce(cp, f, l); err != nil {
		return nil, err
	}
	return cb.retrieveLog()
}

// RecovBytes ... returns an already serialized data, most efficient approach
// when 'cb.config.Inmem == false' ... Describe the slicing protocol for pbuffs
// NOTE: [p, n] indexes are ignored on CircBuff structures ...
func (cb *CircBuffHT) RecovBytes(p, n uint64) ([]byte, error) {
	if n < p {
		return nil, errors.New("invalid interval request, 'n' must be >= 'p'")
	}
	cb.mu.Lock()
	cp, f, l := cb.createStateCopy()
	cb.mu.Unlock()

	if err := cb.mayExecuteLazyReduce(cp, f, l); err != nil {
		return nil, err
	}
	return cb.retrieveRawLog(f, l)
}

// ReduceLog ... is only launched on thread-safe routines ...
func (cb *CircBuffHT) ReduceLog(cp []buffEntry, first, last uint64) error {
	cmds, err := cb.ExecuteReduceAlg(cp, first, last)
	if err != nil {
		return err
	}
	return cb.updateLogState(cmds, first, last)
}

// mayTriggerReduce ... unsafe ... must be called from mutual exclusion ...
func (cb *CircBuffHT) mayTriggerReduce(cp []buffEntry, first, last uint64) error {
	// cap surprassing on next insertion
	if cb.len == cb.cap {
		cb.resetBuffState()
		return cb.ReduceLog(cp, first, last)
	}

	if cb.config.Tick != Interval {
		return nil
	}
	cb.count++
	if cb.count >= cb.config.Period {
		cb.count = 0
		return cb.ReduceLog(cp, first, last)
	}
	return nil
}

// mayExecuteLazyReduce triggers a reduce procedure if delayed config is set or first
// 'av.config.Period' wasnt reached yet. On CircBuff structures, informed [first, last]
// MUST ALWAYS match the first and last indexes contained on the local copy parameter.
// Informing a different interval would incoherent with the Interval config and compromise
// safety.
func (cb *CircBuffHT) mayExecuteLazyReduce(cp []buffEntry, first, last uint64) error {
	if cb.config.Tick == Delayed {
		err := cb.ReduceLog(cp, first, last)
		if err != nil {
			return err
		}

	} else if cb.config.Tick == Interval && !cb.firstReduceExists() {
		err := cb.ReduceLog(cp, first, last)
		if err != nil {
			return err
		}
	}
	return nil
}

func (cb *CircBuffHT) resetBuffState() {
	cb.len = 0   // old values will later be overwritten by insertions
	cb.count = 0 // interval counting
	cb.first = 0
	cb.last = 0
}

// createStateCopy returns a local view of the buffer structure and indexes metadata. Must
// be called from mutual exclusion scope.
func (cb *CircBuffHT) createStateCopy() ([]buffEntry, uint64, uint64) {
	buff := []buffEntry{}
	i := 0
	for i < cb.len {
		// negative values already account circular reference
		pos := (cb.cur - cb.len + i) % cb.cap
		v := (*cb.buff)[pos]
		buff = append(buff, v)
		i++
	}
	return buff, cb.first, cb.last
}

// ExecuteReduceAlg launches the config algorithm on a conflict-free copy.
func (cb *CircBuffHT) ExecuteReduceAlg(cp []buffEntry, p, n uint64) ([]pb.Command, error) {
	switch cb.config.Alg {
	case IterCircBuff:
		return IterCircBuffHT(cp, p, n), nil
	}
	return nil, errors.New("unsupported reduce algorithm for a CircBuffHT structure")
}
