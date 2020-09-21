package beelog

import (
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/Lz-Gustavo/beelog/pb"
)

// ArrayHT ...
type ArrayHT struct {
	arr *[]listEntry
	aux *stateTable
	mu  sync.RWMutex
	logData
}

// NewArrayHT ...
func NewArrayHT() *ArrayHT {
	ht := make(stateTable, 0)
	return &ArrayHT{
		logData: logData{config: DefaultLogConfig()},
		arr:     &[]listEntry{},
		aux:     &ht,
	}
}

// NewArrayHTWithConfig ...
func NewArrayHTWithConfig(cfg *LogConfig) (*ArrayHT, error) {
	err := cfg.ValidateConfig()
	if err != nil {
		return nil, err
	}

	ht := make(stateTable, 0)
	var sz uint32
	if sz = cfg.Period; sz < 1000 {
		sz = 1000
	}
	sl := make([]listEntry, 0, 2*sz)

	return &ArrayHT{
		logData: logData{config: cfg},
		arr:     &sl,
		aux:     &ht,
	}, nil
}

// Str returns a string representation of the array state, used for debug purposes.
func (ar *ArrayHT) Str() string {
	ar.mu.RLock()
	defer ar.mu.RUnlock()

	var strs []string
	for _, v := range *ar.arr {
		strs = append(strs, fmt.Sprintf("%v->", v))
	}
	return strings.Join(strs, " ")
}

// Len returns the list length.
func (ar *ArrayHT) Len() uint64 {
	return uint64(len(*ar.arr))
}

// Log records the occurence of command 'cmd' on the provided index. Writes are
// mapped as a new node on the underlying array, with a pointer to the newly inserted
// state update on the update list for its particular key.
func (ar *ArrayHT) Log(cmd pb.Command) error {
	ar.mu.Lock()
	defer ar.mu.Unlock()

	if cmd.Op != pb.Command_SET {
		// TODO: treat 'ar.first' attribution on GETs
		ar.last = cmd.Id
		return ar.mayTriggerReduce()
	}

	entry := listEntry{
		ind: cmd.Id,
		key: cmd.Key,
	}

	// a write cmd always references a new state on the aux hash table
	st := &State{
		ind: cmd.Id,
		cmd: cmd,
	}

	_, exists := (*ar.aux)[cmd.Key]
	if !exists {
		(*ar.aux)[cmd.Key] = &list{}
	}

	// add state to the list of updates in that particular key
	lNode := (*ar.aux)[cmd.Key].push(st)
	entry.ptr = lNode

	// adjust first structure index
	if ar.Len() == 0 {
		ar.first = cmd.Id
	}

	// insert new entry on the main list
	*ar.arr = append(*ar.arr, entry)

	// adjust last index once inserted
	ar.last = cmd.Id

	// immediately recovery entirely reduces the log to its minimal format
	if ar.config.Tick == Immediately {
		return ar.ReduceLog(ar.first, ar.last)
	}
	return ar.mayTriggerReduce()
}

// Recov returns a compacted log of commands, following the requested [p, n]
// interval if 'Delayed' reduce is configured. On different period configurations,
// the entire reduced log is always returned. On persistent configuration (i.e.
// 'inmem' false) the entire log is loaded and then unmarshaled, consider using
// 'RecovBytes' calls instead.
func (ar *ArrayHT) Recov(p, n uint64) ([]pb.Command, error) {
	if n < p {
		return nil, errors.New("invalid interval request, 'n' must be >= 'p'")
	}
	ar.mu.RLock()
	defer ar.mu.RUnlock()

	if err := ar.mayExecuteLazyReduce(p, n); err != nil {
		return nil, err
	}
	return ar.retrieveLog()
}

// RecovBytes returns an already serialized log, parsed from persistent storage
// or marshaled from the in-memory state. Its the most efficient approach on persistent
// configuration, avoiding an extra marshaling step during recovery. The command
// interpretation from the byte stream follows a simple slicing protocol, where
// the size of each command is binary encoded before the raw pbuff.
func (ar *ArrayHT) RecovBytes(p, n uint64) ([]byte, error) {
	if n < p {
		return nil, errors.New("invalid interval request, 'n' must be >= 'p'")
	}
	ar.mu.RLock()
	defer ar.mu.RUnlock()

	if err := ar.mayExecuteLazyReduce(p, n); err != nil {
		return nil, err
	}
	return ar.retrieveRawLog(p, n)
}

// ReduceLog applies the configured reduce algorithm and updates the current log state.
// Must only be called within mutual exclusion scope.
func (ar *ArrayHT) ReduceLog(p, n uint64) error {
	cmds, err := ApplyReduceAlgo(ar, ar.config.Alg, p, n)
	if err != nil {
		return err
	}
	return ar.updateLogState(cmds, p, n, false)
}

// mayTriggerReduce possibly triggers the reduce algorithm based on config params
// (e.g. interval period reached). Must only be called within mutual exclusion scope.
func (ar *ArrayHT) mayTriggerReduce() error {
	if ar.config.Tick != Interval {
		return nil
	}
	ar.count++
	if ar.count >= ar.config.Period {
		ar.count = 0
		return ar.ReduceLog(ar.first, ar.last)
	}
	return nil
}

// mayExecuteLazyReduce triggers a reduce procedure if delayed config is set or first
// 'config.Period' wasnt reached yet.
func (ar *ArrayHT) mayExecuteLazyReduce(p, n uint64) error {
	if ar.config.Tick == Delayed {
		err := ar.ReduceLog(p, n)
		if err != nil {
			return err
		}

	} else if ar.config.Tick == Interval && !ar.firstReduceExists() {
		// must reduce the entire structure, just the desired interval would
		// be incoherent with the Interval config
		err := ar.ReduceLog(ar.first, ar.last)
		if err != nil {
			return err
		}
	}
	return nil
}

// TODO: later improve with an initial guess near 'ind' pos
func (ar *ArrayHT) searchEntryPosByIndex(ind uint64) uint64 {
	start := int64(0)
	last := int64(ar.Len()) - 1
	var mid int64

	for last > start {
		mid = start + (last-start)/2
		ent := (*ar.arr)[mid]

		if ent.ind == ind {
			return uint64(mid)

		} else if ind > ent.ind { // greater
			start = mid + 1

		} else { // less
			last = mid - 1
		}
	}
	return uint64(mid)
}

func (ar *ArrayHT) resetVisitedValues() {
	for _, list := range *ar.aux {
		list.visited = false
	}
}
