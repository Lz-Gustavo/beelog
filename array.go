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
	len uint64
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

// Str returns a string representation of the list state, used for debug purposes.
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
	return ar.len
}

// Log records the occurence of command 'cmd' on the provided index. Writes are as
//  a new node on the underlying liked list,  with a pointer to the newly inserted
// state update on the update list for its particular key..
func (ar *ArrayHT) Log(index uint64, cmd pb.Command) error {
	ar.mu.Lock()
	defer ar.mu.Unlock()

	if cmd.Op != pb.Command_SET {
		// TODO: treat 'ar.first' attribution on GETs
		ar.last = index
		return ar.mayTriggerReduce()
	}

	// TODO: Ensure same index for now. Log API will change in time
	cmd.Id = index

	entry := listEntry{
		ind: index,
		key: cmd.Key,
	}

	// a write cmd always references a new state on the aux hash table
	st := &State{
		ind: index,
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
	if ar.len == 0 {
		ar.first = entry.ind
	}

	// insert new entry on the main list
	*ar.arr = append(*ar.arr, entry)

	// adjust last index once inserted
	ar.last = index

	// immediately recovery entirely reduces the log to its minimal format
	if ar.config.Tick == Immediately {
		return ar.ReduceLog(ar.first, ar.last)
	}
	return ar.mayTriggerReduce()
}

// Recov ... mention that when 'inmem' is true the persistent way is ineficient,
// considering use RecovBytes instead ...
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

// RecovBytes ... returns an already serialized data, most efficient approach
// when 'ar.config.Inmem == false' ... Describe the slicing protocol for pbuffs
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

// ReduceLog ... is only launched on thread-safe routines ...
func (ar *ArrayHT) ReduceLog(p, n uint64) error {

	// TODO: re-design array algorithm
	cmds, err := ApplyReduceAlgo(ar, ar.config.Alg, p, n)
	if err != nil {
		return err
	}
	return ar.updateLogState(cmds, p, n)
}

// mayTriggerReduce ... unsafe ... must be called from mutual exclusion ...
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

func (ar *ArrayHT) mayExecuteLazyReduce(p, n uint64) error {
	// reduced if delayed config or first 'av.config.Period' wasnt reached yet
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

func (ar *ArrayHT) searchEntryPosByIndex(ind uint64) uint64 {
	// TODO: implement binary search
	return 0
}

func (ar *ArrayHT) resetVisitedValues() {
	for _, list := range *ar.aux {
		list.visited = false
	}
}
