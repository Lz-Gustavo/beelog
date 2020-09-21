package beelog

import (
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/Lz-Gustavo/beelog/pb"
)

type listEntry struct {
	ind uint64
	key string
	ptr *listNode
}

// ListHT ...
type ListHT struct {
	lt  *list
	aux *stateTable
	mu  sync.RWMutex
	logData
}

// NewListHT ...
func NewListHT() *ListHT {
	ht := make(stateTable, 0)
	return &ListHT{
		logData: logData{config: DefaultLogConfig()},
		lt:      &list{},
		aux:     &ht,
	}
}

// NewListHTWithConfig ...
func NewListHTWithConfig(cfg *LogConfig) (*ListHT, error) {
	err := cfg.ValidateConfig()
	if err != nil {
		return nil, err
	}

	ht := make(stateTable, 0)
	return &ListHT{
		logData: logData{config: cfg},
		lt:      &list{},
		aux:     &ht,
	}, nil
}

// Str returns a string representation of the list state, used for debug purposes.
func (l *ListHT) Str() string {
	l.mu.RLock()
	defer l.mu.RUnlock()

	var strs []string
	for i := l.lt.first; i != nil; i = i.next {
		strs = append(strs, fmt.Sprintf("%v->", i.val))
	}
	return strings.Join(strs, " ")
}

// Len returns the list length.
func (l *ListHT) Len() uint64 {
	return l.lt.len
}

// Log records the occurence of command 'cmd' on the provided index. Writes are
// mapped as a new node on the underlying liked list, with a pointer to the newly
// inserted state update on the update list for its particular key.
func (l *ListHT) Log(cmd pb.Command) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if cmd.Op != pb.Command_SET {
		// TODO: treat 'l.first' attribution on GETs
		l.last = cmd.Id
		return l.mayTriggerReduce()
	}

	entry := &listEntry{
		ind: cmd.Id,
		key: cmd.Key,
	}

	// a write cmd always references a new state on the aux hash table
	st := &State{
		ind: cmd.Id,
		cmd: cmd,
	}

	_, exists := (*l.aux)[cmd.Key]
	if !exists {
		(*l.aux)[cmd.Key] = &list{}
	}

	// add state to the list of updates in that particular key
	lNode := (*l.aux)[cmd.Key].push(st)
	entry.ptr = lNode

	// adjust first structure index
	if l.lt.tail == nil {
		l.first = entry.ind
	}

	// insert new entry on the main list
	l.lt.push(entry)

	// adjust last index once inserted
	l.last = cmd.Id

	// immediately recovery entirely reduces the log to its minimal format
	if l.config.Tick == Immediately {
		return l.ReduceLog(l.first, l.last)
	}
	return l.mayTriggerReduce()
}

// Recov returns a compacted log of commands, following the requested [p, n]
// interval if 'Delayed' reduce is configured. On different period configurations,
// the entire reduced log is always returned. On persistent configuration (i.e.
// 'inmem' false) the entire log is loaded and then unmarshaled, consider using
// 'RecovBytes' calls instead.
func (l *ListHT) Recov(p, n uint64) ([]pb.Command, error) {
	if n < p {
		return nil, errors.New("invalid interval request, 'n' must be >= 'p'")
	}
	l.mu.RLock()
	defer l.mu.RUnlock()

	if err := l.mayExecuteLazyReduce(p, n); err != nil {
		return nil, err
	}
	return l.retrieveLog()
}

// RecovBytes returns an already serialized log, parsed from persistent storage
// or marshaled from the in-memory state. Its the most efficient approach on persistent
// configuration, avoiding an extra marshaling step during recovery. The command
// interpretation from the byte stream follows a simple slicing protocol, where
// the size of each command is binary encoded before the raw pbuff.
func (l *ListHT) RecovBytes(p, n uint64) ([]byte, error) {
	if n < p {
		return nil, errors.New("invalid interval request, 'n' must be >= 'p'")
	}
	l.mu.RLock()
	defer l.mu.RUnlock()

	if err := l.mayExecuteLazyReduce(p, n); err != nil {
		return nil, err
	}
	return l.retrieveRawLog(p, n)
}

// ReduceLog applies the configured reduce algorithm and updates the current log state.
// Must only be called within mutual exclusion scope.
func (l *ListHT) ReduceLog(p, n uint64) error {
	cmds, err := ApplyReduceAlgo(l, l.config.Alg, p, n)
	if err != nil {
		return err
	}
	return l.updateLogState(cmds, p, n, false)
}

// mayTriggerReduce possibly triggers the reduce algorithm based on config params
// (e.g. interval period reached). Must only be called within mutual exclusion scope.
func (l *ListHT) mayTriggerReduce() error {
	if l.config.Tick != Interval {
		return nil
	}
	l.count++
	if l.count >= l.config.Period {
		l.count = 0
		return l.ReduceLog(l.first, l.last)
	}
	return nil
}

// mayExecuteLazyReduce triggers a reduce procedure if delayed config is set or first
// 'config.Period' wasnt reached yet.
func (l *ListHT) mayExecuteLazyReduce(p, n uint64) error {
	if l.config.Tick == Delayed {
		err := l.ReduceLog(p, n)
		if err != nil {
			return err
		}

	} else if l.config.Tick == Interval && !l.firstReduceExists() {
		// must reduce the entire structure, just the desired interval would
		// be incoherent with the Interval config
		err := l.ReduceLog(l.first, l.last)
		if err != nil {
			return err
		}
	}
	return nil
}

func (l *ListHT) searchEntryNodeByIndex(ind uint64) *listNode {
	start := l.lt.first
	last := l.lt.tail

	var mid *listNode
	for last != start {

		mid = findMidInList(start, last)
		ent := mid.val.(*listEntry)

		// found in mid
		if ent.ind == ind {
			return mid

			// greater
		} else if ind > ent.ind {
			start = mid.next

			// less
		} else {
			last = mid
		}
	}
	// instead of nil, return the nearest element
	return mid
}

func (l *ListHT) resetVisitedValues() {
	for _, list := range *l.aux {
		list.visited = false
	}
}
