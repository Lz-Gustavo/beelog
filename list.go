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

// Log records the occurence of command 'cmd' on the provided index. Writes are as
//  a new node on the underlying liked list,  with a pointer to the newly inserted
// state update on the update list for its particular key..
func (l *ListHT) Log(index uint64, cmd pb.Command) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if cmd.Op != pb.Command_SET {
		// TODO: treat 'l.first' attribution on GETs
		l.last = index
		return l.mayTriggerReduce()
	}

	// TODO: Ensure same index for now. Log API will change in time
	cmd.Id = index

	entry := &listEntry{
		ind: index,
		key: cmd.Key,
	}

	// a write cmd always references a new state on the aux hash table
	st := &State{
		ind: index,
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
	l.last = index

	// immediately recovery entirely reduces the log to its minimal format
	if l.config.Tick == Immediately {
		return l.ReduceLog(l.first, l.last)
	}
	return l.mayTriggerReduce()
}

// Recov ... mention that when 'inmem' is true the persistent way is ineficient,
// considering use RecovBytes instead ...
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

// RecovBytes ... returns an already serialized data, most efficient approach
// when 'l.config.Inmem == false' ... Describe the slicing protocol for pbuffs
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

// ReduceLog ... is only launched on thread-safe routines ...
func (l *ListHT) ReduceLog(p, n uint64) error {
	cmds, err := ApplyReduceAlgo(l, l.config.Alg, p, n)
	if err != nil {
		return err
	}
	return l.updateLogState(cmds, p, n)
}

// mayTriggerReduce ... unsafe ... must be called from mutual exclusion ...
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

func (l *ListHT) mayExecuteLazyReduce(p, n uint64) error {
	// reduced if delayed config or first 'av.config.Period' wasnt reached yet
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
