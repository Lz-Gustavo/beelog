package beelog

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"

	"github.com/Lz-Gustavo/beelog/pb"
)

const (
	defaultCap   = 4000
	chanBuffSize = 128
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

// buffCopy stores only the useful data for a structure snapshot.
type buffCopy struct {
	buf           []buffEntry
	tbl           minStateTable
	cur, cap, len int
	first, last   uint64
}

// CircBuffHT ...
type CircBuffHT struct {
	buff *[]buffEntry
	aux  *minStateTable
	mu   sync.Mutex
	canc context.CancelFunc

	cur, cap, len int
	reduceReq     chan buffCopy
	logData
}

// NewCircBuffHT ...
func NewCircBuffHT(ctx context.Context) *CircBuffHT {
	ht := make(minStateTable, 0)
	sl := make([]buffEntry, defaultCap, defaultCap) // fixed size
	ct, cancel := context.WithCancel(ctx)

	cb := &CircBuffHT{
		logData:   logData{config: DefaultLogConfig()},
		buff:      &sl,
		aux:       &ht,
		cap:       defaultCap,
		canc:      cancel,
		reduceReq: make(chan buffCopy, chanBuffSize),
	}
	go cb.handleReduce(ct)
	return cb
}

// NewCircBuffHTWithConfig ...
func NewCircBuffHTWithConfig(ctx context.Context, cfg *LogConfig, cap int) (*CircBuffHT, error) {
	err := cfg.ValidateConfig()
	if err != nil {
		return nil, err
	}

	ht := make(minStateTable, 0)
	sl := make([]buffEntry, cap, cap) // fixed size
	ct, cancel := context.WithCancel(ctx)

	cb := &CircBuffHT{
		logData:   logData{config: cfg},
		buff:      &sl,
		aux:       &ht,
		cap:       cap,
		canc:      cancel,
		reduceReq: make(chan buffCopy, chanBuffSize),
	}
	go cb.handleReduce(ct)
	return cb, nil
}

// Str returns a string representation of the buffer state, used for debug purposes.
func (cb *CircBuffHT) Str() string {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	var strs []string
	i := 0
	for i < cb.len {
		// negative values already account circular reference
		pos := modInt((cb.cur - cb.len + i), cb.cap)
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

// Log records the occurence of command 'cmd' on the provided index. Writes are
// mapped as a new node on the buffer array, with a pointer to the newly inserted
// state update on the update list for its particular key.
func (cb *CircBuffHT) Log(cmd pb.Command) error {
	cb.mu.Lock()
	var wrt bool

	if cmd.Op != pb.Command_SET {
		// TODO: treat 'ar.first' attribution on GETs
		cb.last = cmd.Id

	} else {
		wrt = true
		entry := buffEntry{
			ind: cmd.Id,
			key: cmd.Key,
		}

		// update current state for that particular key
		st := State{
			ind: cmd.Id,
			cmd: cmd,
		}
		(*cb.aux)[cmd.Key] = st

		// adjust first structure index
		if cb.Len() == 0 {
			cb.first = entry.ind
		}

		// insert new entry
		(*cb.buff)[cb.cur] = entry

		// update insert cursor
		cb.cur = modInt(cb.cur+1, cb.cap)

		// adjust last index and len once inserted
		cb.last = cmd.Id
		cb.len++
	}

	// avoid an unecessary copy, reduce algorithm will be later executed
	if cb.config.Tick == Delayed && cb.len != cb.cap {
		cb.mu.Unlock()
		return nil
	}

	cp := cb.createStateCopy()
	cb.mu.Unlock()

	// Immediately recovery entirely reduces the log to its minimal format, and
	// delays logging until reduce is finished.
	if wrt && cb.config.Tick == Immediately {
		return cb.ReduceLog(cp)
	}
	cb.mayTriggerReduce(cp)
	return nil
}

// Recov returns a compacted log of commands, following the requested [p, n]
// interval if 'Delayed' reduce is configured. On different period configurations,
// the entire reduced log is always returned. On persistent configuration (i.e.
// 'inmem' false) the entire log is loaded and then unmarshaled, consider using
// 'RecovBytes' calls instead. On CircBuff structures, indexes [p, n] are ignored.
func (cb *CircBuffHT) Recov(p, n uint64) ([]pb.Command, error) {
	if n < p {
		return nil, errors.New("invalid interval request, 'n' must be >= 'p'")
	}
	cb.mu.Lock()
	cp := cb.createStateCopy()
	cb.mu.Unlock()

	// sequentially reduce since 'Recov' will already be called concurrently
	if err := cb.mayExecuteLazyReduce(cp); err != nil {
		return nil, err
	}
	return cb.retrieveLog()
}

// RecovBytes returns an already serialized log, parsed from persistent storage
// or marshaled from the in-memory state. Its the most efficient approach on persistent
// configuration, avoiding an extra marshaling step during recovery. The command
// interpretation from the byte stream follows a simple slicing protocol, where
// the size of each command is binary encoded before the raw pbuff. On CircBuff
// structures, indexes [p, n] are ignored.
func (cb *CircBuffHT) RecovBytes(p, n uint64) ([]byte, error) {
	if n < p {
		return nil, errors.New("invalid interval request, 'n' must be >= 'p'")
	}
	cb.mu.Lock()
	cp := cb.createStateCopy()
	cb.mu.Unlock()

	// sequentially reduce since 'RecovBytes' will already be called concurrently
	if err := cb.mayExecuteLazyReduce(cp); err != nil {
		return nil, err
	}
	return cb.retrieveRawLog(cp.first, cp.last)
}

// ReduceLog applies the configured algorithm on a concurrent-safe copy and
// updates the lates log state.
//
// TODO: maybe implement mutual exclusion during state update using a different
// lock.
func (cb *CircBuffHT) ReduceLog(cp buffCopy) error {
	cmds, err := cb.executeReduceAlgOnCopy(&cp)
	if err != nil {
		return err
	}
	return cb.updateLogState(cmds, cp.first, cp.last, false)
}

// mayTriggerReduce possibly triggers the reduce algorithm based on config params
// (e.g. interval period reached) or when the buffer capacity is surprassed on next
// insertion. The circular buffer variant operates over a copy, so it's safe to be
// called concurrently.
func (cb *CircBuffHT) mayTriggerReduce(cp buffCopy) {
	// cap surprassing on next insertion
	if cb.len == cb.cap {
		cb.resetBuffState()
		cb.reduceReq <- cp
		return
	}

	if cb.config.Tick != Interval {
		return
	}
	cb.count++
	if cb.count >= cb.config.Period {
		cb.count = 0
		cb.reduceReq <- cp
	}
}

// mayExecuteLazyReduce triggers a reduce procedure if delayed config is set or first
// 'config.Period' wasnt reached yet. On CircBuff structures, informed [first, last]
// MUST ALWAYS match the first and last indexes contained on the local copy parameter.
// Informing a different interval would incoherent with the 'Interval' config and compromise
// safety.
func (cb *CircBuffHT) mayExecuteLazyReduce(cp buffCopy) error {
	if cb.config.Tick == Delayed {
		err := cb.ReduceLog(cp)
		if err != nil {
			return err
		}

	} else if cb.config.Tick == Interval && !cb.firstReduceExists() {
		err := cb.ReduceLog(cp)
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
//
// TODO: investigate trade-offs between copying an array of states or the buffer array and
// the auxiliar hash table.
func (cb *CircBuffHT) createStateCopy() buffCopy {
	cp := buffCopy{
		cur:   cb.cur,
		len:   cb.len,
		cap:   cb.cap,
		first: cb.first,
		last:  cb.last,
		buf:   make([]buffEntry, cb.cap, cb.cap),
		tbl:   make(minStateTable, len(*cb.aux)),
	}

	copy(cp.buf, *cb.buff)
	for k, v := range *cb.aux {
		cp.tbl[k] = v
	}
	return cp
}

// executeReduceAlgOnCopy applies the configured reduce algorithm on a conflict-free copy.
func (cb *CircBuffHT) executeReduceAlgOnCopy(cp *buffCopy) ([]pb.Command, error) {
	switch cb.config.Alg {
	case IterCircBuff:
		return IterCircBuffHT(cp), nil
	}
	return nil, errors.New("unsupported reduce algorithm for a CircBuffHT structure")
}

func (cb *CircBuffHT) handleReduce(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return

		case cp := <-cb.reduceReq:
			err := cb.ReduceLog(cp)
			if err != nil {
				log.Fatalln("failed during reduce procedure, err:", err.Error())
			}
		}
	}
}

// Shutdown ...
func (cb *CircBuffHT) Shutdown() {
	cb.canc()
}
