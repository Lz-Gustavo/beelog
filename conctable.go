package beelog

import (
	"context"
	"errors"
	"log"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/Lz-Gustavo/beelog/pb"
)

const (
	// maximum of 'concLevel' different views of the same structure
	concLevel int = 2
)

// ConcTable ...
type ConcTable struct {
	views [concLevel]minStateTable
	mu    [concLevel]sync.Mutex
	logs  [concLevel]logData
	canc  context.CancelFunc

	reduceReq chan int
	curMu     sync.Mutex
	current   int
	prevLog   int32 // atomic
}

// NewConcTable ...
func NewConcTable(ctx context.Context) *ConcTable {
	c, cancel := context.WithCancel(ctx)
	ct := &ConcTable{
		canc:      cancel,
		reduceReq: make(chan int, chanBuffSize),
	}

	for i := 0; i < concLevel; i++ {
		def := *DefaultLogConfig()
		def.Fname = def.Fname + "." + strconv.Itoa(i)
		ct.logs[i] = logData{config: &def}
		ct.views[i] = make(minStateTable, 0)
	}
	go ct.handleReduce(c)
	return ct
}

// NewConcTableWithConfig ...
func NewConcTableWithConfig(ctx context.Context, cfg *LogConfig) (*ConcTable, error) {
	err := cfg.ValidateConfig()
	if err != nil {
		return nil, err
	}

	c, cancel := context.WithCancel(ctx)
	ct := &ConcTable{
		canc:      cancel,
		reduceReq: make(chan int, chanBuffSize),
	}

	for i := 0; i < concLevel; i++ {
		nCfg := *cfg
		nCfg.Fname = nCfg.Fname + "." + strconv.Itoa(i)
		ct.logs[i] = logData{config: &nCfg}
		ct.views[i] = make(minStateTable, 0)
	}
	go ct.handleReduce(c)
	return ct, nil
}

// Str ...
func (ct *ConcTable) Str() string {
	// TODO:
	return ""
}

// Len returns the length of the current active view. A structure lenght
// is defined as the number of inserted elements on its underlying container,
// which disregards read operations. To interpret the absolute number of cmds
// safely discarded on ConcTable structures, just compute:
//   ct.logs[ct.current].last - ct.logs[ct.current].first + 1
func (ct *ConcTable) Len() uint64 {
	return uint64(len(ct.views[ct.current]))
}

// Log records the occurence of command 'cmd' on the provided index.
func (ct *ConcTable) Log(index uint64, cmd pb.Command) error {
	wrt := cmd.Op == pb.Command_SET
	var willReduce bool

	ct.curMu.Lock()
	cur := ct.current

	willReduce = ct.willRequireReduceOnView(wrt, cur)
	if willReduce {
		ct.advanceCurrentView()
	}
	ct.curMu.Unlock()

	tbl := ct.views[cur]
	ct.mu[cur].Lock()
	if !wrt {
		// TODO: treat 'ar.first' attribution on GETs
		ct.logs[cur].last = index

	} else {
		wrt = true

		// TODO: Ensure same index for now. Log API will change in time
		cmd.Id = index

		// update current state for that particular key
		st := State{
			ind: index,
			cmd: cmd,
		}
		tbl[cmd.Key] = st

		// adjust first structure index
		if !ct.logs[cur].logged {
			ct.logs[cur].first = cmd.Id
			ct.logs[cur].logged = true
		}

		// adjust last index once inserted
		ct.logs[cur].last = cmd.Id
	}
	ct.mu[cur].Unlock()

	if willReduce {
		ct.reduceReq <- cur
	}
	return nil
}

// Recov returns a compacted log of commands, following the requested [p, n]
// interval if 'Delayed' reduce is configured. On different period configurations,
// the entire reduced log is always returned. On persistent configuration (i.e.
// 'inmem' false) the entire log is loaded and then unmarshaled, consider using
// 'RecovBytes' calls instead. On CircBuff structures, indexes [p, n] are ignored.
func (ct *ConcTable) Recov(p, n uint64) ([]pb.Command, error) {
	if n < p {
		return nil, errors.New("invalid interval request, 'n' must be >= 'p'")
	}
	cur := ct.readAndAdvanceCurrentView()
	ct.mu[cur].Lock()
	defer ct.mu[cur].Unlock()

	// sequentially reduce since 'Recov' will already be called concurrently
	exec, err := ct.mayExecuteLazyReduce(cur)
	if err != nil {
		return nil, err
	}

	// executed a lazy reduce, must read from the 'cur' log
	if exec {
		return ct.logs[cur].retrieveLog()
	}
	// didnt execute, must read from the previous log cursor
	prev := atomic.LoadInt32(&ct.prevLog)
	return ct.logs[prev].retrieveLog()
}

// RecovBytes returns an already serialized log, parsed from persistent storage
// or marshaled from the in-memory state. Its the most efficient approach on persistent
// configuration, avoiding an extra marshaling step during recovery. The command
// interpretation from the byte stream follows a simple slicing protocol, where
// the size of each command is binary encoded before the raw pbuff.
func (ct *ConcTable) RecovBytes(p, n uint64) ([]byte, error) {
	if n < p {
		return nil, errors.New("invalid interval request, 'n' must be >= 'p'")
	}
	cur := ct.readAndAdvanceCurrentView()
	ct.mu[cur].Lock()
	defer ct.mu[cur].Unlock()

	// sequentially reduce since 'Recov' will already be called concurrently
	exec, err := ct.mayExecuteLazyReduce(cur)
	if err != nil {
		return nil, err
	}

	// executed a lazy reduce, must read from the 'cur' log
	if exec {
		return ct.logs[cur].retrieveRawLog(ct.logs[cur].first, ct.logs[cur].last)
	}
	// didnt execute, must read from the previous log cursor
	prev := atomic.LoadInt32(&ct.prevLog)
	return ct.logs[prev].retrieveRawLog(ct.logs[prev].first, ct.logs[prev].last)
}

// ReduceLog applies the configured algorithm on a different copy and
// updates the lates log state.
func (ct *ConcTable) ReduceLog(id int) error {
	cmds, err := ct.executeReduceAlgOnView(id)
	if err != nil {
		return err
	}
	return ct.logs[id].updateLogState(cmds, ct.logs[id].first, ct.logs[id].last)
}

func (ct *ConcTable) handleReduce(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return

		case cur := <-ct.reduceReq:
			ct.mu[cur].Lock()
			err := ct.ReduceLog(cur)
			if err != nil {
				log.Fatalln("failed during reduce procedure, err:", err.Error())
			}

			// update the last reduced index
			atomic.StoreInt32(&ct.prevLog, int32(cur))

			// clean 'cur' view state
			ct.resetViewState(cur)
			ct.mu[cur].Unlock()
		}
	}
}

// readAndAdvanceCurrentView reads the current view id then advances it to the next
// available identifier, returning the old observed value.
func (ct *ConcTable) readAndAdvanceCurrentView() int {
	ct.curMu.Lock()
	cur := ct.current
	ct.advanceCurrentView()
	ct.curMu.Unlock()
	return cur
}

// advanceCurrentView advances the current view to its next id.
func (ct *ConcTable) advanceCurrentView() {
	d := (ct.current - concLevel + 1)
	ct.current = modInt(d, concLevel)
}

// mayTriggerReduceOnView possibly triggers the reduce algorithm over the informed view
// based on config params (e.g. interval period reached).
func (ct *ConcTable) mayTriggerReduceOnView(id int) {
	if ct.logs[id].config.Tick != Interval {
		return
	}
	ct.logs[id].count++

	// reached reduce period or immediately config
	if ct.logs[id].count >= ct.logs[id].config.Period {
		ct.logs[id].count = 0
		// trigger reduce on view
		ct.reduceReq <- id
	}
}

func (ct *ConcTable) willRequireReduceOnView(wrt bool, id int) bool {
	// write operation and immediately config
	if wrt && ct.logs[id].config.Tick == Immediately {
		return true
	}

	// read on immediately or delayed config, wont need reduce
	if ct.logs[id].config.Tick != Interval {
		return false
	}
	ct.logs[id].count++

	// reached reduce period
	if ct.logs[id].count >= ct.logs[id].config.Period {
		ct.logs[id].count = 0
		return true
	}
	return false
}

// mayExecuteLazyReduce triggers a reduce procedure if delayed config is set or first
// 'config.Period' wasnt reached yet. Returns true if reduce was executed, false otherwise.
func (ct *ConcTable) mayExecuteLazyReduce(id int) (bool, error) {
	if ct.logs[id].config.Tick == Delayed {
		err := ct.ReduceLog(id)
		if err != nil {
			return true, err
		}

	} else if ct.logs[id].config.Tick == Interval && !ct.logs[id].firstReduceExists() {
		err := ct.ReduceLog(id)
		if err != nil {
			return true, err
		}

	} else {
		return false, nil
	}
	return true, nil
}

// retrieveCurrentViewCopy returns a copy of the current view, without advancing it.
// Used only for test purposes.
func (ct *ConcTable) retrieveCurrentViewCopy() minStateTable {
	ct.curMu.Lock()
	defer ct.curMu.Unlock()
	return ct.views[ct.current]
}

func (ct *ConcTable) willRequireCopy() bool {
	// TODO: i dont remember why ive declared it...
	return false
}

// resetViewState cleans the current state of the informed view. Must be called from mutual
// exclusion scope.
func (ct *ConcTable) resetViewState(id int) {
	ct.views[id] = make(minStateTable, 0)

	// reset log data
	ct.logs[id].first, ct.logs[id].last = 0, 0
	ct.logs[id].logged = false
}

// executeReduceAlgOnView applies the configured reduce algorithm on a conflict-free view,
// mutual exclusion is done by outer scope.
func (ct *ConcTable) executeReduceAlgOnView(id int) ([]pb.Command, error) {
	switch ct.logs[id].config.Alg {
	case IterConcTable:
		return IterConcTableOnView(&ct.views[id]), nil
	}
	return nil, errors.New("unsupported reduce algorithm for a CircBuffHT structure")
}

// Shutdown ...
func (ct *ConcTable) Shutdown() {
	ct.canc()
}

// computes the modulu operation, returning the dividend signal result. In all cases
// b is ALWAYS a non-negative constant, which allows a minor optimization (one less
// comparison for b signal).
func modInt(a, b int) int {
	a = a % b
	if a >= 0 {
		return a
	}
	return a + b
}
