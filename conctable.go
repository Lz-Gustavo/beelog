package beelog

import (
	"context"
	"sync"

	"github.com/Lz-Gustavo/beelog/pb"
)

const (
	// maximum of 'concLevel' + 1 different views of the same structure
	concLevel int = 2
)

// ConcTable ...
type ConcTable struct {
	views [concLevel]minStateTable
	mu    [concLevel]sync.Mutex
	logs  [concLevel]logData
	canc  context.CancelFunc

	reduceReq chan int
	current   int
}

// NewConcTable ...
func NewConcTable(ctx context.Context) *ConcTable {
	c, cancel := context.WithCancel(ctx)
	ct := &ConcTable{
		canc:      cancel,
		reduceReq: make(chan int, chanBuffSize),
	}

	for i := 0; i < concLevel; i++ {
		ct.logs[i] = logData{config: DefaultLogConfig()}
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
		ct.logs[i] = logData{config: cfg}
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

// Len ...
func (ct *ConcTable) Len() uint64 {
	// TODO: investigate later
	return ct.logs[ct.current].last - ct.logs[ct.current].first
}

// Log ...
func (ct *ConcTable) Log(index uint64, cmd pb.Command) error {
	tbl := ct.views[ct.current]
	ld := ct.logs[ct.current]
	var wrt bool

	ct.mu[ct.current].Lock()
	defer ct.mu[ct.current].Unlock()

	if cmd.Op != pb.Command_SET {
		// TODO: treat 'ar.first' attribution on GETs
		ld.last = index

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
		if ct.Len() == 0 {
			ld.first = cmd.Id
		}

		// adjust last index once inserted
		ld.last = cmd.Id
	}

	// trigger immediately reduce
	if wrt && ld.config.Tick == Immediately {
		ct.triggerReduceOnView(ct.current)
	}

	// Delayed config will be ignored
	ct.mayTriggerReduceOnView(ct.current)
	return nil
}

// Recov ...
func (ct *ConcTable) Recov(p, n uint64) ([]pb.Command, error) {
	// TODO:
	return nil, nil
}

// RecovBytes ...
func (ct *ConcTable) RecovBytes(p, n uint64) ([]byte, error) {
	// TODO:
	return nil, nil
}

func (ct *ConcTable) handleReduce(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return

		case <-ct.reduceReq:
			// TODO: ...
		}
	}
}

// triggerReduceOnView ...
func (ct *ConcTable) triggerReduceOnView(id int) {
	ct.reduceReq <- id

	// advance current state for next insertions
	num := concLevel + 1
	ct.current = (ct.current - num + 1) % num
}

// mayTriggerReduceOnView ...
func (ct *ConcTable) mayTriggerReduceOnView(id int) {
	if ct.logs[id].config.Tick != Interval {
		return
	}
	ct.logs[id].count++

	// reached reduce period or immediately config
	if ct.logs[id].count >= ct.logs[id].config.Period {
		ct.logs[id].count = 0
		ct.triggerReduceOnView(id)
	}
}

func (ct *ConcTable) willRequireCopy() bool {
	// TODO:
	return false
}

// Shutdown ...
func (ct *ConcTable) Shutdown() {
	ct.canc()
}
