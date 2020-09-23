package beelog

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Lz-Gustavo/beelog/pb"
)

const (
	// ConcTable.concLevel sets the number of different views of the same structure.
	defaultConcLvl int = 2

	// number of commands to wait until a complete state reset for Immediately
	// reduce period.
	resetOnImmediately int = 4000
)

// logEvent represents a event metadata passed to logger routines signalling a persistence
// to a certain table, and the array position to store the measurement data.
type logEvent struct {
	table, measure int
}

// ConcTable ...
type ConcTable struct {
	views []minStateTable
	mu    []sync.Mutex
	logs  []logData
	canc  context.CancelFunc

	concLevel int
	loggerReq chan logEvent
	curMu     sync.Mutex
	current   int
	prevLog   int32 // atomic
	logFolder string

	msr bool
	lm  *latencyMeasure
}

// NewConcTable ...
func NewConcTable(ctx context.Context) *ConcTable {
	c, cancel := context.WithCancel(ctx)
	ct := &ConcTable{
		canc:      cancel,
		loggerReq: make(chan logEvent, chanBuffSize),
		concLevel: defaultConcLvl,

		views: make([]minStateTable, defaultConcLvl, defaultConcLvl),
		mu:    make([]sync.Mutex, defaultConcLvl, defaultConcLvl),
		logs:  make([]logData, defaultConcLvl, defaultConcLvl),
	}

	def := *DefaultLogConfig()
	def.Alg = IterConcTable
	for i := 0; i < defaultConcLvl; i++ {
		ct.logs[i] = logData{config: &def}
		ct.views[i] = make(minStateTable, 0)
	}
	ct.logFolder = extractLocation(def.Fname)

	// Measure disabled in default config
	go ct.handleReduce(c, false)
	return ct
}

// NewConcTableWithConfig ...
func NewConcTableWithConfig(ctx context.Context, concLvl int, cfg *LogConfig) (*ConcTable, error) {
	err := cfg.ValidateConfig()
	if err != nil {
		return nil, err
	}
	if concLvl < 0 {
		return nil, errors.New("must inform a positive value for 'concLevel' argument")
	}

	c, cancel := context.WithCancel(ctx)
	ct := &ConcTable{
		canc:      cancel,
		loggerReq: make(chan logEvent, chanBuffSize),
		concLevel: concLvl,

		views: make([]minStateTable, concLvl, concLvl),
		mu:    make([]sync.Mutex, concLvl, concLvl),
		logs:  make([]logData, concLvl, concLvl),
	}

	for i := 0; i < concLvl; i++ {
		ct.logs[i] = logData{config: cfg}
		ct.views[i] = make(minStateTable, 0)
	}
	ct.logFolder = extractLocation(cfg.Fname)

	if cfg.Measure {
		ct.msr = true
		fn := ct.logFolder + "bl-" + strconv.Itoa(int(cfg.Period)) + "-latency.out"
		ct.lm, err = newLatencyMeasure(concLvl, int(cfg.Period), fn)
		if err != nil {
			return nil, err
		}
	}
	go ct.handleReduce(c, false)

	// launch another reduce for secondary disk
	if cfg.ParallelIO {
		go ct.handleReduce(c, true)
	}
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
func (ct *ConcTable) Log(cmd pb.Command) error {
	wrt := cmd.Op == pb.Command_SET
	ct.curMu.Lock()
	cur := ct.current

	// first command
	if ct.msr {
		ct.lm.absIndex++
		if (ct.lm.absIndex%ct.lm.interval == 1 || ct.lm.interval == 1) && rand.Intn(measureChance) == 0 {
			ct.lm.initLat[ct.lm.msrIndex] = time.Now().UnixNano()
			ct.lm.drawn = true
		}
	}

	willReduce, advance := ct.willRequireReduceOnView(wrt, cur)
	if advance {
		ct.advanceCurrentView()
	}

	// must acquire view mutex before releasing cursor to ensure safety
	ct.mu[cur].Lock()
	ct.curMu.Unlock()

	if ct.msr && ct.lm.drawn {
		if ct.lm.absIndex%ct.lm.interval == 1 {
			// first command was written into table
			ct.lm.writeLat[ct.lm.msrIndex] = time.Now().UnixNano()

		} else if ct.lm.interval == 1 {
			// special case of first and last command, which does not fall
			// on first condition
			ct.lm.writeLat[ct.lm.msrIndex] = time.Now().UnixNano()
			ct.lm.fillLat[ct.lm.msrIndex] = time.Now().UnixNano()

		} else if ct.lm.absIndex%ct.lm.interval == 0 {
			// last command, table filled
			ct.lm.fillLat[ct.lm.msrIndex] = time.Now().UnixNano()
		}
	}

	// adjust first structure index
	if !ct.logs[cur].logged {
		ct.logs[cur].first = cmd.Id
		ct.logs[cur].logged = true
	}

	if wrt {
		// update current state for that particular key
		st := State{
			ind: cmd.Id,
			cmd: cmd,
		}
		ct.views[cur][cmd.Key] = st
	}
	// adjust last index
	ct.logs[cur].last = cmd.Id

	if willReduce {
		// mutext will be later unlocked by the logger routine
		if ct.msr && ct.lm.drawn {
			ct.loggerReq <- logEvent{cur, ct.lm.msrIndex}
			ct.lm.msrIndex++
			ct.lm.drawn = false

		} else {
			ct.loggerReq <- logEvent{cur, -1}
		}

	} else {
		ct.mu[cur].Unlock()
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

	// sequentially reduce since 'Recov' will already be called concurrently
	exec, err := ct.mayExecuteLazyReduce(cur)
	if err != nil {
		return nil, err
	}

	var cmds []pb.Command
	if exec {
		defer ct.mu[cur].Unlock()

		// executed a lazy reduce, must read from the 'cur' log
		cmds, err = ct.logs[cur].retrieveLog()
		if err != nil {
			return nil, err
		}

	} else {
		// didnt execute, must read from the previous log cursor
		prev := atomic.LoadInt32(&ct.prevLog)
		cmds, err = ct.logs[prev].retrieveLog()
		if err != nil {
			return nil, err
		}
	}
	return cmds, nil
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

	// sequentially reduce since 'Recov' will already be called concurrently
	exec, err := ct.mayExecuteLazyReduce(cur)
	if err != nil {
		return nil, err
	}

	var raw []byte
	if exec {
		defer ct.mu[cur].Unlock()

		// executed a lazy reduce, must read from the 'cur' log
		raw, err = ct.logs[cur].retrieveRawLog(ct.logs[cur].first, ct.logs[cur].last)
		if err != nil {
			return nil, err
		}

	} else {
		// didnt execute, must read from the previous log cursor
		prev := atomic.LoadInt32(&ct.prevLog)
		raw, err = ct.logs[prev].retrieveRawLog(ct.logs[prev].first, ct.logs[prev].last)
		if err != nil {
			return nil, err
		}
	}
	return raw, nil
}

// RecovEntireLog ...
func (ct *ConcTable) RecovEntireLog() ([]byte, int, error) {
	fp := ct.logFolder + "*.log"
	fs, err := filepath.Glob(fp)
	if err != nil {
		return nil, 0, err
	}

	// sorts by lenght and lexicographically for equal len
	sort.Sort(byLenAlpha(fs))
	buf := bytes.NewBuffer(nil)

	for _, fn := range fs {
		fd, err := os.OpenFile(fn, os.O_RDONLY, 0400)
		if err != nil && err != io.EOF {
			return nil, 0, fmt.Errorf("failed while opening log '%s', err: '%s'", fn, err.Error())
		}
		defer fd.Close()

		// read the retrieved log interval
		var f, l uint64
		_, err = fmt.Fscanf(fd, "%d\n%d\n", &f, &l)
		if err != nil {
			return nil, 0, fmt.Errorf("failed while reading log '%s', err: '%s'", fn, err.Error())
		}

		// reset cursor
		_, err = fd.Seek(0, io.SeekStart)
		if err != nil {
			return nil, 0, fmt.Errorf("failed while reading log '%s', err: '%s'", fn, err.Error())
		}

		// each copy stages through a temporary buffer, copying to dest once completed
		_, err = io.Copy(buf, fd)
		if err != nil {
			return nil, 0, fmt.Errorf("failed while copying log '%s', err: '%s'", fn, err.Error())
		}
	}
	return buf.Bytes(), len(fs), nil
}

// RecovEntireLogConc ...
// TODO: comeback later once sequential solution is done.
func (ct *ConcTable) RecovEntireLogConc() (<-chan []byte, int, error) {
	fp := ct.logFolder + "*.log"
	fs, err := filepath.Glob(fp)
	if err != nil {
		return nil, 0, err
	}

	// sorts by lenght and lexicographically for equal len
	sort.Sort(byLenAlpha(fs))
	buf := bytes.NewBuffer(nil)
	mu := &sync.Mutex{}

	wg := sync.WaitGroup{}
	wg.Add(len(fs))
	fmt.Println("will be waiting on", len(fs), "files")

	for _, f := range fs {
		// read each file concurrently and write to buffer once done
		go func(fn string) {
			fd, err := os.OpenFile(fn, os.O_RDONLY, 0400)
			if err != nil && err != io.EOF {
				log.Fatalf("failed while opening log '%s', err: '%s'\n", fn, err.Error())
			}
			defer fd.Close()

			// read the retrieved log interval
			var f, l uint64
			_, err = fmt.Fscanf(fd, "%d\n%d\n", &f, &l)
			if err != nil {
				log.Fatalf("failed while reading log '%s', err: '%s'\n", fn, err.Error())
			}

			mu.Lock()
			defer mu.Unlock()

			// increase buffer's capacity, if necessary
			if size := int(l - f); size >= (buf.Cap() - buf.Len()) {
				buf.Grow(size)
			}

			// reset cursor
			_, err = fd.Seek(0, io.SeekStart)
			if err != nil {
				log.Fatalf("failed while reading log '%s', err: '%s'\n", fn, err.Error())
			}

			// each copy stages through a temporary buffer, copying to dest once completed
			_, err = io.Copy(buf, fd)
			if err != nil {
				log.Fatalf("failed while copying log '%s', err: '%s'\n", fn, err.Error())
			}

			wg.Done()
			fmt.Println("finished one...")
			return
		}(f)
	}

	wg.Wait()
	fmt.Println("finished reading logs!")

	out := make(chan []byte, 0)
	sc := bufio.NewScanner(buf)
	go func() {
		for sc.Scan() {
			out <- sc.Bytes()
		}
		close(out)
		return
	}()
	return out, len(fs), nil
}

// persistTable applies the configured algorithm on a specific view and updates
// the latest log state into a new file.
func (ct *ConcTable) persistTable(id int, secDisk bool) error {
	cmds, err := ct.executeReduceAlgOnView(id)
	if err != nil {
		return err
	}
	return ct.logs[id].updateLogState(cmds, ct.logs[id].first, ct.logs[id].last, secDisk)
}

func (ct *ConcTable) reduceLog(cur int, count *int, secDisk bool) error {
	err := ct.persistTable(cur, secDisk)
	if err != nil {
		return err
	}

	// always log, but reset persistent state only after 'resetOnImmediately' cmds
	if ct.logs[cur].config.Tick == Immediately {
		*count++
		if *count < resetOnImmediately {
			ct.mu[cur].Unlock()
			return nil
		}
		*count = 0
	}

	// update the last reduced index, its concurrently accessed by Recov procedures.
	// TODO: uncomment once Recov are evaluated...
	// atomic.StoreInt32(&ct.prevLog, int32(cur))

	// clean 'cur' view state
	ct.resetViewState(cur)
	ct.mu[cur].Unlock()
	return nil
}

func (ct *ConcTable) handleReduce(ctx context.Context, secDisk bool) {
	var count int
	for {
		select {
		case <-ctx.Done():
			return

		case event := <-ct.loggerReq:
			err := ct.reduceLog(event.table, &count, secDisk)
			if err != nil {
				log.Fatalln("failed during reduce procedure, err:", err.Error())
			}

			// requested latency measurement for persist
			if event.measure != -1 {
				ct.lm.perstLat[event.measure] = time.Now().UnixNano()
			}
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
	d := (ct.current - ct.concLevel + 1)
	ct.current = modInt(d, ct.concLevel)
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
		ct.loggerReq <- logEvent{id, -1}
	}
}

// willRequireReduceOnView informs if a reduce procedure will later be trigged on a log procedure,
// and if the current view cursor must be advanced, following some specific rules:
//
// TODO: describe later...
func (ct *ConcTable) willRequireReduceOnView(wrt bool, id int) (bool, bool) {
	// write operation and immediately config
	if wrt && ct.logs[id].config.Tick == Immediately {
		return true, false
	}

	// read on immediately or delayed config, wont need reduce
	if ct.logs[id].config.Tick != Interval {
		return false, false
	}
	ct.logs[id].count++

	// reached reduce period
	if ct.logs[id].count >= ct.logs[id].config.Period {
		ct.logs[id].count = 0
		return true, true
	}
	return false, false
}

// mayExecuteLazyReduce triggers a reduce procedure if delayed config is set or first
// 'config.Period' wasnt reached yet. Returns true if reduce was executed, false otherwise.
//
// TODO: currently 'false' is always passed to persist procedure, which basically flushes
// and stores to the primary disk even if config.ParallelIO is set. Adjust recovery procedure
// implications later.
func (ct *ConcTable) mayExecuteLazyReduce(id int) (bool, error) {
	if ct.logs[id].config.Tick == Delayed {
		ct.mu[id].Lock()
		err := ct.persistTable(id, false)
		if err != nil {
			return true, err
		}

	} else if ct.logs[id].config.Tick == Interval && !ct.logs[id].firstReduceExists() {
		ct.mu[id].Lock()
		err := ct.persistTable(id, false)
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
	return nil, errors.New("unsupported reduce algorithm for a ConcTable structure")
}

// Shutdown ...
func (ct *ConcTable) Shutdown() {
	ct.canc()
	if ct.msr {
		ct.lm.flush()
		ct.lm.close()
	}
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

func applyConcIndexInFname(fn string, id int) string {
	ind := strconv.Itoa(id)
	sep := strings.SplitAfter(fn, ".")
	sep[len(sep)-1] = ind + ".log"
	return strings.Join(sep, "")
}

// extractLocation returns the folder location specified in 'fn', searching for the
// occurence of slash characters ('/'). If none slash is found, "./" is returned instead.
//
// Example:
//   "/path/to/something/content.log" -> "/path/to/something/"
//   "foo.bar"                        -> "./"
func extractLocation(fn string) string {
	ind := strings.LastIndex(fn, "/") + 1
	if ind != -1 {
		return fn[:ind]
	}
	return "./"
}

type byLenAlpha []string

func (a byLenAlpha) Len() int      { return len(a) }
func (a byLenAlpha) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a byLenAlpha) Less(i, j int) bool {
	// lenght order prio
	if len(a[i]) < len(a[j]) {
		return true
	}
	// alphabetic
	if len(a[i]) == len(a[j]) {
		return strings.Compare(a[i], a[j]) == -1
	}
	return false
}
