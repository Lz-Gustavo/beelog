package beelog

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"time"
)

const (
	// Every measureInitLat invocation has a '1/measureChance' chance to set 'hold'
	// value, and capturing timestamps for latency analysis until latency tuple is
	// recorded.
	measureChance int = 10
)

// latencyMeasure holds auxiliar variables to implement an in-deep latency analysis
// on ConcTable operations.
type latencyMeasure struct {
	hold   []bool
	buff   *bytes.Buffer
	latOut *os.File

	initLat []time.Time
	fillLat []time.Time
	persLat []time.Time
}

func newLatencyMeasure(concLvl int, filename string) (*latencyMeasure, error) {
	fd, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_APPEND|os.O_TRUNC, 0644)
	if err != nil {
		return nil, err
	}

	return &latencyMeasure{
		hold:   make([]bool, concLvl, concLvl),
		buff:   bytes.NewBuffer(nil),
		latOut: fd,

		initLat: make([]time.Time, concLvl, concLvl),
		fillLat: make([]time.Time, concLvl, concLvl),
		persLat: make([]time.Time, concLvl, concLvl),
	}, nil
}

func (lm *latencyMeasure) measureInitLat(id int) bool {
	// already holding a time value, reset only on 'recordLatency()' calls
	if lm.hold[id] {
		return false
	}

	if coin := rand.Intn(measureChance); coin != 0 {
		return false
	}

	lm.initLat[id] = time.Now()
	lm.hold[id] = true
	return true
}

func (lm *latencyMeasure) measureFillLat(id int) bool {
	if !lm.hold[id] {
		return false
	}
	lm.fillLat[id] = time.Now()
	return true
}

func (lm *latencyMeasure) measurePersLat(id int) bool {
	if !lm.hold[id] {
		return false
	}
	lm.persLat[id] = time.Now()
	return true
}

func (lm *latencyMeasure) recordLatencyTuple(id int) (bool, error) {
	if !lm.hold[id] {
		return false, nil
	}
	_, err := fmt.Fprintf(lm.buff, "%d,%d,%d\n", lm.initLat[id].UnixNano(), lm.fillLat[id].UnixNano(), lm.persLat[id].UnixNano())
	if err != nil {
		return false, err
	}
	lm.hold[id] = false
	return true, nil
}

func (lm *latencyMeasure) recordLatency(id int) (bool, error) {
	// do not record if not holding, time wasnt initialized
	if !lm.hold[id] {
		return false, nil
	}
	_, err := fmt.Fprintf(lm.buff, "%d\n", time.Since(lm.initLat[id]))
	if err != nil {
		return false, err
	}
	lm.hold[id] = false
	return true, nil
}

func (lm *latencyMeasure) flush() error {
	_, err := lm.buff.WriteTo(lm.latOut)
	if err != nil {
		return err
	}
	return nil
}

func (lm *latencyMeasure) close() {
	lm.latOut.Close()
}
