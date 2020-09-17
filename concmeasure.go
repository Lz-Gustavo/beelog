package beelog

import (
	"fmt"
	"os"
	"time"
)

// LatencyMeasure ...
type LatencyMeasure struct {
	hold   []bool
	latOut *os.File

	initLat []time.Time
	fillLat []time.Time
	persLat []time.Time
}

// NewLatencyMeasure ...
func NewLatencyMeasure(concLvl int, filename string) (*LatencyMeasure, error) {
	fd, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_APPEND|os.O_TRUNC, 0644)
	if err != nil {
		return nil, err
	}

	return &LatencyMeasure{
		hold:   make([]bool, concLvl, concLvl),
		latOut: fd,

		initLat: make([]time.Time, concLvl, concLvl),
		fillLat: make([]time.Time, concLvl, concLvl),
		persLat: make([]time.Time, concLvl, concLvl),
	}, nil
}

func (lm *LatencyMeasure) measureInitLat(id int) bool {
	// already holding a time value, reset only on 'recordLatency()' calls
	if lm.hold[id] {
		return false
	}
	lm.initLat[id] = time.Now()
	lm.hold[id] = true
	return true
}

func (lm *LatencyMeasure) measureFillLat(id int) bool {
	lm.fillLat[id] = time.Now()
	return true
}

func (lm *LatencyMeasure) measurePersLat(id int) bool {
	lm.persLat[id] = time.Now()
	return true
}

func (lm *LatencyMeasure) recordLatency(id int) (bool, error) {
	// do not record if not holding, time wasnt initialized
	if !lm.hold[id] {
		return false, nil
	}
	_, err := fmt.Fprintf(lm.latOut, "%d\n", time.Since(lm.initLat[id]))
	if err != nil {
		return false, err
	}
	lm.hold[id] = false
	return true, nil
}

func (lm *LatencyMeasure) recordLatencyTuple(id int) (bool, error) {
	_, err := fmt.Fprintf(lm.latOut, "%d,%d,%d\n", lm.initLat[id].UnixNano(), lm.fillLat[id].UnixNano(), lm.persLat[id].UnixNano())
	if err != nil {
		return false, err
	}
	lm.hold[id] = false
	return true, nil
}

func (lm *LatencyMeasure) close() {
	lm.latOut.Close()
}
