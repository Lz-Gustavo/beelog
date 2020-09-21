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
	measureChance int = 1

	initArraySize = 10000000
)

type latData struct {
	init, fill, write, perst int64
}

// latencyMeasure holds auxiliar variables to implement an in-deep latency analysis
// on ConcTable operations.
type latencyMeasure struct {
	hold   []bool
	data   []latData
	latOut *os.File

	drawn    bool
	absIndex int
	msrIndex int
	interval int

	initLat  [initArraySize]int64
	writeLat [initArraySize]int64
	fillLat  [initArraySize]int64
	perstLat [initArraySize]int64
}

func newLatencyMeasure(concLvl, interval int, filename string) (*latencyMeasure, error) {
	fd, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_APPEND|os.O_TRUNC, 0644)
	if err != nil {
		return nil, err
	}

	return &latencyMeasure{
		hold:     make([]bool, concLvl, concLvl),
		data:     make([]latData, 0),
		interval: interval,
		latOut:   fd,
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

	lm.initLat[id] = time.Now().UnixNano()
	lm.hold[id] = true
	return true
}

func (lm *latencyMeasure) measureWriteLat(id int) bool {
	if !lm.hold[id] {
		return false
	}
	lm.writeLat[id] = time.Now().UnixNano()
	return true
}

func (lm *latencyMeasure) measureFillLat(id int) bool {
	if !lm.hold[id] {
		return false
	}
	lm.fillLat[id] = time.Now().UnixNano()
	return true
}

func (lm *latencyMeasure) measurePersLat(id int) bool {
	if !lm.hold[id] {
		return false
	}
	lm.perstLat[id] = time.Now().UnixNano()
	return true
}

func (lm *latencyMeasure) recordLatencyTuple(id int) (bool, error) {
	if !lm.hold[id] {
		return false, nil
	}

	lm.data = append(lm.data, latData{
		init:  lm.initLat[id],
		write: lm.writeLat[id],
		fill:  lm.fillLat[id],
		perst: lm.perstLat[id],
	})
	lm.hold[id] = false
	return true, nil
}

func (lm *latencyMeasure) flush() error {
	var err error
	buff := bytes.NewBuffer(nil)

	for i, init := range lm.initLat {
		w := lm.writeLat[i]
		f := lm.fillLat[i]
		p := lm.perstLat[i]

		// got the maximum number of unique-size tuples
		if init == 0 || w == 0 || f == 0 || p == 0 {
			break
		}

		_, err = fmt.Fprintf(buff, "%d,%d,%d,%d\n", init, w, f, p)
		if err != nil {
			return err
		}
	}

	_, err = buff.WriteTo(lm.latOut)
	if err != nil {
		return err
	}
	return nil
}

func (lm *latencyMeasure) flushDataSlice() error {
	var err error
	buff := bytes.NewBuffer(nil)

	for _, d := range lm.data {
		_, err = fmt.Fprintf(buff, "%d,%d,%d,%d\n", d.init, d.write, d.fill, d.perst)
		if err != nil {
			return err
		}
	}

	_, err = buff.WriteTo(lm.latOut)
	if err != nil {
		return err
	}
	return nil
}

func (lm *latencyMeasure) close() {
	lm.latOut.Close()
}
