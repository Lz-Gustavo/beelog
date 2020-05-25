package main

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/BurntSushi/toml"
)

// TestCase ...
type TestCase struct {
	Name          string
	Struct        StructID
	NumCmds       int
	PercentWrites int
	NumDiffKeys   int
	Iterations    int
	Algo          []Reducer
	LogFilename   string
}

func newTestCase(cfg []byte) (*TestCase, error) {
	tc := &TestCase{}
	err := toml.Unmarshal(cfg, tc)
	if err != nil {
		return nil, err
	}
	if validateTestCase(tc); err != nil {
		return nil, err
	}
	return tc, nil
}

func validateTestCase(tc *TestCase) error {
	if tc.NumCmds < 0 || tc.NumDiffKeys < 0 || tc.Iterations < 0 {
		return errors.New("negative config number")
	}
	if tc.PercentWrites < 0 || tc.PercentWrites > 100 {
		return errors.New("invalid write percentage value")
	}
	if tc.Struct != LogDAG && tc.Struct != LogList && tc.Struct != LogAVL {
		return errors.New("unknow log structure")
	}
	if len(tc.Algo) < 1 {
		return errors.New("no reduce algorithm provided")
	}
	return nil
}

func (tc *TestCase) run() error {
	var (
		st  Structure
		err error
	)
	hasInputLog := tc.LogFilename != ""

	for i := 0; i < tc.Iterations; i++ {
		if hasInputLog {
			cnt := TranslateConst(tc.Struct)
			st, err = cnt(tc.LogFilename)
			if err != nil {
				return err
			}
			tc.NumCmds = st.Len()

		} else {
			gen := TranslateGen(tc.Struct)
			st, err = gen(tc.NumCmds, tc.PercentWrites, tc.NumDiffKeys)
			if err != nil {
				return err
			}
		}

		for _, a := range tc.Algo {
			start := time.Now()
			log, err := ApplyReduceAlgo(st, a, 0, st.Len()-1)
			if err != nil {
				return err
			}
			tc.output(i, a, time.Since(start), log)
		}
	}
	return nil
}

func (tc *TestCase) output(ind int, alg Reducer, dur time.Duration, log []KVCommand) error {
	fmt.Println(
		"\n==========",
		"\nIteration:", ind,
		"\nAlgorithm:", alg,
		"\nRemoved cmds:", tc.NumCmds-len(log),
		"\nDuration:", dur.String(),
		"\n==========",
	)

	fname := tc.Name + "-iteration-" + strconv.Itoa(ind) + "-alg-" + strconv.Itoa(int(alg)) + ".out"
	out, err := os.Create(fname)
	if err != nil {
		return err
	}
	defer out.Close()

	_, err = fmt.Fprintf(out, "%v", log)
	if err != nil {
		return err
	}
	return nil
}
