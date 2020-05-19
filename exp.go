package main

import (
	"errors"

	"github.com/BurntSushi/toml"
)

// TestCase ...
type TestCase struct {
	Name          string
	Struct        GenID
	NumCmds       int
	PercentWrites int
	NumDiffKeys   int
	Iterations    int
	Algo          []Reducer
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
	if tc.Struct != LogDAG && tc.Struct != LogList {
		return errors.New("unknow log structure")
	}
	if len(tc.Algo) < 1 {
		return errors.New("no reduce algorithm provided")
	}
	return nil
}

func (tc *TestCase) run() error {

	gen := TranslateGen(tc.Struct)
	for i := 0; i < tc.Iterations; i++ {
		st, err := gen(tc.NumCmds, tc.PercentWrites, tc.NumDiffKeys)
		if err != nil {
			return err
		}

		for _, a := range tc.Algo {
			_, err := ApplyReduceAlgo(st, a, 0, st.Len()-1)
			if err != nil {
				return err
			}
		}
	}
	return nil
}
