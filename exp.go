package main

import (
	"github.com/BurntSushi/toml"
)

// TestCase ...
type TestCase struct {
	Name        string
	Struct      GenID
	NumCmds     int
	NumDiffKeys int
	Iterations  int
	Algo        []Reducer
}

func newTestCase(cfg []byte) (*TestCase, error) {
	tc := &TestCase{}
	err := toml.Unmarshal(cfg, tc)
	if err != nil {
		return nil, err
	}
	return tc, nil
}

func (tc *TestCase) run() error {

	gen := TranslateGen(tc.Struct)
	gen.Seed()

	for i := 0; i < tc.Iterations; i++ {
		st, err := gen.Gen(tc.NumCmds)
		if err != nil {
			return err
		}

		for _, a := range tc.Algo {
			err := ApplyReduceAlgo(st, a)
			if err != nil {
				return err
			}
		}
	}
	return nil
}
