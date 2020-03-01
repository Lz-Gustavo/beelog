package main

import (
	"github.com/BurntSushi/toml"
)

// TestCase ...
type TestCase struct {
	Struct     GenID
	NumCmds    int
	Iterations int
	Algo       []Reducer
}

func newTestCase(cfg []byte) (*TestCase, error) {
	tc := &TestCase{}
	err := toml.Unmarshal(cfg, tc)
	if err != nil {
		return nil, err
	}
	return tc, nil
}

func (tc *TestCase) run() {
}
