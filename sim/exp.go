package main

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"

	bl "github.com/Lz-Gustavo/beelog"
	"github.com/Lz-Gustavo/beelog/pb"

	"github.com/BurntSushi/toml"
)

// TestCase reflects the .TOML input files, configuring experimental evaluation
// scenarios. If 'LogFile' is provided, the random parameters (NumCmds, PWrites,
// NDiffKeys) are ignored and the static log is parsed from the provided path.
type TestCase struct {
	Name          string
	Struct        StructID
	NumCmds       int
	PercentWrites int
	NumDiffKeys   int
	Iterations    int
	Algo          []bl.Reducer
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
		st  bl.Structure
		err error
		ln  int
	)
	hasInputLog := tc.LogFilename != ""

	for i := 0; i < tc.Iterations; i++ {
		if hasInputLog {
			cnt := TranslateConst(tc.Struct)
			st, ln, err = cnt(tc.LogFilename)
			if err != nil {
				return err
			}
			tc.NumCmds = ln

		} else {
			gen := TranslateGen(tc.Struct)
			st, err = gen(tc.NumCmds, tc.PercentWrites, tc.NumDiffKeys)
			if err != nil {
				return err
			}
		}

		for _, a := range tc.Algo {
			start := time.Now()
			log, err := bl.ApplyReduceAlgo(st, a, 0, uint64(tc.NumCmds-1))
			if err != nil {
				return err
			}

			if err = tc.output(i, a, time.Since(start), log); err != nil {
				fmt.Println("error encountered during log output:", err.Error(), ", ignoring...")
				continue
			}
		}
	}
	return nil
}

func (tc *TestCase) output(ind int, alg bl.Reducer, dur time.Duration, log []pb.Command) error {
	fmt.Println(
		"\n====================",
		"\n====", tc.Name,
		"\nIteration:", ind,
		"\nAlgorithm:", alg,
		"\nRemoved cmds:", tc.NumCmds-len(log),
		"\nDuration:", dur.String(),
		"\n====================",
	)

	outF := "./output/"
	fn := outF + tc.Name + "-iteration-" + strconv.Itoa(ind) + "-alg-" + strconv.Itoa(int(alg)) + ".out"

	err := dumpLogIntoFile(outF, fn, log)
	if err != nil {
		return err
	}
	return nil
}

func dumpLogIntoFile(folder, name string, log []pb.Command) error {
	if _, exists := os.Stat(folder); os.IsNotExist(exists) {
		os.Mkdir(folder, 0744)
	}

	out, err := os.OpenFile(name, os.O_CREATE|os.O_WRONLY, 0744)
	if err != nil {
		return err
	}
	defer out.Close()

	for _, cmd := range log {
		_, err = fmt.Fprintf(out, "%d %s %v\n", cmd.Op, cmd.Key, cmd.Value)
		if err != nil {
			return err
		}
	}
	return nil
}
