package beelog

import (
	"fmt"
	"io"
	"testing"
)

func TestConcTableRecovEntireLog(t *testing.T) {
	nCmds, wrt, dif := uint64(2000), 50, 100
	//p, n := uint64(100), uint64(1500)

	cfgs := []LogConfig{
		// {
		// 	Inmem:   false,
		// 	KeepAll: true,
		// 	Alg:     IterConcTable,
		// 	Tick:    Delayed,
		// 	Fname:   "./logstate.out",
		// },
		{
			Inmem:   false,
			KeepAll: true,
			Alg:     IterConcTable,
			Tick:    Interval,
			Period:  100,
			Fname:   "./logstate.out",
		},
	}

	for _, cf := range cfgs {
		var st Structure
		var err error

		// clean state before creating
		if err := cleanAllLogStates(); err != nil {
			t.Log(err.Error())
			t.FailNow()
		}

		st, err = generateRandStructure(4, nCmds, wrt, dif, &cf)
		if err != nil {
			t.Log(err.Error())
			t.FailNow()
		}

		ch, err := st.(*ConcTable).RecovEntireLog()
		if err != nil {
			t.Log(err.Error())
			t.FailNow()
		}
		fmt.Println("now reading ch...")

		for {
			raw, ok := <-ch
			if !ok { // closed ch
				fmt.Println("closed!")
				break
			}
			fmt.Println("got one, deserializing")

			log, err := deserializeRawLog(raw)
			if err == io.EOF {
				t.Log("empty log")
				t.Fail()

			} else if err != nil {
				t.Log("error while deserializing log, err:", err.Error())
				t.FailNow()
			}

			fmt.Println("got log:", log)
			// TODO: test log...
		}
	}

	if err := cleanAllLogStates(); err != nil {
		t.Log(err.Error())
		t.FailNow()
	}
}
