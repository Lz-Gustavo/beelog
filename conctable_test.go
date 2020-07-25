package beelog

import "testing"

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
		// {
		// 	Inmem:   false,
		// 	KeepAll: true,
		// 	Alg:     IterConcTable,
		// 	Tick:    Interval,
		// 	Period:  100,
		// 	Fname:   "./logstate.out",
		// },
	}

	for _, cf := range cfgs {
		var st Structure
		var err error

		st, err = generateRandStructure(4, nCmds, wrt, dif, &cf)
		if err != nil {
			t.Log(err.Error())
			t.FailNow()
		}

		// TODO: call st.RecovEntireLog and check recv commands ...
		st.Str()
	}

	if err := cleanAllLogStates(); err != nil {
		t.Log(err.Error())
		t.FailNow()
	}
}
