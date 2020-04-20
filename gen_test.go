package main

import "testing"

func TestList(t *testing.T) {
	lg := &ListGenerator{}
	lg.Seed()
	l, err := lg.Gen(100, 50, 100)
	if err != nil {
		t.Log(err.Error())
		t.FailNow()
	}
	t.Log(l.Str())
}

func TestInit(t *testing.T) {
	fs, err := readCurrentDir()
	if err != nil {
		t.Log(err.Error())
		t.FailNow()
	}

	err = initTestCases(fs)
	if err != nil {
		t.Log(err.Error())
		t.FailNow()
	}
}

func TestReduceAlgos(t *testing.T) {

	debugOutput := false
	testCases := []struct {
		name     string
		nCmds    int
		pWrts    int
		diffKeys int
		alg      Reducer
	}{
		{
			"Case1",
			20,
			100,
			5,
			Bubbler,
		},
		{
			"Case2",
			20,
			100,
			5,
			Greedy,
		},
		{
			"Case3",
			20000000,
			90,
			10000,
			Bubbler,
		},
		{
			"Case4",
			20000,
			90,
			10000,
			Greedy,
		},
	}

	for _, tc := range testCases {
		lg := &ListGenerator{}
		lg.Seed()
		l, err := lg.Gen(tc.nCmds, tc.pWrts, tc.diffKeys)
		if err != nil {
			t.Log("test", tc.name, "failed with err:", err.Error())
			t.FailNow()
		}

		ln := l.Len()
		if debugOutput {
			t.Log("Init:\n", l.Str())
		}

		err = ApplyReduceAlgo(l, tc.alg)
		if err != nil {
			t.Log("test", tc.name, "failed with err:", err.Error())
			t.FailNow()
		}

		t.Log("Removed commands:", ln-l.Len())
		if debugOutput {
			t.Log("After Reduce:\n", l.Str())
		}
	}
}
