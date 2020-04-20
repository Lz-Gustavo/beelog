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
