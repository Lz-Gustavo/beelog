package main

import "testing"

func TestInit(t *testing.T) {
	fs, err := parseDir("./input/")
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

func TestListGen(t *testing.T) {
	l, err := ListGen(100, 50, 100)
	if err != nil {
		t.Log(err.Error())
		t.FailNow()
	}
	t.Log(l.Str())
}

func TestAVLTreeHTGen(t *testing.T) {
	avl, err := AVLTreeHTGen(100, 50, 100)
	if err != nil {
		t.Log(err.Error())
		t.FailNow()
	}
	t.Log(avl.Str())
}

func TestAVLTreeHTConst(t *testing.T) {
	avl, _, err := AVLTreeHTConst("input/logavl-a.log")
	if err != nil {
		t.Log(err.Error())
		t.FailNow()
	}
	t.Log(avl.Str())
}
