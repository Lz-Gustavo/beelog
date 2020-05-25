package main

import "testing"

func TestListAlgos(t *testing.T) {

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
			BubblerLt,
		},
		{
			"Case2",
			20,
			100,
			5,
			GreedyLt,
		},
		{
			"Case3",
			20000000,
			90,
			10000,
			BubblerLt,
		},
		{
			"Case4",
			20000,
			90,
			10000,
			GreedyLt,
		},
	}

	for _, tc := range testCases {
		l, err := ListGen(tc.nCmds, tc.pWrts, tc.diffKeys)
		if err != nil {
			t.Log("test", tc.name, "failed with err:", err.Error())
			t.FailNow()
		}

		ln := l.Len()
		if debugOutput {
			t.Log("Init:\n", l.Str())
		}

		_, err = ApplyReduceAlgo(l, tc.alg, 0, ln-1)
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

func TestAVLTreeAlgos(t *testing.T) {

	testCases := []struct {
		numCmds      int
		writePercent int
		diffKeys     int
		p, n         int
	}{
		{
			20,
			100,
			5,
			0,
			20,
		},
		{
			1000,
			50,
			100,
			0,
			1000,
		},
		{
			1000000,
			50,
			1000,
			0,
			1000000,
		},
		{
			1000000,
			50,
			1000,
			5000,
			12000,
		},
	}

	log := []KVCommand{}
	for _, tc := range testCases {
		avl, err := AVLTreeHTGen(tc.numCmds, tc.writePercent, tc.diffKeys)
		if err != nil {
			t.Log(err.Error())
			t.FailNow()
		}
		t.Logf("Tree structure:\n %s \n", avl.Str())

		log, err = ApplyReduceAlgo(avl, GreedyAvl, tc.p, tc.n)
		if err != nil {
			t.Log(err.Error())
			t.FailNow()
		}
		t.Logf("GreedyAvl log:\n %v \n", log)
		t.Log("Removed", tc.numCmds-len(log), "comands")

		log, err = ApplyReduceAlgo(avl, IterBFSAvl, tc.p, tc.n)
		if err != nil {
			t.Log(err.Error())
			t.FailNow()
		}
		t.Logf("IterBFSAvl log:\n %v \n", log)
		t.Log("Removed", tc.numCmds-len(log), "comands")

		log, err = ApplyReduceAlgo(avl, IterDFSAvl, tc.p, tc.n)
		if err != nil {
			t.Log(err.Error())
			t.FailNow()
		}
		t.Logf("IterDFSAvl log:\n %v \n", log)
		t.Log("Removed", tc.numCmds-len(log), "comands")
	}
}

func BenchmarkAVLTreeAlgos(b *testing.B) {

	scenarios := []struct {
		numCmds      int
		writePercent int
		diffKeys     int
		p, n         int
	}{
		{
			1000,
			50,
			100,
			0,
			1000,
		},
		{
			1000000,
			50,
			1000,
			0,
			1000000,
		},
		{
			1000000,
			50,
			1000,
			5000,
			12000,
		},
		{
			10000000,
			50,
			1000,
			0,
			10000000,
		},
	}

	for i := 0; i < b.N; i++ {
		for _, sc := range scenarios {
			st, err := AVLTreeHTGen(sc.numCmds, sc.writePercent, sc.diffKeys)
			if err != nil {
				b.Log(err.Error())
				b.FailNow()
			}
			avl := st.(*AVLTreeHT)

			b.ResetTimer()
			b.Run("GreedyAvl", func(b *testing.B) {
				GreedyAVLTreeHT(avl, sc.p, sc.n)
			})

			b.ResetTimer()
			b.Run("BFS-IterAvl", func(b *testing.B) {
				IterBFSAVLTreeHT(avl, sc.p, sc.n)
			})

			b.ResetTimer()
			b.Run("DFS-IterAvl", func(b *testing.B) {
				IterDFSAVLTreeHT(avl, sc.p, sc.n)
			})
			b.StopTimer()
		}
	}
}
