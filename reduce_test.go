package main

import "testing"

func TestAVLTreeAlgos(t *testing.T) {

	testCases := []struct {
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
	}

	log := []KVCommand{}
	for _, tc := range testCases {
		avl, err := AVLTreeHTGen(tc.numCmds, tc.writePercent, tc.diffKeys)
		if err != nil {
			t.Log(err.Error())
			t.FailNow()
		}
		t.Logf("Tree structure:\n %s \n", avl.Str())

		log, err = ApplyReduceAlgo(avl, GreedyB1, tc.p, tc.n)
		if err != nil {
			t.Log(err.Error())
			t.FailNow()
		}
		t.Logf("GreedyB1 log:\n %v \n", log)
		t.Log("Removed", tc.numCmds-len(log), "comands")

		log, err = ApplyReduceAlgo(avl, IterB1, tc.p, tc.n)
		if err != nil {
			t.Log(err.Error())
			t.FailNow()
		}
		t.Logf("IterB1 log:\n %v \n", log)
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
			b.Run("GreedyB1", func(b *testing.B) {
				GreedyB1AVLTreeHT(avl, sc.p, sc.n)
			})

			b.ResetTimer()
			b.Run("IterB1", func(b *testing.B) {
				IterB1AVLTreeHT(avl, sc.p, sc.n)
			})

			b.ResetTimer()
			b.Run("Slices-IterB1", func(b *testing.B) {
				IterB1AVLTreeHTWithSlice(avl, sc.p, sc.n)
			})
			b.StopTimer()
		}
	}
}

func BenchmarkGreedyB1Algorithm(b *testing.B) {

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
			b.StartTimer()
			GreedyB1AVLTreeHT(avl, sc.p, sc.n)
			b.StopTimer()
		}
	}
}

func BenchmarkIterB1Algorithm(b *testing.B) {

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
			b.StartTimer()
			IterB1AVLTreeHT(avl, sc.p, sc.n)
			b.StopTimer()
		}
	}
}
