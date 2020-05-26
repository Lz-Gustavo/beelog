package main

import (
	"fmt"
	"math/rand"
	"runtime"
	"sync"
	"testing"
	"time"
)

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

		if debugOutput {
			t.Log("Init:\n", l.Str())
		}

		_, err = ApplyReduceAlgo(l, tc.alg, 0, tc.nCmds-1)
		if err != nil {
			t.Log("test", tc.name, "failed with err:", err.Error())
			t.FailNow()
		}

		t.Log("Removed commands:", tc.nCmds-1-l.Len())
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

// run with:
// go test -run none -bench BenchmarkAlgosThroughput -benchtime 1ns -benchmem -v
func BenchmarkAlgosThroughput(b *testing.B) {

	b.SetParallelism(runtime.NumCPU())
	numCommands, diffKeys, writePercent := 100000, 100, 50
	log := make(chan KVCommand, numCommands)

	// dummy goroutine that creates a random log of commands
	go createRandomLog(numCommands, diffKeys, writePercent, log)

	// deploy the different workers, each implementing a diff recov protocol
	chA := make(chan KVCommand, 0)
	chB := make(chan KVCommand, 0)
	chC := make(chan KVCommand, 0)

	wg := &sync.WaitGroup{}
	mu := &sync.Mutex{}
	wg.Add(3)

	go runAlgorithm(GreedyAvl, chA, numCommands, mu, wg)
	go runAlgorithm(IterBFSAvl, chB, numCommands, mu, wg)
	go runAlgorithm(IterDFSAvl, chC, numCommands, mu, wg)

	// fan-out that output to the different goroutines
	go splitIntoWorkers(log, chA, chB, chC)

	// close the input log channel once all algorithms are executed
	wg.Wait()
	close(log)
}

func createRandomLog(n, dif, wrt int, out chan<- KVCommand) {

	srand := rand.NewSource(time.Now().UnixNano())
	r := rand.New(srand)

	for i := 0; i < n; i++ {
		cmd := KVCommand{
			key: r.Intn(dif),
		}

		// WRITE operation
		if cn := r.Intn(100); cn < wrt {
			cmd.value = r.Uint32()
			cmd.op = Write

		} else {
			cmd.op = Read
		}
		out <- cmd
	}

	// indicates the last command in the log, forcing consumer goroutines to halt
	out <- KVCommand{}
}

func splitIntoWorkers(src <-chan KVCommand, wrks ...chan<- KVCommand) {
	for {
		select {
		case cmd, ok := <-src:
			if !ok {
				return
			}
			for _, ch := range wrks {
				// avoid blocking receive on the sync ch
				go func(dest chan<- KVCommand, c KVCommand) {
					dest <- c
				}(ch, cmd)
			}
		}
	}
}

func runAlgorithm(alg Reducer, log <-chan KVCommand, n int, mu *sync.Mutex, wg *sync.WaitGroup) {

	avl := NewAVLTreeHT()
	i := 0
	defer wg.Done()

	start := time.Now()
	for {
		select {
		case cmd, ok := <-log:
			if !ok {
				return
			}

			if i < n {
				avl.Log(i, cmd)
				i++

			} else {
				// finished logging
				goto BREAK
			}
		}
	}

BREAK:
	// elapsed time to interpret the sequence of commands and construct the tree struct
	construct := time.Since(start)

	var (
		fn  string
		out []KVCommand
	)

	switch alg {
	case GreedyAvl:
		start = time.Now()
		out = GreedyAVLTreeHT(avl, 0, n)

		// elapsed time to recovery the entire log
		recov := time.Since(start)

		mu.Lock()
		defer mu.Unlock()

		fmt.Println(
			"\n====================",
			"\n=== RecurGreedy Benchmark",
			"\nConstruct Time:", construct,
			"\nRecovery Time:", recov,
			"\nRemoved cmds:", n-len(out),
			"\n====================",
		)
		fn = "output/recurgreedy-bench.out"
		break

	case IterBFSAvl:
		start = time.Now()
		out = IterBFSAVLTreeHT(avl, 0, n)

		// elapsed time to recovery the entire log
		recov := time.Since(start)

		mu.Lock()
		defer mu.Unlock()

		fmt.Println(
			"\n====================",
			"\n=== IterBFS Benchmark",
			"\nConstruct Time:", construct,
			"\nRecovery Time:", recov,
			"\nRemoved cmds:", n-len(out),
			"\n====================",
		)
		fn = "output/iterbfs-bench.out"
		break

	case IterDFSAvl:
		start = time.Now()
		out = IterDFSAVLTreeHT(avl, 0, n)

		// elapsed time to recovery the entire log
		recov := time.Since(start)

		mu.Lock()
		defer mu.Unlock()

		fmt.Println(
			"\n====================",
			"\n=== IterDFS Benchmark",
			"\nConstruct Time:", construct,
			"\nRecovery Time:", recov,
			"\nRemoved cmds:", n-len(out),
			"\n====================",
		)
		fn = "output/iterdfs-bench.out"
		break

	default:
		fmt.Println("unrecognized algorithm '", alg, "' provided")
		return
	}

	err := dumpLogIntoFile("./output/", fn, out)
	if err != nil {
		fmt.Println(err.Error())
	}
}
