package beelog

import (
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/Lz-Gustavo/beelog/pb"
)

func TestListAlgos(t *testing.T) {
	debugOutput := false
	testCases := []struct {
		nCmds    uint64
		pWrts    int
		diffKeys int
		alg      Reducer
	}{
		{
			20,
			100,
			5,
			GreedyLt,
		},
		{
			2000,
			90,
			1000,
			GreedyLt,
		},
	}

	for i, tc := range testCases {
		l, err := generateRandStructure(0, tc.nCmds, tc.pWrts, tc.diffKeys, nil)
		if err != nil {
			t.Log("test num", i, "failed with err:", err.Error())
			t.FailNow()
		}

		if debugOutput {
			t.Log("Init with", l.Len(), "commands:\n", l.Str())
		}

		log, err := ApplyReduceAlgo(l, tc.alg, 0, l.Len())
		if err != nil {
			t.Log("test num", i, "failed with err:", err.Error())
			t.FailNow()
		}

		if debugOutput {
			t.Log("Reduced Log:\n", log)
			t.Log("Removed commands:", l.Len()-uint64(len(log)))
		}
		// TODO: implement a validation procedure...
	}
}

func TestArrayAlgos(t *testing.T) {
	debugOutput := false
	testCases := []struct {
		nCmds    uint64
		pWrts    int
		diffKeys int
		alg      Reducer
	}{
		{
			20,
			100,
			5,
			GreedyArray,
		},
		{
			2000,
			90,
			1000,
			GreedyArray,
		},
	}

	for i, tc := range testCases {
		ar, err := generateRandStructure(1, tc.nCmds, tc.pWrts, tc.diffKeys, nil)
		if err != nil {
			t.Log("test num", i, "failed with err:", err.Error())
			t.FailNow()
		}

		if debugOutput {
			t.Log("Init with", ar.Len(), "commands:\n", ar.Str())
		}

		log, err := ApplyReduceAlgo(ar, tc.alg, 0, ar.Len())
		if err != nil {
			t.Log("test num", i, "failed with err:", err.Error())
			t.FailNow()
		}

		if debugOutput {
			t.Log("Reduced Log:\n", log)
			t.Log("Removed commands:", ar.Len()-uint64(len(log)))
		}
		// TODO: implement a validation procedure...
	}
}

func TestAVLTreeAlgos(t *testing.T) {
	debugOutput := false
	testCases := []struct {
		numCmds      uint64
		writePercent int
		diffKeys     int
		p, n         uint64
	}{
		{
			20,
			100,
			5,
			0,
			20,
		},
		{
			2000,
			50,
			100,
			0,
			2000,
		},
	}

	log := []pb.Command{}
	for _, tc := range testCases {
		avl, err := generateRandStructure(2, tc.numCmds, tc.writePercent, tc.diffKeys, nil)
		if err != nil {
			t.Log(err.Error())
			t.FailNow()
		}
		if debugOutput {
			t.Logf("Tree structure:\n %s \n", avl.Str())
		}

		log, err = ApplyReduceAlgo(avl, GreedyAvl, tc.p, tc.n)
		if err != nil {
			t.Log(err.Error())
			t.FailNow()
		}
		greedyRmv := tc.numCmds - uint64(len(log))
		if debugOutput {
			t.Logf("GreedyAvl log:\n %v \n", log)
			t.Log("Removed", greedyRmv, "comands")
		}

		log, err = ApplyReduceAlgo(avl, IterBFSAvl, tc.p, tc.n)
		if err != nil {
			t.Log(err.Error())
			t.FailNow()
		}
		bfsRmv := tc.numCmds - uint64(len(log))
		if debugOutput {
			t.Logf("IterBFSAvl log:\n %v \n", log)
			t.Log("Removed", bfsRmv, "comands")
		}

		log, err = ApplyReduceAlgo(avl, IterDFSAvl, tc.p, tc.n)
		if err != nil {
			t.Log(err.Error())
			t.FailNow()
		}
		dfsRmv := tc.numCmds - uint64(len(log))
		if debugOutput {
			t.Logf("IterDFSAvl log:\n %v \n", log)
			t.Log("Removed", dfsRmv, "comands")
		}

		if greedyRmv != bfsRmv || greedyRmv != dfsRmv || bfsRmv != dfsRmv {
			t.Log("Different algorithms presented different results, incoherent")
			t.FailNow()
		}
	}
}

func TestCircBuffAlgos(t *testing.T) {
	debugOutput := false
	testCases := []struct {
		nCmds    uint64
		pWrts    int
		diffKeys int
		alg      Reducer
	}{
		{
			20,
			100,
			5,
			IterCircBuff,
		},
		{
			2000,
			90,
			1000,
			IterCircBuff,
		},
	}

	for i, tc := range testCases {
		buf, err := generateRandStructure(3, tc.nCmds, tc.pWrts, tc.diffKeys, nil)
		if err != nil {
			t.Log("test num", i, "failed with err:", err.Error())
			t.FailNow()
		}

		if debugOutput {
			t.Log("Init with", buf.Len(), "commands:\n", buf.Str())
		}

		log, err := ApplyReduceAlgo(buf, tc.alg, 0, buf.Len())
		if err != nil {
			t.Log("test num", i, "failed with err:", err.Error())
			t.FailNow()
		}

		if debugOutput {
			t.Log("Reduced Log:\n", log)
			t.Log("Removed commands:", buf.Len()-uint64(len(log)))
		}
		// TODO: implement a validation procedure...
	}
}

func TestConcTableAlgos(t *testing.T) {
	debugOutput := false
	testCases := []struct {
		nCmds    uint64
		pWrts    int
		diffKeys int
		alg      Reducer
	}{
		{
			20,
			100,
			5,
			IterConcTable,
		},
		{
			2000,
			90,
			1000,
			IterConcTable,
		},
	}

	for i, tc := range testCases {
		tbl, err := generateRandStructure(4, tc.nCmds, tc.pWrts, tc.diffKeys, nil)
		if err != nil {
			t.Log("test num", i, "failed with err:", err.Error())
			t.FailNow()
		}

		if debugOutput {
			t.Log("Init with", tbl.Len(), "commands:\n", tbl.Str())
		}

		log, err := ApplyReduceAlgo(tbl, tc.alg, 0, tbl.Len())
		if err != nil {
			t.Log("test num", i, "failed with err:", err.Error())
			t.FailNow()
		}

		if debugOutput {
			t.Log("Reduced Log:\n", log)
			t.Log("Removed commands:", tbl.Len()-uint64(len(log)))
		}
		// TODO: implement a validation procedure...
	}
}

func BenchmarkAVLTreeAlgos(b *testing.B) {
	scenarios := []struct {
		numCmds      uint64
		writePercent int
		diffKeys     int
		p, n         uint64
	}{
		{
			1000,
			50,
			100,
			0,
			1000,
		},
		{
			10000,
			50,
			1000,
			0,
			10000,
		},
		{
			100000,
			50,
			10000,
			5000,
			12000,
		},
	}

	for i := 0; i < b.N; i++ {
		for _, sc := range scenarios {
			st, err := generateRandStructure(2, sc.numCmds, sc.writePercent, sc.diffKeys, nil)
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

// Dear dev, avoid crash on your IDE by running with:
// go test -run none -bench BenchmarkAlgosThroughput -benchtime 1ns -benchmem -v
func BenchmarkAlgosThroughput(b *testing.B) {

	b.SetParallelism(runtime.NumCPU())
	numCommands, diffKeys, writePercent := uint64(1000000), 1000, 50
	log := make(chan pb.Command, numCommands)

	// dummy goroutine that creates a random log of commands
	go createRandomLog(numCommands, diffKeys, writePercent, log)

	// deploy the different workers, each implementing a diff recov protocol
	chA := make(chan pb.Command, 0)
	chB := make(chan pb.Command, 0)
	chC := make(chan pb.Command, 0)
	chD := make(chan pb.Command, 0)

	wg := &sync.WaitGroup{}
	mu := &sync.Mutex{}
	wg.Add(4)

	go runTraditionalLog(chD, numCommands, mu, wg)
	go runAlgorithm(GreedyAvl, chA, numCommands, mu, wg)
	go runAlgorithm(IterBFSAvl, chB, numCommands, mu, wg)
	go runAlgorithm(IterDFSAvl, chC, numCommands, mu, wg)

	// fan-out that output to the different goroutines
	go splitIntoWorkers(log, chA, chB, chC, chD)

	// close the input log channel once all algorithms are executed
	wg.Wait()
	close(log)
}

func createRandomLog(n uint64, dif, wrt int, out chan<- pb.Command) {
	srand := rand.NewSource(time.Now().UnixNano())
	r := rand.New(srand)

	for i := uint64(0); i < n; i++ {
		cmd := pb.Command{
			Id:  i,
			Key: strconv.Itoa(r.Intn(dif)),
		}

		// WRITE operation
		if cn := r.Intn(100); cn < wrt {
			cmd.Value = strconv.Itoa(r.Int())
			cmd.Op = pb.Command_SET

		} else {
			cmd.Op = pb.Command_GET
		}
		out <- cmd
	}

	// indicates the last command in the log, forcing consumer goroutines to halt
	out <- pb.Command{}
}

func splitIntoWorkers(src <-chan pb.Command, wrks ...chan<- pb.Command) {
	for {
		select {
		case cmd, ok := <-src:
			if !ok {
				return
			}
			for _, ch := range wrks {
				// avoid blocking receive on the sync ch
				go func(dest chan<- pb.Command, c pb.Command) {
					dest <- c
				}(ch, cmd)
			}
		}
	}
}

func runAlgorithm(alg Reducer, log <-chan pb.Command, n uint64, mu *sync.Mutex, wg *sync.WaitGroup) {
	avl := NewAVLTreeHT()
	var i uint64
	defer wg.Done()

	start := time.Now()
	for {
		select {
		case cmd, ok := <-log:
			if !ok {
				return
			}

			if i < n {
				avl.Log(cmd)
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
		fn, id string
		out    []pb.Command

		// elapsed time to recovery the entire log
		recov time.Duration
	)

	switch alg {
	case GreedyAvl:
		start = time.Now()
		out = GreedyAVLTreeHT(avl, 0, n)
		recov = time.Since(start)

		id = "RecurGreedy Benchmark"
		fn = "recurgreedy-bench.out"
		break

	case IterBFSAvl:
		start = time.Now()
		out = IterBFSAVLTreeHT(avl, 0, n)
		recov = time.Since(start)

		id = "IterBFS Benchmark"
		fn = "iterbfs-bench.out"
		break

	case IterDFSAvl:
		start = time.Now()
		out = IterDFSAVLTreeHT(avl, 0, n)
		recov = time.Since(start)

		id = "IterDFS Benchmark"
		fn = "iterdfs-bench.out"
		break

	default:
		fmt.Println("unrecognized algorithm '", alg, "' provided")
		return
	}

	start = time.Now()
	err := dumpLogIntoFile("./", fn, out)
	if err != nil {
		fmt.Println(err.Error())
	}
	dump := time.Since(start)

	mu.Lock()
	fmt.Println(
		"\n====================",
		"\n===", id,
		"\nRemoved cmds: ", n-uint64(len(out)),
		"\nConstruction Time: ", construct,
		"\nCompactation Time: ", recov,
		"\nInstallation Time:", dump,
		"\n====================",
	)
	mu.Unlock()
}

func runTraditionalLog(log <-chan pb.Command, n uint64, mu *sync.Mutex, wg *sync.WaitGroup) {
	logfile := make([]pb.Command, 0, n)
	var i uint64
	defer wg.Done()

	start := time.Now()
	for {
		select {
		case cmd, ok := <-log:
			if !ok {
				return
			}

			if i < n {
				logfile = append(logfile, cmd)
				i++

			} else {
				// finished logging
				goto BREAK
			}
		}
	}

BREAK:
	construct := time.Since(start)
	fn := "traditionallog-bench.out"

	start = time.Now()
	err := dumpLogIntoFile("./", fn, logfile)
	if err != nil {
		fmt.Println(err.Error())
	}
	dump := time.Since(start)

	mu.Lock()
	fmt.Println(
		"\n====================",
		"\n=== Traditional Log Benchmark",
		"\nRemoved cmds:", n-uint64(len(logfile)),
		"\nConstruction Time:", construct,
		"\nCompactation Time: -",
		"\nInstallation Time:", dump,
		"\n====================",
	)
	mu.Unlock()
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
