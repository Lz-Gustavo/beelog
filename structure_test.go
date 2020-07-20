package beelog

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"math/rand"
	"os"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/Lz-Gustavo/beelog/pb"

	"github.com/golang/protobuf/proto"
)

func TestStructuresLog(t *testing.T) {
	first := uint64(1)
	n := uint64(1000)

	avl := NewAVLTreeHT()
	lt := NewListHT()
	arr := NewArrayHT()
	buf := NewCircBuffHT(context.TODO())
	ct := NewConcTable(context.TODO())

	for _, st := range []Structure{lt, arr, avl, buf, ct} {
		// populate some SET commands
		for i := first; i < n; i++ {
			// TODO: yeah, I will change this API of index log in time
			err := st.Log(i, pb.Command{Id: i, Op: pb.Command_SET, Key: strconv.Itoa(int(i))})
			if err != nil {
				t.Log(err.Error())
				t.FailNow()
			}
		}
		l := st.Len()
		if l != n-first {
			t.Log(l, "commands, expected", n-first)
			t.FailNow()
		}

		// log another GET
		err := st.Log(n, pb.Command{Id: n, Op: pb.Command_GET})
		if err != nil {
			t.Log(err.Error())
			t.FailNow()
		}

		// GET command shouldnt increase size, but modify 'avl.last' index
		if l != st.Len() {
			t.Log(l, "commands, expected", st.Len())
			t.FailNow()
		}

		switch tp := st.(type) {
		case *ListHT:
			if tp.first != first {
				// indexes should be 'first' and 'n', considering n-1 SETS + 1 GET
				t.Log("first cmd index is", tp.first, ", expected", first)
				t.FailNow()
			}
			if tp.last != n {
				t.Log("last cmd index is", tp.last, ", expected", n)
				t.FailNow()
			}
			break

		case *ArrayHT:
			// cant assign multiple types on the same case statement, since Structure
			// types are inconvertible between them.
			if tp.first != first {
				t.Log("first cmd index is", tp.first, ", expected", first)
				t.FailNow()
			}
			if tp.last != n {
				t.Log("last cmd index is", tp.last, ", expected", n)
				t.FailNow()
			}
			break

		case *AVLTreeHT:
			if tp.first != first {
				t.Log("first cmd index is", tp.first, ", expected", first)
				t.FailNow()
			}
			if tp.last != n {
				t.Log("last cmd index is", tp.last, ", expected", n)
				t.FailNow()
			}
			break

		case *CircBuffHT:
			if tp.first != first {
				t.Log("first cmd index is", tp.first, ", expected", first)
				t.FailNow()
			}
			if tp.last != n {
				t.Log("last cmd index is", tp.last, ", expected", n)
				t.FailNow()
			}
			break

		case *ConcTable:
			if tp.logs[tp.current].first != first {
				t.Log("first cmd index is", tp.logs[tp.current].first, ", expected", first)
				t.FailNow()
			}
			if tp.logs[tp.current].last != n {
				t.Log("last cmd index is", tp.logs[tp.current].last, ", expected", n)
				t.FailNow()
			}
			break

		default:
			t.Logf("unknown structure type '%T' informed", tp)
			t.FailNow()
		}
	}
}

func TestStructuresDifferentRecoveries(t *testing.T) {
	// Requesting the last matching index (i.e. n == nCmds) is mandatory
	// on Immediately and Interval configurations.
	nCmds, wrt, dif := uint64(2000), 50, 10
	p, n := uint64(10), uint64(2000)

	// For now, interval config is intentionally delayed to allow a state
	// comparison between the different routines.
	//
	// TODO: Implement an exclusive test procedure for intervalar reduce.
	cfgs := []LogConfig{
		{ // immediately inmem
			Tick:  Immediately,
			Inmem: true,
		},
		{ // delayed inmem
			Tick:  Delayed,
			Inmem: true,
		},
		{ // interval inmem
			Tick:   Interval,
			Period: 10000,
			Inmem:  true,
		},
		{ // immediately disk
			Tick:  Immediately,
			Inmem: false,
			Fname: "./logstate.out",
		},
		{ // delayed disk
			Tick:  Delayed,
			Inmem: false,
			Fname: "./logstate.out",
		},
		{ // interval disk
			Tick:   Interval,
			Period: 10000,
			Inmem:  false,
			Fname:  "./logstate.out",
		},
	}

	testCases := []struct {
		structID uint8
		Alg      Reducer
		configs  []LogConfig
	}{
		{
			0, // list
			GreedyLt,
			cfgs,
		},
		{
			1, // array
			GreedyArray,
			cfgs,
		},
		{
			2, // avltree
			IterDFSAvl,
			cfgs,
		},
		{
			3, // circbuff
			IterCircBuff,
			cfgs,
		},
		{
			4, // conctable
			IterConcTable,
			cfgs,
		},
	}
	structNames := []string{"List", "Array", "AVLTree", "CircBuff", "ConcTable"}

	for _, tc := range testCases {
		for j, cf := range tc.configs {

			// delete current logstate, if any, in order to avoid conflict on
			// persistent interval scenarios
			if cf.Tick == Interval && cf.Fname != "" {
				if err := resetLogStates(cf.Fname); err != nil {
					t.Log(err.Error())
					t.FailNow()
				}
			}

			var st Structure
			var err error

			// set the current test case algorithm
			cf.Alg = tc.Alg

			// Currently logs must be re-generated for each different config, which
			// prohibits the generation of an unique log file for all configs. An
			// unique log is not needed on a unit test scenario, since the idea is
			// not to compare these strategies, only tests its correctness.
			st, err = generateRandStructure(tc.structID, nCmds, wrt, dif, &cf)
			if err != nil {
				t.Log(err.Error())
				t.FailNow()
			}
			t.Log("====Executing", structNames[tc.structID], "Test Case #", j)

			// the compacted log used for later comparison
			redLog, err := ApplyReduceAlgo(st, cf.Alg, p, n)
			if err != nil {
				t.Log(err.Error())
				t.FailNow()
			}

			// wait for new state attribution on concurrent structures...
			if tc.structID == 4 {
				time.Sleep(time.Second)
			}

			log, err := st.Recov(p, n)
			if err != nil {
				t.Log(err.Error())
				t.FailNow()
			}

			if !logsAreEquivalent(redLog, log) {
				t.Log("Logs are not equivalent")
				t.Log(redLog)
				t.Log(log)
				t.FailNow()
			}

			if len(redLog) == 0 {
				t.Log("Both logs are empty")
				t.Log(redLog)
				t.Log(log)
				t.FailNow()
			}
		}
	}
}

func TestStructuresRecovBytesInterpretation(t *testing.T) {
	nCmds, wrt, dif := uint64(2000), 50, 100
	p, n := uint64(100), uint64(1500)

	cfgs := []LogConfig{
		{ // inmem byte recov
			Tick:  Delayed,
			Inmem: true,
		},
		{ // disk byte recov
			Tick:  Delayed,
			Inmem: false,
			Fname: "./logstate.out",
		},
	}

	testCases := []struct {
		structID uint8
		Alg      Reducer
		configs  []LogConfig
	}{
		{
			0, // list
			GreedyLt,
			cfgs,
		},
		{
			1, // array
			GreedyArray,
			cfgs,
		},
		{
			2, // avltree
			IterDFSAvl,
			cfgs,
		},
		{
			3, // circbuff
			IterCircBuff,
			cfgs,
		},
		{
			4, // conctable
			IterConcTable,
			cfgs,
		},
	}
	structNames := []string{"List", "Array", "AVLTree", "CircBuff", "ConcTable"}

	for _, tc := range testCases {
		for j, cf := range tc.configs {
			var st Structure
			var err error

			// set the current test case algorithm
			cf.Alg = tc.Alg

			st, err = generateRandStructure(tc.structID, nCmds, wrt, dif, &cf)
			if err != nil {
				t.Log(err.Error())
				t.FailNow()
			}
			t.Log("====Executing", structNames[tc.structID], "Test Case #", j)

			// the compacted log used for later comparison
			redLog, err := ApplyReduceAlgo(st, cf.Alg, p, n)
			if err != nil {
				t.Log(err.Error())
				t.FailNow()
			}

			// wait for new state attribution on concurrent structures...
			if tc.structID == 4 {
				time.Sleep(time.Second)
			}

			raw, err := st.RecovBytes(p, n)
			if err != nil {
				t.Log(err.Error())
				t.FailNow()
			}

			log, err := deserializeRawLog(raw)
			if err != nil {
				t.Log(err.Error())
				t.FailNow()
			}

			if !logsAreEquivalent(redLog, log) {
				t.Log("Logs are not equivalent")
				t.Log(redLog)
				t.Log(log)
				t.FailNow()
			}

			if len(redLog) == 0 {
				t.Log("Both logs are empty")
				t.Log(redLog)
				t.Log(log)
				t.FailNow()
			}
		}
	}
}

func generateRandStructure(id uint8, n uint64, wrt, dif int, cfg *LogConfig) (Structure, error) {
	srand := rand.NewSource(time.Now().UnixNano())
	r := rand.New(srand)
	var st Structure
	var err error

	switch id {
	case 0: // list
		if cfg == nil {
			st = NewListHT() // uses DefaultLogConfig underneath
		} else {
			st, err = NewListHTWithConfig(cfg)
			if err != nil {
				return nil, err
			}
		}
		break

	case 1: // array
		if cfg == nil {
			st = NewArrayHT()
		} else {
			st, err = NewArrayHTWithConfig(cfg)
			if err != nil {
				return nil, err
			}
		}
		break

	case 2: // avl
		if cfg == nil {
			st = NewAVLTreeHT()
		} else {
			st, err = NewAVLTreeHTWithConfig(cfg)
			if err != nil {
				return nil, err
			}
		}
		break

	case 3: // circbuff
		if cfg == nil {
			st = NewCircBuffHT(context.TODO())
		} else {
			st, err = NewCircBuffHTWithConfig(context.TODO(), cfg, int(n))
			if err != nil {
				return nil, err
			}
		}
		break

	case 4: // conctable
		if cfg == nil {
			st = NewConcTable(context.TODO())
		} else {
			st, err = NewConcTableWithConfig(context.TODO(), cfg)
			if err != nil {
				return nil, err
			}
		}
		break

	default:
		return nil, fmt.Errorf("unknow structure '%d' requested", id)
	}

	for i := uint64(0); i < n; i++ {
		var cmd pb.Command
		if cn := r.Intn(100); cn < wrt {
			cmd = pb.Command{
				Id:    i,
				Key:   strconv.Itoa(r.Intn(dif)),
				Value: strconv.Itoa(r.Int()),
				Op:    pb.Command_SET,
			}

		} else {
			// only SETS states are needed
			cmd = pb.Command{
				Id: i,
				Op: pb.Command_GET,
			}
		}
		err = st.Log(i, cmd)
		if err != nil {
			return nil, err
		}
	}
	return st, nil
}

// deserializeRawLog emulates the same procedure implemented by a recoverying
// replica, interpreting the serialized log received from any byte stream.
func deserializeRawLog(log []byte) ([]pb.Command, error) {
	rd := bytes.NewReader(log)

	// read the retrieved log interval
	var f, l uint64
	_, err := fmt.Fscanf(rd, "%d\n%d\n", &f, &l)
	if err != nil {
		return nil, err
	}

	cmds := make([]pb.Command, 0, l-f)
	for {

		var commandLength int32
		err := binary.Read(rd, binary.BigEndian, &commandLength)
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}

		serializedCmd := make([]byte, commandLength)
		_, err = rd.Read(serializedCmd)
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}

		c := &pb.Command{}
		err = proto.Unmarshal(serializedCmd, c)
		if err != nil {
			return nil, err
		}
		cmds = append(cmds, *c)
	}
	return cmds, nil
}

// logsAreEquivalent checks if two command log sequences are equivalent with
// one another. In this context, two logs 'a' and 'b' are considered 'equivalent'
// iff the deterministic, sequentialy execution of key-value commands on 'b'
// yields the same final state observed on executing 'a'. Two recovered sequences
// from beelog can have commands on different orders, which is safe as long as
// if a log posses a command 'c' on index 'i', no other log records a command 'k'
// on 'i' where 'k' != 'c'.
func logsAreEquivalent(logA, logB []pb.Command) bool {
	// not the same size, directly not equivalent
	if len(logA) != len(logB) {
		return false
	}

	// they are already deeply equal, same values on the same positions
	if reflect.DeepEqual(logA, logB) {
		return true
	}

	// apply each log on a hash table, checking if they have the same
	// values for the same keys
	htA := make(map[string]string)
	htB := make(map[string]string)

	for i := range logA {
		htA[logA[i].Key] = logA[i].Value
		htB[logB[i].Key] = logB[i].Value
	}
	return reflect.DeepEqual(htA, htB)
}

func resetLogStates(fn string) error {
	err := os.Remove(fn)
	if err != nil && !os.IsNotExist(err) {
		return err
	}

	// reset not only the informed index, but the different view states as well
	for i := 0; i < concLevel; i++ {
		err := os.Remove(fn + "." + strconv.Itoa(i))
		if err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return nil
}
