package beelog

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math/rand"
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

	for _, st := range []Structure{avl, lt, arr} {
		// populate some SET commands
		for i := first; i < n; i++ {
			// TODO: yeah, I will change this API of index log in time
			err := st.Log(i, pb.Command{Id: i, Op: pb.Command_SET})
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
			t.Log(l, "commands, expected", n-first)
			t.FailNow()
		}
	}

	// indexes should be 'first' and 'n', considering n-1 SETS + 1 GET
	if avl.first != first {
		t.Log("first cmd index is", avl.first, ", expected", first)
		t.FailNow()
	}
	if avl.last != n {
		t.Log("last cmd index is", avl.last, ", expected", n)
		t.FailNow()
	}

	// same as list indexes
	if lt.first != first {
		t.Log("first cmd index is", lt.first, ", expected", first)
		t.FailNow()
	}
	if lt.last != n {
		t.Log("last cmd index is", lt.last, ", expected", n)
		t.FailNow()
	}

	// same as array
	if arr.first != first {
		t.Log("first cmd index is", arr.first, ", expected", first)
		t.FailNow()
	}
	if arr.last != n {
		t.Log("last cmd index is", arr.last, ", expected", n)
		t.FailNow()
	}
}

func TestStructuresDifferentRecoveries(t *testing.T) {
	// Requesting the last matching index (i.e. n == nCmds) is mandatory
	// on Immediately and Interval configurations.
	nCmds, wrt, dif := uint64(2000), 50, 10
	p, n := uint64(10), uint64(2000)

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
			Period: 100,
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
			Period: 100,
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
	}

	for _, tc := range testCases {
		for j, cf := range tc.configs {
			var st Structure
			var err error

			// set the current test case algorithm
			cf.Alg = tc.Alg

			// Currently logs must be re-generated for each different config, which
			// prohibits the generation of an unique log file for all configs. An
			// unique log is not needed on a unit test scenario, since the idea is
			// not to compare these strategies, only tests its correctness.
			switch tc.structID {
			case 0:
				st, err = generateRandListHT(nCmds, wrt, dif, &cf)
				if err != nil {
					t.Log(err.Error())
					t.FailNow()
				}
				t.Log("====Executing List Test Case #", j)
				break

			case 1:
				st, err = generateRandArrayHT(nCmds, wrt, dif, &cf)
				if err != nil {
					t.Log(err.Error())
					t.FailNow()
				}
				t.Log("====Executing Array Test Case #", j)
				break

			case 2:
				st, err = generateRandAVLTreeHT(nCmds, wrt, dif, &cf)
				if err != nil {
					t.Log(err.Error())
					t.FailNow()
				}
				t.Log("====Executing AVLTree Test Case #", j)
				break

			default:
				t.Logf("unknow structure '%d' provided", tc.structID)
				t.FailNow()
			}

			// the compacted log used for later comparison
			redLog, err := ApplyReduceAlgo(st, cf.Alg, p, n)
			if err != nil {
				t.Log(err.Error())
				t.FailNow()
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
	}

	for _, tc := range testCases {
		for j, cf := range tc.configs {
			var st Structure
			var err error

			// set the current test case algorithm
			cf.Alg = tc.Alg

			switch tc.structID {
			case 0:
				st, err = generateRandListHT(nCmds, wrt, dif, &cf)
				if err != nil {
					t.Log(err.Error())
					t.FailNow()
				}
				t.Log("====Executing List Test Case #", j)
				break

			case 1:
				st, err = generateRandArrayHT(nCmds, wrt, dif, &cf)
				if err != nil {
					t.Log(err.Error())
					t.FailNow()
				}
				t.Log("====Executing Array Test Case #", j)
				break

			case 2:
				st, err = generateRandAVLTreeHT(nCmds, wrt, dif, &cf)
				if err != nil {
					t.Log(err.Error())
					t.FailNow()
				}
				t.Log("====Executing AVLTree Test Case #", j)
				break

			default:
				t.Logf("unknow structure '%d' provided", tc.structID)
				t.FailNow()
			}

			// the compacted log used for later comparison
			redLog, err := ApplyReduceAlgo(st, cf.Alg, p, n)
			if err != nil {
				t.Log(err.Error())
				t.FailNow()
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

func generateRandListHT(n uint64, wrt, dif int, cfg *LogConfig) (*ListHT, error) {
	srand := rand.NewSource(time.Now().UnixNano())
	r := rand.New(srand)
	var lt *ListHT
	var err error

	if cfg == nil {
		lt = NewListHT() // uses DefaultLogConfig underneath
	} else {
		lt, err = NewListHTWithConfig(cfg)
		if err != nil {
			return nil, err
		}
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
		err = lt.Log(i, cmd)
		if err != nil {
			return nil, err
		}
	}
	return lt, nil
}

func generateRandArrayHT(n uint64, wrt, dif int, cfg *LogConfig) (*ArrayHT, error) {
	srand := rand.NewSource(time.Now().UnixNano())
	r := rand.New(srand)
	var arr *ArrayHT
	var err error

	if cfg == nil {
		arr = NewArrayHT() // uses DefaultLogConfig underneath
	} else {
		arr, err = NewArrayHTWithConfig(cfg)
		if err != nil {
			return nil, err
		}
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
		err = arr.Log(i, cmd)
		if err != nil {
			return nil, err
		}
	}
	return arr, nil
}

func generateRandAVLTreeHT(n uint64, wrt, dif int, cfg *LogConfig) (*AVLTreeHT, error) {
	srand := rand.NewSource(time.Now().UnixNano())
	r := rand.New(srand)
	var avl *AVLTreeHT
	var err error

	if cfg == nil {
		avl = NewAVLTreeHT() // uses DefaultLogConfig underneath
	} else {
		avl, err = NewAVLTreeHTWithConfig(cfg)
		if err != nil {
			return nil, err
		}
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
		err = avl.Log(i, cmd)
		if err != nil {
			return nil, err
		}
	}
	return avl, nil
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
