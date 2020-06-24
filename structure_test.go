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

func TestAVLTreeLog(t *testing.T) {
	avl := NewAVLTreeHT()
	first := uint64(1)
	n := uint64(1000)

	// populate some SET commands
	for i := first; i < n; i++ {

		// TODO: yeah, I will change this API of index log in time
		err := avl.Log(i, pb.Command{Id: i, Op: pb.Command_SET})
		if err != nil {
			t.Log(err.Error())
			t.FailNow()
		}
	}

	l := avl.Len()
	if l != n-first {
		t.Log(l, "commands, expected", n-first)
		t.FailNow()
	}

	// log another GET
	err := avl.Log(n, pb.Command{Id: n, Op: pb.Command_GET})
	if err != nil {
		t.Log(err.Error())
		t.FailNow()
	}

	// GET command shouldnt increase size, but modify 'avl.last' index
	if l != avl.Len() {
		t.Log(l, "commands, expected", n-first)
		t.FailNow()
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
}

func TestAVLTreeDifferentRecoveries(t *testing.T) {
	nCmds, wrt, dif := uint64(2000), 50, 100
	p, n := uint64(100), uint64(2000)
	defAlg := IterDFSAvl

	testCases := []LogConfig{
		{ // immediately inmem
			alg:   defAlg,
			tick:  Immediately,
			inmem: true,
		},
		{ // delayed inmem
			alg:   defAlg,
			tick:  Delayed,
			inmem: true,
		},
		{ // immediately disk
			alg:   defAlg,
			tick:  Immediately,
			inmem: false,
			fname: "./logstate.out",
		},
		{ // delayed disk
			alg:   defAlg,
			tick:  Delayed,
			inmem: false,
			fname: "./logstate.out",
		},
	}

	for i, tc := range testCases {
		t.Log("====Executing Test Case #", i)

		// Currently logs must be re-generated for each different config, which
		// prohibits the generation of an unique log file for all configs. An
		// unique log is not needed on a unit test scenario, since the idea is
		// not to compare these strategies, only tests its correctness.
		avl, err := generateRandAVLTreeHT(nCmds, wrt, dif, &tc)
		if err != nil {
			t.Log(err.Error())
			t.FailNow()
		}

		// the compacted log used for later comparison
		redLog, err := ApplyReduceAlgo(avl, defAlg, p, n)
		if err != nil {
			t.Log(err.Error())
			t.FailNow()
		}

		log, err := avl.Recov(p, n)
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
	}
}

func TestAVLTreeRecovBytesInterpretation(t *testing.T) {
	nCmds, wrt, dif := uint64(2000), 50, 100
	p, n := uint64(100), uint64(2000)
	defAlg := IterDFSAvl

	testCases := []LogConfig{
		{ // inmem byte recov
			alg:   defAlg,
			tick:  Delayed,
			inmem: true,
		},
		{ // disk byte recov
			alg:   defAlg,
			tick:  Delayed,
			inmem: false,
			fname: "./logstate.out",
		},
	}

	for i, tc := range testCases {
		t.Log("====Executing Test Case #", i)

		avl, err := generateRandAVLTreeHT(nCmds, wrt, dif, &tc)
		if err != nil {
			t.Log(err.Error())
			t.FailNow()
		}

		// the compacted log used for later comparison
		redLog, err := ApplyReduceAlgo(avl, defAlg, p, n)
		if err != nil {
			t.Log(err.Error())
			t.FailNow()
		}

		raw, err := avl.RecovBytes(p, n)
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
	}
}

func generateRandList(n uint64, wrt, dif int) (*List, error) {
	srand := rand.NewSource(time.Now().UnixNano())
	r := rand.New(srand)
	l := &List{}

	for i := uint64(0); i < n; i++ {

		if cn := r.Intn(100); cn < wrt {
			cmd := pb.Command{
				Key:   strconv.Itoa(r.Intn(dif)),
				Value: strconv.Itoa(r.Int()),
				Op:    pb.Command_SET,
			}

			// the list is represented on the oposite order
			l.Log(n-1-i, cmd)

		} else {
			continue
		}
	}
	return l, nil
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
		// only WRITE operations are recorded on the tree
		if cn := r.Intn(100); cn < wrt {
			cmd := pb.Command{
				Id:    i,
				Key:   strconv.Itoa(r.Intn(dif)),
				Value: strconv.Itoa(r.Int()),
				Op:    pb.Command_SET,
			}

			err = avl.Log(i, cmd)
			if err != nil {
				return nil, err
			}

		} else {
			continue
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

// logsAreEquivalent checks iff ...
func logsAreEquivalent(logA, logB []pb.Command) bool {
	// not the same size, directly not equivalent
	if len(logA) != len(logB) {
		return false
	}

	// they are already deeply equal, same values on the same pos
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
