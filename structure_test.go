package beelog

import (
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/Lz-Gustavo/beelog/pb"
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
	testCases := []struct {
		numCmds      uint64
		writePercent int
		diffKeys     int
		p, n         uint64
		config       LogConfig
	}{
		{ // immediately inmem
			2000,
			50,
			100,
			0,
			2000,
			LogConfig{
				alg:   IterDFSAvl,
				tick:  Immediately,
				inmem: true,
			},
		},
		{ // delayed inmem
			2000,
			50,
			100,
			0,
			2000,
			LogConfig{
				alg:   IterDFSAvl,
				tick:  Delayed,
				inmem: true,
			},
		},
		{ // immediately disk
			2000,
			50,
			100,
			0,
			2000,
			LogConfig{
				alg:   IterDFSAvl,
				tick:  Immediately,
				inmem: false,
				fname: "./logstate.out",
			},
		},
		{ // dealayed disk
			2000,
			50,
			100,
			0,
			2000,
			LogConfig{
				alg:   IterDFSAvl,
				tick:  Delayed,
				inmem: false,
				fname: "./logstate.out",
			},
		},
	}

	for _, tc := range testCases {
		_, err := generateRandAVLTreeHT(tc.numCmds, tc.writePercent, tc.diffKeys, &tc.config)
		if err != nil {
			t.Log(err.Error())
			t.FailNow()
		}

		// TODO: Complete test cases
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
		avl = NewAVLTreeHT()
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
