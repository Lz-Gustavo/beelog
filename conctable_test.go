package beelog

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"path/filepath"
	"testing"
	"time"

	"github.com/Lz-Gustavo/beelog/pb"
	"github.com/golang/protobuf/proto"
)

func TestConcTableRecovEntireLog(t *testing.T) {
	nCmds, wrt, dif := uint64(2000), 50, 100
	concRecov := false

	cfgs := []LogConfig{
		{
			Inmem:   false,
			KeepAll: true,
			Alg:     IterConcTable,
			Tick:    Interval,
			Period:  100,
			Fname:   "./logstate.log",
		},
	}

	for _, cf := range cfgs {
		var st Structure
		var err error

		// clean state before creating
		if err := cleanAllLogStates(); err != nil {
			t.Log(err.Error())
			t.FailNow()
		}

		st, err = generateRandStructure(4, nCmds, wrt, dif, &cf)
		if err != nil {
			t.Log(err.Error())
			t.FailNow()
		}

		if concRecov {
			ch, num, err := st.(*ConcTable).RecovEntireLogConc()
			if err != nil {
				t.Log(err.Error())
				t.FailNow()
			}
			fmt.Println("now reading ch...")

			for {
				raw, ok := <-ch
				if !ok { // closed ch
					fmt.Println("closed!")
					break
				}
				fmt.Println("got one, deserializing")

				log, err := deserializeRawLogStream(raw, num)
				if err == io.EOF {
					t.Log("empty log")
					t.Fail()

				} else if err != nil {
					t.Log("error while deserializing log, err:", err.Error())
					t.FailNow()
				}

				t.Log("got one log:", log)
				// TODO: test log...
			}

		} else {
			raw, num, err := st.(*ConcTable).RecovEntireLog()
			if err != nil {
				t.Log(err.Error())
				t.FailNow()
			}

			log, err := deserializeRawLogStream(raw, num)
			if err != nil {
				t.Log(err.Error())
				t.FailNow()
			}

			t.Log("got log:", log)
			// TODO: test log...
		}
	}

	if err := cleanAllLogStates(); err != nil {
		t.Log(err.Error())
		t.FailNow()
	}
}

func TestConcTableLatencyMeasurementAndSync(t *testing.T) {
	nCmds, wrt, dif := uint64(2000), 50, 100
	cfgs := []LogConfig{
		{
			Inmem:   false,
			KeepAll: true,
			Sync:    true,
			Measure: true,
			Alg:     IterConcTable,
			Tick:    Interval,
			Period:  200,
			Fname:   "./logstate.log",
		},
	}

	for _, cf := range cfgs {
		// clean state before creating
		err := cleanAllLogStates()
		if err != nil {
			t.Log(err.Error())
			t.FailNow()
		}

		// latency is already recorded while being generated
		st, err := generateRandStructure(4, nCmds, wrt, dif, &cf)
		if err != nil {
			t.Log(err.Error())
			t.FailNow()
		}

		// persist latency metrics by during shutdown
		st.(*ConcTable).Shutdown()
	}

	if err := cleanAllLogStates(); err != nil {
		t.Log(err.Error())
		t.FailNow()
	}
}

func TestConcTableParallelIO(t *testing.T) {
	nCmds, wrt, dif := uint64(800), 50, 200

	primDir := t.TempDir()
	secdDir := t.TempDir()
	cfgs := []LogConfig{
		{
			KeepAll:     true,
			Alg:         IterConcTable,
			Tick:        Interval,
			Period:      200,
			Fname:       primDir + "/logstate.log",
			ParallelIO:  true,
			SecondFname: secdDir + "/logstate2.log",
		},
	}

	for _, cf := range cfgs {
		// log files should be interchanged between primary and second fns
		_, err := generateRandStructure(4, nCmds, wrt, dif, &cf)
		if err != nil {
			t.Log(err.Error())
			t.FailNow()
		}

		// must wait concurrent persistence...
		time.Sleep(time.Second)

		logsPrim, err := filepath.Glob(primDir + "/*.log")
		if err != nil {
			t.Log(err.Error())
			t.FailNow()
		}

		logsSecd, err := filepath.Glob(secdDir + "/*.log")
		if err != nil {
			t.Log(err.Error())
			t.FailNow()
		}

		if p, s := len(logsPrim), len(logsSecd); p != s {
			t.Log("With an even interval config, expected primary and secondary locations to have the same number of log files")
			t.Log("PRIMARY HAS:", p)
			t.Log("SECONDARY HAS:", s)
			t.FailNow()
		}
	}
}

// deserializeRawLogStream emulates the same procedure implemented by a recov
// replica, interpreting the serialized log stream received from RecovEntireLog
// different calls.
func deserializeRawLogStream(stream []byte, size int) ([]pb.Command, error) {
	rd := bytes.NewReader(stream)
	cmds := make([]pb.Command, 0, 256*size)

	for i := 0; i < size; i++ {
		// read the retrieved log interval
		var f, l uint64
		var ln int
		_, err := fmt.Fscanf(rd, "%d\n%d\n%d\n", &f, &l, &ln)
		if err != nil {
			return nil, err
		}

		for j := 0; j < ln; j++ {
			var commandLength int32
			err = binary.Read(rd, binary.BigEndian, &commandLength)
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
				fmt.Println("could not parse")
				return nil, err
			}
			cmds = append(cmds, *c)
		}

		var eol string
		_, err = fmt.Fscanf(rd, "\n%s\n", &eol)
		if err != nil {
			return nil, err
		}

		if eol != "EOL" {
			return nil, fmt.Errorf("expected EOL flag, got '%s'", eol)
		}
	}
	return cmds, nil
}
