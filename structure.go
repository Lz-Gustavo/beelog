package beelog

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"os"

	"github.com/Lz-Gustavo/beelog/pb"

	"github.com/golang/protobuf/proto"
)

// Structure is an abstraction for the different log representation structures
// implemented.
type Structure interface {
	Str() string
	Len() uint64
	Log(index uint64, cmd pb.Command) error
	Recov(p, n uint64) ([]pb.Command, error)
	RecovBytes(p, n uint64) ([]byte, error)
}

type listNode struct {
	val  interface{}
	next *listNode
}

type list struct {
	first   *listNode
	tail    *listNode
	len     uint64
	visited bool
}

// push inserts a new node with the argument value on the list, returning a
// reference to it.
func (l *list) push(v interface{}) *listNode {
	nd := &listNode{
		val: v,
	}

	// empty list, first element
	if l.tail == nil {
		l.first = nd
		l.tail = nd
	} else {
		l.tail.next = nd
		l.tail = nd
	}
	l.len++
	return nd
}

// pop removes and returns the first element on the list.
func (l *list) pop() *listNode {
	if l.first == nil {
		return nil
	}

	l.len--
	if l.first == l.tail {
		aux := l.first
		l.first = nil
		l.tail = nil
		return aux
	}
	aux := l.first
	l.first = aux.next
	return aux
}

// similar to Floyd's tortoise and hare algorithm
func findMidInList(start, last *listNode) *listNode {
	if start == nil || last == nil {
		return nil
	}
	slow := start
	fast := start.next

	for fast != last {
		fast = fast.next
		if fast != last {
			slow = slow.next
			fast = fast.next
		}
	}
	return slow
}

// State represents a new state, a command execution happening on a certain
// consensus index, analogous to a logical clock event.
type State struct {
	ind uint64
	cmd pb.Command
}

// stateTable maps state updates for particular keys, stored as an underlying
// list of State.
type stateTable map[string]*list

// logData is the general data for each implementation of Structure interface
type logData struct {
	config      *LogConfig
	logged      bool
	first, last uint64
	recentLog   *[]pb.Command // used only on Immediately inmem config
	count       uint32        // used on Interval config
}

func (ld *logData) retrieveLog() ([]pb.Command, error) {
	if ld.config.Inmem {
		return *ld.recentLog, nil
	}

	// recover from the most recent state at ld.config.Fname
	fd, err := os.OpenFile(ld.config.Fname, os.O_RDONLY, 0644)
	if err != nil {
		return nil, err
	}
	defer fd.Close()
	return UnmarshalLogFromReader(fd)
}

func (ld *logData) retrieveRawLog(p, n uint64) ([]byte, error) {
	var rd io.Reader
	if ld.config.Inmem {
		buff := bytes.NewBuffer(nil)
		err := MarshalLogIntoWriter(buff, ld.recentLog, p, n)
		if err != nil {
			return nil, err
		}
		rd = buff

	} else {
		fd, err := os.OpenFile(ld.config.Fname, os.O_RDONLY, 0644)
		if err != nil {
			return nil, err
		}
		defer fd.Close()
		rd = fd
	}

	logs, err := ioutil.ReadAll(rd)
	if err != nil {
		return nil, err
	}
	return logs, nil
}

func (ld *logData) updateLogState(lg []pb.Command, p, n uint64) error {
	if ld.config.Inmem {
		// update the most recent inmem log state
		ld.recentLog = &lg
		return nil
	}

	// update the current state at ld.config.Fname
	fd, err := os.OpenFile(ld.config.Fname, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer fd.Close()

	err = MarshalLogIntoWriter(fd, &lg, p, n)
	if err != nil {
		return err
	}
	return nil
}

func (ld *logData) appendToLogState(lg []pb.Command, p, n uint64) error {
	if ld.config.Inmem {
		for _, c := range lg {
			*ld.recentLog = append(*ld.recentLog, c)
		}
		return nil
	}

	// update the current state at ld.config.Fname
	fd, err := os.OpenFile(ld.config.Fname, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer fd.Close()

	if err = UpdateLogIndexesInFile(fd, p, n); err != nil {
		return err
	}

	if err = MarshalAndAppendIntoWriter(fd, &lg); err != nil {
		return err
	}
	return nil
}

// firstReduceExists is execute on Interval tick config, and checks if a ReduceLog
// procedure was already executed. False is returned if no recent reduced state is
// found (i.e. first 'ld.config.Period' wasnt reached yet).
func (ld *logData) firstReduceExists() bool {
	if ld.config.Inmem {
		return ld.recentLog != nil
	}

	// disk config, found any state file
	// TODO: verify if the found file has a matching interval?
	if _, exists := os.Stat(ld.config.Fname); exists == nil {
		return true
	}
	return false
}

// RetainLogInterval receives an entire log and returns the corresponding log
// matching [p, n] indexes.
func RetainLogInterval(log *[]pb.Command, p, n uint64) []pb.Command {
	cmds := make([]pb.Command, 0, n-p)

	// TODO: Later improve retrieve algorithm, exploiting the pre-ordering of
	// commands based on c.Id. The idea is to simply identify the first and last
	// underlying indexes and return a subslice copy.
	for _, c := range *log {
		if c.Id >= p && c.Id <= n {
			cmds = append(cmds, c)
		}
	}
	return cmds
}

// UnmarshalLogFromReader returns the entire log contained at 'logRd', interpreting commands
// from the byte stream following a simple slicing protocol, where the size of each command
// is binary encoded before each raw pbuff.
func UnmarshalLogFromReader(logRd io.Reader) ([]pb.Command, error) {
	// read the retrieved log interval
	var f, l uint64
	_, err := fmt.Fscanf(logRd, "%d\n%d\n", &f, &l)
	if err != nil {
		return nil, err
	}

	cmds := make([]pb.Command, 0, l-f)
	for {
		var commandLength int32
		err := binary.Read(logRd, binary.BigEndian, &commandLength)
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}

		serializedCmd := make([]byte, commandLength)
		_, err = logRd.Read(serializedCmd)
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

// MarshalLogIntoWriter records the provided log indexes into 'logWr' writer, then marshals
// the entire command log following a simple serialization procedure where the size of
// each command is binary encoded before the raw pbuff. Commands are marshaled and written to
// 'logWr' one by one.
func MarshalLogIntoWriter(logWr io.Writer, log *[]pb.Command, p, n uint64) error {
	// write requested delimiters for the current state
	_, err := fmt.Fprintf(logWr, "%d\n%d\n", p, n)
	if err != nil {
		return err
	}

	for _, c := range *log {
		raw, err := proto.Marshal(&c)
		if err != nil {
			return err
		}

		// writing size of each serialized message as streaming delimiter
		err = binary.Write(logWr, binary.BigEndian, int32(len(raw)))
		if err != nil {
			return err
		}

		_, err = logWr.Write(raw)
		if err != nil {
			return err
		}
	}
	return nil
}

// MarshalAndAppendIntoWriter marshals the entire command log following a simple serialization
// procedure where the size of each command is binary encoded before the raw pbuff. After
// serialization the entire byte sequence is appended to 'logWr' on a single call.
func MarshalAndAppendIntoWriter(logWr io.WriteSeeker, log *[]pb.Command) error {
	buff := bytes.NewBuffer(nil)
	for _, c := range *log {
		raw, err := proto.Marshal(&c)
		if err != nil {
			return err
		}

		// writing size of each serialized message as streaming delimiter
		err = binary.Write(buff, binary.BigEndian, int32(len(raw)))
		if err != nil {
			return err
		}

		_, err = buff.Write(raw)
		if err != nil {
			return err
		}
	}

	_, err := logWr.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}

	if _, err = buff.WriteTo(logWr); err != nil {
		return err
	}
	return nil
}

// UpdateLogIndexesInFile updates the persistent log indexes without unmarshaling then marshaling
// the entire sequence. Recognizes the following format (single quotes (') chars not present):
//   'p index'\n
//   'n index'\n
//   'log...'
func UpdateLogIndexesInFile(fd *os.File, p, n uint64) error {
	_, err := fd.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}

	_, err = fmt.Fprintf(fd, "%d\n%d\n", p, n)
	if err != nil {
		return err
	}
	return nil
}
