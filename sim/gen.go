package main

import (
	"bufio"
	"errors"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"time"

	bl "github.com/Lz-Gustavo/beelog"
	"github.com/Lz-Gustavo/beelog/pb"
)

// StructID indexes different structs available for log representation.
type StructID int8

const (
	// LogList maps the command log as a simple linked list, represented
	// on the oposite order. The first element is the last proposed index
	// in the command log, its neighbor is the prior-indexed command, and
	// so on.
	LogList StructID = iota

	// LogAVL represents the log as an underlying AVL tree, indexed by
	// command indexes. Only state updates (i.e. Writes) are mapped into
	// nodes. An auxilary hash table is used to track state updates over
	// a particular key.
	LogAVL

	// LogDAG is a work-in-progress log representation as an underlying
	// BST with a state updates being mapped as a DAG. The tree nodes
	// represent particular keys, each pointing to the first node of the
	// graph. The main purpose of this approach is to track dependencies
	// between multiple-key operations (i.e. SWAPS).
	LogDAG
)

// Generator generates a structure with random elements, considering the config
// parameters provided. 'n' is the total number of commands; 'wrt' the write
// percentage of that randomized load profile; and 'dif' the number of different
// keys to be considered.
type Generator func(n, wrt, dif int) (bl.Structure, error)

// TranslateGen returns a known generator for a particular structure.
func TranslateGen(id StructID) Generator {
	switch id {
	case LogList:
		return ListGen

	case LogAVL:
		return AVLTreeHTGen

	default:
		return nil
	}
}

// ListGen generates a random log following the LogList representation.
// TODO: Reimplement this procedure adapting for the new ListHT structure
func ListGen(n, wrt, dif int) (bl.Structure, error) {
	srand := rand.NewSource(time.Now().UnixNano())
	r := rand.New(srand)
	l := bl.NewListHT()

	for i := 0; i < n; i++ {
		if cn := r.Intn(100); cn < wrt {
			cmd := pb.Command{
				Key:   strconv.Itoa(r.Intn(dif)),
				Value: strconv.Itoa(r.Int()),
				Op:    pb.Command_SET,
			}

			// the list is represented on the oposite order
			l.Log(uint64(n-1-i), cmd)

		} else {
			continue
		}
	}
	return l, nil
}

// AVLTreeHTGen generates a random log following the LogAVL representation.
func AVLTreeHTGen(n, wrt, dif int) (bl.Structure, error) {
	srand := rand.NewSource(time.Now().UnixNano())
	r := rand.New(srand)
	avl := bl.NewAVLTreeHT()

	for i := 0; i < n; i++ {
		// only WRITE operations are recorded on the tree
		if cn := r.Intn(100); cn < wrt {
			cmd := pb.Command{
				Key:   strconv.Itoa(r.Intn(dif)),
				Value: strconv.Itoa(r.Int()),
				Op:    pb.Command_SET,
			}

			err := avl.Log(uint64(i), cmd)
			if err != nil {
				return nil, err
			}

		} else {
			continue
		}
	}
	return avl, nil
}

// Constructor constructs a command log by parsing the contents of the file
// 'fn', returning the specific structure and the number of commands interpreted.
type Constructor func(fn string) (bl.Structure, int, error)

// TranslateConst returns a known constructor for a particular structure.
func TranslateConst(id StructID) Constructor {
	switch id {
	case LogList:
		// TODO:
		return nil

	case LogAVL:
		return AVLTreeHTConst

	default:
		return nil
	}
}

// AVLTreeHTConst constructs a command log following the LogAVL representation.
func AVLTreeHTConst(fn string) (bl.Structure, int, error) {
	log, err := parseLog(fn)
	if err != nil {
		return nil, 0, err
	}

	ln := len(log)
	if ln == 0 {
		return nil, 0, errors.New("empty logfile informed")
	}
	avl := bl.NewAVLTreeHT()

	for i, cmd := range log {

		err := avl.Log(uint64(i), cmd)
		if err != nil {
			return nil, 0, err
		}
	}
	return avl, ln, nil
}

// parseLog interprets the custom defined log format, equivalent to the string
// representation of the pb.Command struct.
func parseLog(fn string) ([]pb.Command, error) {
	fd, err := os.Open(fn)
	if err != nil {
		return nil, err
	}
	defer fd.Close()

	log := make([]pb.Command, 0)
	sc := bufio.NewScanner(fd)

	for sc.Scan() {
		line := sc.Text()
		fields := strings.Split(line, " ")

		op, _ := strconv.Atoi(fields[0])
		key := fields[1]
		value := fields[2]

		cmd := pb.Command{
			Op:    pb.Command_Operation(op),
			Key:   key,
			Value: value,
		}
		log = append(log, cmd)
	}
	return log, nil
}
