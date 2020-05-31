package main

import (
	"bufio"
	"errors"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"time"
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
type Generator func(n, wrt, dif int) (Structure, error)

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

// Operation indexes the different commands recognized by the kvstore application.
// Besides reads and writes, the idea is to later support SWAP operations.
type Operation uint8

const (
	// Read the value of a certain key.
	Read Operation = iota

	// Write a specific value over an informed key.
	Write
)

// KVCommand defines the command format for the simulated key-value application.
type KVCommand struct {
	op    Operation
	key   int
	value uint32
}

// ListGen generates a random log following the LogList representation.
func ListGen(n, wrt, dif int) (Structure, error) {

	srand := rand.NewSource(time.Now().UnixNano())
	r := rand.New(srand)
	l := &List{len: n}

	for i := 0; i < n; i++ {

		if cn := r.Intn(100); cn < wrt {
			cmd := KVCommand{
				key:   r.Intn(dif),
				value: r.Uint32(),
				op:    Write,
			}

			// the list is represented on the oposite order
			l.Log(n-1-i, cmd)

		} else {
			continue
		}
	}
	return l, nil
}

// AVLTreeHTGen generates a random log following the LogAVL representation.
func AVLTreeHTGen(n, wrt, dif int) (Structure, error) {

	srand := rand.NewSource(time.Now().UnixNano())
	r := rand.New(srand)
	avl := NewAVLTreeHT()

	for i := 0; i < n; i++ {

		// only WRITE operations are recorded on the tree
		if cn := r.Intn(100); cn < wrt {
			cmd := KVCommand{
				key:   r.Intn(dif),
				value: r.Uint32(),
				op:    Write,
			}

			err := avl.Log(i, cmd)
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
type Constructor func(fn string) (Structure, int, error)

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
func AVLTreeHTConst(fn string) (Structure, int, error) {

	log, err := parseLog(fn)
	if err != nil {
		return nil, 0, err
	}

	ln := len(log)
	if ln == 0 {
		return nil, 0, errors.New("empty logfile informed")
	}
	avl := NewAVLTreeHT()

	for i, cmd := range log {

		err := avl.Log(i, cmd)
		if err != nil {
			return nil, 0, err
		}
	}
	return avl, ln, nil
}

// parseLog interprets the custom defined log format, equivalent to the string
// representation of the KVCommand struct.
func parseLog(fn string) ([]KVCommand, error) {
	fd, err := os.Open(fn)
	if err != nil {
		return nil, err
	}
	defer fd.Close()

	log := make([]KVCommand, 0)
	sc := bufio.NewScanner(fd)

	for sc.Scan() {
		line := sc.Text()
		fields := strings.Split(line, " ")

		op, _ := strconv.Atoi(fields[0])
		key, _ := strconv.Atoi(fields[1])
		value, _ := strconv.ParseInt(fields[2], 10, 32)

		cmd := KVCommand{
			op:    Operation(op),
			key:   key,
			value: uint32(value),
		}
		log = append(log, cmd)
	}
	return log, nil
}
