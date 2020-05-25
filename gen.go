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

// StructID ...
type StructID int8

const (
	// LogList ...
	LogList StructID = iota

	// LogAVL ...
	LogAVL

	// LogDAG ...
	LogDAG
)

// Generator ...
type Generator func(n, wrt, dif int) (Structure, error)

// TranslateGen ...
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

// Operation ...
type Operation uint8

const (
	// Read ...
	Read Operation = iota

	// Write ...
	Write
)

// KVCommand ...
type KVCommand struct {
	op    Operation
	key   int
	value uint32
}

// ListGen ...
func ListGen(n, wrt, dif int) (Structure, error) {

	srand := rand.NewSource(time.Now().UnixNano())
	r := rand.New(srand)

	l := &List{
		len: n,
	}

	for i := 0; i < n; i++ {

		if cn := r.Intn(100); cn < wrt {
			cmd := KVCommand{
				key:   r.Intn(dif),
				value: r.Uint32(),
				op:    Write,
			}
			st := State{
				ind: n - 1 - i,
				cmd: cmd,
			}
			nd := &listNode{
				val: st,
			}

			// empty list, first element
			if l.first == nil {
				l.first = nd
				l.tail = nd

			} else {
				l.tail.next = nd
				l.tail = nd
			}

		} else {
			continue
		}
	}
	return l, nil
}

// AVLTreeHTGen ...
func AVLTreeHTGen(n, wrt, dif int) (Structure, error) {

	srand := rand.NewSource(time.Now().UnixNano())
	r := rand.New(srand)

	ht := make(stateTable, 0)
	avl := &AVLTreeHT{
		aux: &ht,
		len: 0,
	}

	for i := 0; i < n; i++ {

		// only WRITE nodes are recorded on the tree
		if cn := r.Intn(100); cn < wrt {

			rkey := r.Intn(dif)
			cmd := KVCommand{
				key:   rkey,
				value: r.Uint32(),
				op:    Write,
			}

			aNode := &avlTreeNode{
				ind: i,
				key: rkey,
			}

			// A write cmd always references a new state on the aux hash table
			st := &State{
				ind: i,
				cmd: cmd,
			}

			_, exists := (*avl.aux)[rkey]
			if !exists {
				(*avl.aux)[rkey] = &List{}
			}

			// Add state to the list of updates in that particular key
			lNode := (*avl.aux)[rkey].push(st)
			aNode.ptr = lNode

			ok := avl.insert(aNode)
			if !ok {
				return nil, errors.New("cannot insert equal keys on BSTs")
			}

		} else {
			continue
		}
	}
	return avl, nil
}

// Constructor ...
type Constructor func(fn string) (Structure, error)

// TranslateConst ...
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

// AVLTreeHTConst ...
func AVLTreeHTConst(fn string) (Structure, error) {

	log, err := parseLog(fn)
	if err != nil {
		return nil, err
	}

	if len(log) == 0 {
		return nil, errors.New("empty logfile")
	}

	ht := make(stateTable, 0)
	avl := &AVLTreeHT{
		aux: &ht,
		len: 0,
	}

	for i, cmd := range log {
		aNode := &avlTreeNode{
			ind: i,
			key: cmd.key,
		}

		// A write cmd always references a new state on the aux hash table
		st := &State{
			ind: i,
			cmd: cmd,
		}

		_, exists := (*avl.aux)[cmd.key]
		if !exists {
			(*avl.aux)[cmd.key] = &List{}
		}

		// Add state to the list of updates in that particular key
		lNode := (*avl.aux)[cmd.key].push(st)
		aNode.ptr = lNode

		ok := avl.insert(aNode)
		if !ok {
			return nil, errors.New("cannot insert equal keys on BSTs")
		}
	}
	return avl, nil
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
