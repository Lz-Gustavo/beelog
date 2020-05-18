package main

import (
	"errors"
	"math/rand"
	"time"
)

// Generator ...
type Generator func(n, wrt, dif int) (Structure, error)

// GenID ...
type GenID int8

const (
	// LogList ...
	LogList GenID = iota

	// LogAVL ...
	LogAVL

	// LogDAG ...
	LogDAG
)

// TranslateGen ...
func TranslateGen(id GenID) Generator {
	switch id {
	case LogList:
		return ListGen

	case LogAVL:
		return AVLTreeGen

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

	cmd := KVCommand{
		key:   r.Intn(dif),
		value: r.Uint32(),
	}
	if cn := r.Intn(100); cn < wrt {
		cmd.op = Write
	} else {
		cmd.op = Read
	}

	root := &listNode{
		val: cmd,
	}
	l := &List{
		first: root,
		tail:  root,
		len:   n,
	}

	for i := 1; i < n; i++ {
		cmd := KVCommand{
			key:   r.Intn(dif),
			value: r.Uint32(),
		}
		if cn := r.Intn(100); cn < wrt {
			cmd.op = Write
		} else {
			cmd.op = Read
		}

		nd := &listNode{
			val: cmd,
		}

		l.tail.next = nd
		l.tail = nd
	}
	return l, nil
}

// AVLTreeGen ...
func AVLTreeGen(n, wrt, dif int) (Structure, error) {

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
			}

			// A write cmd always references a new state on the aux hash table
			st := &State{
				ind: i,
				cmd: cmd,
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
