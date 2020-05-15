package main

import (
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
	return nil, nil
}
