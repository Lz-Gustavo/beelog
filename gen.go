package main

import (
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"time"
)

// Generator ...
type Generator interface {
	Seed()
	Gen(n, wrt, dif int) (Structure, error)
}

// GenID ...
type GenID int8

const (
	// LogList ...
	LogList GenID = iota

	// LogDAG ...
	LogDAG
)

// TranslateGen ...
func TranslateGen(id GenID) Generator {
	switch id {
	case LogList:
		return &ListGenerator{}

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

// ListGenerator ...
type ListGenerator struct {
	srand rand.Source
	r     *rand.Rand
}

// Seed reseeds the pseudo-random number generator with current system
// time on UNIX ns format.
func (lg *ListGenerator) Seed() {
	lg.srand = rand.NewSource(time.Now().UnixNano())
	lg.r = rand.New(lg.srand)
}

// Gen ...
func (lg *ListGenerator) Gen(n, wrt, dif int) (Structure, error) {

	if lg.r == nil {
		return nil, errors.New("Not seeded generator, run gen.Seed()")
	}

	cmd := KVCommand{
		key:   lg.r.Intn(dif),
		value: lg.r.Uint32(),
	}
	if cn := lg.r.Intn(100); cn < wrt {
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
			key:   lg.r.Intn(dif),
			value: lg.r.Uint32(),
		}
		if cn := lg.r.Intn(100); cn < wrt {
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

// Structure ...
type Structure interface {
	Str() string
	Len() int
	Hash()
}

type listNode struct {
	val  interface{}
	next *listNode
}

// List ...
type List struct {
	first *listNode
	tail  *listNode
	len   int
	mu    sync.Mutex
}

// Str returns a string representation of the list state, used for debug purposes.
func (l *List) Str() string {
	l.mu.Lock()
	defer l.mu.Unlock()

	var strs []string
	for i := l.first; i != nil; i = i.next {
		strs = append(strs, fmt.Sprintf("%v->", i.val))
	}
	return strings.Join(strs, " ")
}

// Len returns the list length.
func (l *List) Len() int {
	return l.len
}

// Hash tranverses the list returning a hash codification for the entire structure.
func (l *List) Hash() {
	l.mu.Lock()
	defer l.mu.Unlock()
}
