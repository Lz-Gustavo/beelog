package main

import (
	"fmt"
	"strings"
	"sync"
)

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
