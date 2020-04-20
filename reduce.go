package main

import (
	"errors"
)

// Reducer indexes different log compact strategies ...
type Reducer int8

const (
	// Bubbler ...
	Bubbler Reducer = iota

	// Merge ...
	Merge

	// Greedy ...
	Greedy
)

// ApplyReduceAlgo ...
func ApplyReduceAlgo(s Structure, r Reducer) error {
	switch st := s.(type) {
	case *List:
		switch r {
		case Bubbler:
			BubblerList(st)
			break

		case Merge:
			MergeList(st)
			break

		case Greedy:
			GreedyList(st)
			break

		default:
			return errors.New("unsupported reduce algorithm")
		}
		break

	default:
		return errors.New("unsupported log datastructure")
	}
	return nil
}

// BubblerList is the simpliest algorithm.
// Remember that the list is represented on the oposite order. The first
// element is the last proposed index in the command log, its neighbor
// is the prior-indexed command, and so on.
func BubblerList(l *List) {
	var (
		rm bool
		rc int
		i  *listNode
	)

	for {
		rm = false
		for i = l.first; i.next != nil; i = i.next {

			cmd := i.val.(KVCommand)
			neigh := i.next.val.(KVCommand)

			// Subsequent write operatios over the same key
			if cmd.op == Write && neigh.op == Write && cmd.key == neigh.key {
				i.next = i.next.next
				rm = true
				rc++
			}

			// TODO: remove read operations
		}
		l.tail = i

		if !rm {
			break
		}
	}
	l.len -= rc
}

// MergeList is based on MergeSort algorithm ...
func MergeList(s *List) {
}

// GreedyList returns an optimal solution...
func GreedyList(s *List) {
}
