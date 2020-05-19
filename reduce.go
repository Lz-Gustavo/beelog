package main

import (
	"errors"
)

// Reducer indexes different log compact strategies. For all list-based algorithms,
// it's assumed that the list is represented on the oposite order.  The first element
// is the last proposed index in the command log, its neighbor is the prior-indexed
// command, and so on.
type Reducer int8

const (
	// BubblerLt ...
	BubblerLt Reducer = iota

	// MergeLt ...
	MergeLt

	// GreedyLt ...
	GreedyLt

	// RecurB1 ...
	RecurB1

	// GreedyB1 ...
	GreedyB1

	// IterB1 ...
	IterB1
)

// ApplyReduceAlgo ...
func ApplyReduceAlgo(s Structure, r Reducer, p, n int) ([]KVCommand, error) {
	var log []KVCommand

	switch st := s.(type) {
	case *List:
		switch r {
		case BubblerLt:
			log = BubblerList(st, p, n)
			break

		case MergeLt:
			log = MergeList(st, p, n)
			break

		case GreedyLt:
			log = GreedyList(st, p, n)
			break

		default:
			return nil, errors.New("unsupported reduce algorithm")
		}
		break

	case *AVLTreeHT:
		switch r {
		case RecurB1:
		}
	default:
		return nil, errors.New("unsupported log datastructure")
	}
	return log, nil
}

// BubblerList is the simpliest algorithm.
func BubblerList(l *List, p, n int) []KVCommand {
	var (
		rm bool
		rc int
		i  *listNode
	)
	log := make([]KVCommand, 0)

	for {
		rm = false
		for i = l.first; i != nil && i.next != nil; i = i.next {

			cmd := i.val.(State).cmd
			neigh := i.next.val.(State).cmd

			// Subsequent write operations over the same key
			if cmd.key == neigh.key {
				i.next = i.next.next
				rm = true
				rc++
			}
		}
		l.tail = i

		if !rm {
			break
		}
	}
	l.len -= rc

	for i = l.first; i != nil; i = i.next {
		st := i.val.(State)
		if st.ind >= p && st.ind <= n {
			log = append(log, st.cmd)
		}
	}
	return log
}

// MergeList is based on MergeSort algorithm ...
func MergeList(l *List, i, n int) []KVCommand {
	return nil
}

// GreedyList returns an optimal solution...
func GreedyList(l *List, p, n int) []KVCommand {
	var (
		rc   int
		i, j *listNode
	)
	log := make([]KVCommand, 0)

	// iterator i can reach nil value on the last j iteration
	for i = l.first; i != nil && i.next != nil; i = i.next {

		st := i.val.(State)
		priorNeigh := i
		for j = i.next; j != nil; j = j.next {

			neigh := j.val.(State)

			// Subsequent write operations over the same key
			if st.cmd.key == neigh.cmd.key {
				priorNeigh.next = j.next
				rc++

			} else {
				priorNeigh = j
			}
		}

		if st.ind >= p && st.ind <= n {
			log = append(log, st.cmd)
		}
	}
	l.len -= rc
	return log
}

// RecurB1AVLTreeHT ...
func RecurB1AVLTreeHT(avl *AVLTreeHT, p, n int) []KVCommand {
	return nil
}

// GreedyB1AVLTreeHT ...
func GreedyB1AVLTreeHT(avl *AVLTreeHT, p, n int) []KVCommand {
	return nil
}

// IterB1AVLTreeHT ...
func IterB1AVLTreeHT(avl *AVLTreeHT, p, n int) []KVCommand {
	return nil
}
