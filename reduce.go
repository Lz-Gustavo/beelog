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

	// GreedyLt ...
	GreedyLt

	// GreedyAvl ...
	GreedyAvl

	// IterBFSAvl ...
	IterBFSAvl

	// IterDFSAvl ...
	IterDFSAvl
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

		case GreedyLt:
			log = GreedyList(st, p, n)
			break

		default:
			return nil, errors.New("unsupported reduce algorithm")
		}
		break

	case *AVLTreeHT:
		switch r {
		case GreedyAvl:
			log = GreedyAVLTreeHT(st, p, n)
			break

		case IterBFSAvl:
			log = IterBFSAVLTreeHT(st, p, n)
			break

		case IterDFSAvl:
			log = IterDFSAVLTreeHT(st, p, n)
			break

		default:
			return nil, errors.New("unsupported reduce algorithm")
		}
		break

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

			// subsequent write operations over the same key
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

// GreedyAVLTreeHT ...
func GreedyAVLTreeHT(avl *AVLTreeHT, p, n int) []KVCommand {
	log := []KVCommand{}
	avl.resetVisitedValues()
	greedyRecur(avl, avl.root, p, n, &log)
	return log
}

func greedyRecur(avl *AVLTreeHT, k *avlTreeNode, p, n int, log *[]KVCommand) {

	// nil or key already satisfied in the log
	if k == nil {
		return
	}

	// index in [p, n] interval and key not already satisfied on the log
	if !(*avl.aux)[k.key].visited && k.ind >= p && k.ind <= n {

		var phi KVCommand
		for j := k.ptr; j != nil && j.val.(*State).ind <= n; j = j.next {
			phi = j.val.(*State).cmd
		}

		// append only the last update of a particular key
		*log = append(*log, phi)
		(*avl.aux)[k.key].visited = true
	}
	if k.ind > p {
		greedyRecur(avl, k.left, p, n, log)
	}
	if k.ind < n {
		greedyRecur(avl, k.right, p, n, log)
	}
}

// IterBFSAVLTreeHT ...
func IterBFSAVLTreeHT(avl *AVLTreeHT, p, n int) []KVCommand {

	avl.resetVisitedValues()
	log := []KVCommand{}
	queue := []*avlTreeNode{avl.root}
	var u *avlTreeNode

	for len(queue) != 0 {

		u, queue = queue[0], queue[1:]

		// index in [p, n] interval and key not already satisfied on the log
		if !(*avl.aux)[u.key].visited && u.ind >= p && u.ind <= n {

			var phi KVCommand
			for j := u.ptr; j != nil && j.val.(*State).ind <= n; j = j.next {
				phi = j.val.(*State).cmd
			}

			// append only the last update of a particular key
			log = append(log, phi)
			(*avl.aux)[u.key].visited = true
		}

		if u.ind > p && u.left != nil {
			queue = append(queue, u.left)
		}
		if u.ind < n && u.right != nil {
			queue = append(queue, u.right)
		}
	}
	return log
}

// IterDFSAVLTreeHT ...
func IterDFSAVLTreeHT(avl *AVLTreeHT, p, n int) []KVCommand {

	avl.resetVisitedValues()
	log := []KVCommand{}
	queue := []*avlTreeNode{avl.root}
	var u *avlTreeNode

	for ln := len(queue); ln != 0; ln = len(queue) {

		u, queue = queue[ln-1], queue[:ln-1]

		// index in [p, n] interval and key not already satisfied on the log
		if !(*avl.aux)[u.key].visited && u.ind >= p && u.ind <= n {

			var phi KVCommand
			for j := u.ptr; j != nil && j.val.(*State).ind <= n; j = j.next {
				phi = j.val.(*State).cmd
			}

			// append only the last update of a particular key
			log = append(log, phi)
			(*avl.aux)[u.key].visited = true
		}

		if u.ind > p && u.left != nil {
			queue = append(queue, u.left)
		}
		if u.ind < n && u.right != nil {
			queue = append(queue, u.right)
		}
	}
	return log
}
