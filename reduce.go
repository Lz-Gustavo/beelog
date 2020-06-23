package beelog

import (
	"errors"

	"github.com/Lz-Gustavo/beelog/pb"
)

// Reducer indexes different log compact strategies.
type Reducer int8

const (
	// BubblerLt is similar to a bubble sort implementation, in the sense
	// that command dependencies are identified on each iteration considering
	// the result of the prior. Does not provided an optimal solution unlike
	// the others.
	BubblerLt Reducer = iota

	// GreedyLt is a greedy approach of the first algorithm. On each iteration,
	// the algorithm continues iterating over the list removing any prior
	// occurence of writes on that particular key.
	GreedyLt

	// GreedyAvl recursively implements a greedy search over LogAVL structures.
	// On each iteration, the algorithm continues iterating over the key update
	// list until the request upper bound is surpassed.
	GreedyAvl

	// IterBFSAvl is an iterative version of GreedyAVL, adapted from the iterative
	// implementation of the BFS algorithm presented in CLRS09.
	IterBFSAvl

	// IterDFSAvl is a small variation of IterBFSAvl, simply replacing FIFO for
	// LIFO semantics. The stack is implemented as an underlying slice.
	IterDFSAvl
)

// ApplyReduceAlgo executes over a Structure the choosen Reducer algorithm, returning
// a compacted log of commands within the requested [p, n] interval.
func ApplyReduceAlgo(s Structure, r Reducer, p, n uint64) ([]pb.Command, error) {
	if s.Len() < 1 {
		return nil, errors.New("empty structure")
	}

	var log []pb.Command
	switch st := s.(type) {
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

	default:
		return nil, errors.New("unsupported log datastructure")
	}
	return log, nil
}

// BubblerList is the simpliest algorithm.
func BubblerList(l *List, p, n uint64) []pb.Command {
	var (
		rm bool
		rc uint64
		i  *listNode
	)
	log := make([]pb.Command, 0)

	for {
		rm = false
		for i = l.first; i != nil && i.next != nil; i = i.next {

			cmd := i.val.(State).cmd
			neigh := i.next.val.(State).cmd

			// Subsequent write operations over the same key
			if cmd.Key == neigh.Key {
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

// GreedyList returns an optimal solution.
func GreedyList(l *List, p, n uint64) []pb.Command {
	var (
		rc   uint64
		i, j *listNode
	)
	log := make([]pb.Command, 0)

	// iterator i can reach nil value on the last j iteration
	for i = l.first; i != nil && i.next != nil; i = i.next {

		st := i.val.(State)
		priorNeigh := i
		for j = i.next; j != nil; j = j.next {

			neigh := j.val.(State)

			// subsequent write operations over the same key
			if st.cmd.Key == neigh.cmd.Key {
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

// GreedyAVLTreeHT implements a recursive search on top of LogAVL structs.
func GreedyAVLTreeHT(avl *AVLTreeHT, p, n uint64) []pb.Command {
	log := []pb.Command{}
	avl.mu.RLock()
	defer avl.mu.RUnlock()

	avl.resetVisitedValues()
	greedyRecur(avl, avl.root, p, n, &log)
	return log
}

func greedyRecur(avl *AVLTreeHT, k *avlTreeNode, p, n uint64, log *[]pb.Command) {

	// nil or key already satisfied in the log
	if k == nil {
		return
	}

	// index in [p, n] interval and key not already satisfied on the log
	if !(*avl.aux)[k.key].visited && k.ind >= p && k.ind <= n {

		var phi pb.Command
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

// IterBFSAVLTreeHT is an iterative variantion of an GreedyAVL based on BFS.
func IterBFSAVLTreeHT(avl *AVLTreeHT, p, n uint64) []pb.Command {
	log := []pb.Command{}
	avl.mu.RLock()
	defer avl.mu.RUnlock()

	avl.resetVisitedValues()
	queue := []*avlTreeNode{avl.root}
	var u *avlTreeNode

	for len(queue) != 0 {

		u, queue = queue[0], queue[1:]

		// index in [p, n] interval and key not already satisfied on the log
		if !(*avl.aux)[u.key].visited && u.ind >= p && u.ind <= n {

			var phi pb.Command
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

// IterDFSAVLTreeHT is an iterative variantion of an GreedyAVL based on DFS.
func IterDFSAVLTreeHT(avl *AVLTreeHT, p, n uint64) []pb.Command {
	log := []pb.Command{}
	avl.mu.RLock()
	defer avl.mu.RUnlock()

	avl.resetVisitedValues()
	queue := []*avlTreeNode{avl.root}
	var u *avlTreeNode

	for ln := len(queue); ln != 0; ln = len(queue) {

		u, queue = queue[ln-1], queue[:ln-1]

		// index in [p, n] interval and key not already satisfied on the log
		if !(*avl.aux)[u.key].visited && u.ind >= p && u.ind <= n {

			var phi pb.Command
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
