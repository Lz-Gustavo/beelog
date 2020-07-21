package beelog

import (
	"errors"

	"github.com/Lz-Gustavo/beelog/pb"
)

// Reducer indexes different log compact strategies.
type Reducer int8

const (
	// GreedyLt implements a greedy search over LogList structures. After finding
	// the node of the lower bound in the requested interval through a binary search,
	// the algorithm continues iterating over the key update list until the requested
	// upper bound is surpassed.
	GreedyLt Reducer = iota

	// GreedyArray ...
	GreedyArray

	// GreedyAvl recursively implements a greedy search over LogAVL structures.
	// On each iteration, the algorithm continues iterating over the key update
	// list until the requested upper bound is surpassed.
	GreedyAvl

	// IterBFSAvl is an iterative version of GreedyAVL, adapted from the iterative
	// implementation of the BFS algorithm presented in CLRS09.
	IterBFSAvl

	// IterDFSAvl is a small variation of IterBFSAvl, simply replacing FIFO for
	// LIFO semantics. The stack is implemented as an underlying slice.
	IterDFSAvl

	// IterCircBuff ...
	IterCircBuff

	// IterConcTable ...
	IterConcTable
)

// ApplyReduceAlgo executes over a Structure the choosen Reducer algorithm, returning
// a compacted log of commands within the requested [p, n] interval.
//
//  IMPORTANT: Unsafe operation. Use Recov() calls for a safe log retrieval.
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
			return nil, errors.New("unsupported reduce algorithm for an AVLTreeHT structure")
		}
		break

	case *ListHT:
		switch r {
		case GreedyLt:
			log = GreedyListHT(st, p, n)
			break

		default:
			return nil, errors.New("unsupported reduce algorithm for a ListHT structure")
		}
		break

	case *ArrayHT:
		switch r {
		case GreedyArray:
			log = GreedyArrayHT(st, p, n)
			break

		default:
			return nil, errors.New("unsupported reduce algorithm for an ArrayHT structure")
		}
		break

	case *CircBuffHT:
		switch r {
		case IterCircBuff:
			// a copy is created on concurrency unsafe scope. Check 'cb.ExecuteReduceAlgOnCopy'
			// implementation for a safe alternative.
			cp := st.createStateCopy()
			log = IterCircBuffHT(&cp)
			break

		default:
			return nil, errors.New("unsupported reduce algorithm for a CircBuffHT structure")
		}

	case *ConcTable:
		switch r {
		case IterConcTable:
			view := st.retrieveCurrentViewCopy()
			log = IterConcTableOnView(&view)

		default:
			return nil, errors.New("unsupported reduce algorithm for a ConcTable structure")
		}

	default:
		return nil, errors.New("unsupported log datastructure")
	}
	return log, nil
}

// BubblerList doesnt provide an optimal solution.
//
// NOTE: The list must be represented on the oposite order. Deprecated for
// now, will be later removed when convenient.
func BubblerList(l *ListHT, p, n uint64) []pb.Command {
	var (
		rm bool
		rc uint64
		i  *listNode
	)
	log := make([]pb.Command, 0)

	for {
		rm = false
		for i = l.lt.first; i != nil && i.next != nil; i = i.next {

			cmd := i.val.(State).cmd
			neigh := i.next.val.(State).cmd

			// Subsequent write operations over the same key
			if cmd.Key == neigh.Key {
				i.next = i.next.next
				rm = true
				rc++
			}
		}
		l.lt.tail = i

		if !rm {
			break
		}
	}
	l.lt.len -= rc

	for i = l.lt.first; i != nil; i = i.next {
		st := i.val.(State)
		if st.ind >= p && st.ind <= n {
			log = append(log, st.cmd)
		}
	}
	return log
}

// OldGreedyList returns an optimal solution.
//
// NOTE: The list must be represented on the oposite order. Deprecated for
// now, will be later removed when convenient.
func OldGreedyList(l *ListHT, p, n uint64) []pb.Command {
	var (
		rc   uint64
		i, j *listNode
	)
	log := make([]pb.Command, 0)

	// iterator i can reach nil value on the last j iteration
	for i = l.lt.first; i != nil && i.next != nil; i = i.next {

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
	l.lt.len -= rc
	return log
}

// GreedyListHT implements a binary search, then a linear greedy search on top
// of LogLists.
func GreedyListHT(l *ListHT, p, n uint64) []pb.Command {
	log := []pb.Command{}
	l.resetVisitedValues()
	first := l.searchEntryNodeByIndex(p)

	for i := first; i != nil; i = i.next {
		ent := i.val.(*listEntry)

		// reached the last index position
		if ent.ind > n {
			break
		}
		st := (*l.aux)[ent.key]

		// current key state not yet satisfied in log
		if !st.visited {
			var phi pb.Command
			for j := ent.ptr; j != nil && j.val.(*State).ind <= n; j = j.next {
				phi = j.val.(*State).cmd
			}

			// append only the last update of a particular key
			log = append(log, phi)
			st.visited = true
		}
	}
	return log
}

// GreedyArrayHT implements a binary search, then a linear greedy search on top
// of an 'array-backed' structure.
func GreedyArrayHT(ar *ArrayHT, p, n uint64) []pb.Command {
	log := []pb.Command{}
	ar.resetVisitedValues()
	first := ar.searchEntryPosByIndex(p)

	for i := first; i < ar.Len(); i++ {
		ent := (*ar.arr)[i]

		// reached the last index position
		if ent.ind > n {
			break
		}
		st := (*ar.aux)[ent.key]

		// current key state not yet satisfied in log
		if !st.visited {
			var phi pb.Command
			for j := ent.ptr; j != nil && j.val.(*State).ind <= n; j = j.next {
				phi = j.val.(*State).cmd
			}

			// append only the last update of a particular key
			log = append(log, phi)
			st.visited = true
		}
	}
	return log
}

// GreedyAVLTreeHT implements a recursive search on top of LogAVL structs.
func GreedyAVLTreeHT(avl *AVLTreeHT, p, n uint64) []pb.Command {
	log := []pb.Command{}
	avl.resetVisitedValues()
	greedyRecur(avl, avl.root, p, n, &log)
	return log
}

func greedyRecur(avl *AVLTreeHT, k *avlTreeEntry, p, n uint64, log *[]pb.Command) {
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
	avl.resetVisitedValues()
	queue := []*avlTreeEntry{avl.root}
	var u *avlTreeEntry

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
	avl.resetVisitedValues()
	queue := []*avlTreeEntry{avl.root}
	var u *avlTreeEntry

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

// IterCircBuffHT executes on top of a local copy of the log structure, parsing
// the entire structure without any interval bound. During iteration, ignores
// repetitive commands to a key already satisfied in log.
func IterCircBuffHT(cp *buffCopy) []pb.Command {
	log := []pb.Command{}
	visited := make(map[string]bool, 0)

	i := 0
	for i < (*cp).len {
		// negative values already account circular reference. In order to change
		// order (oldest cmmd first), just switch to:
		//   pos := modInt((cur - len + i), cap)

		pos := modInt(((*cp).cur - 1 - i), (*cp).cap)
		ent := (*cp).buf[pos]
		st := (*cp).tbl[ent.key]

		if _, ok := visited[ent.key]; !ok {
			visited[ent.key] = true
			log = append(log, st.cmd)
		}
		i++
	}
	return log
}

// IterConcTableOnView ...
func IterConcTableOnView(tbl *minStateTable) []pb.Command {
	log := []pb.Command{}
	for _, st := range *tbl {
		log = append(log, st.cmd)
	}
	return log
}
