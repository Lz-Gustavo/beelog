package beelog

import (
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/Lz-Gustavo/beelog/pb"
)

type avlTreeEntry struct {
	ind uint64
	key string
	ptr *listNode

	left   *avlTreeEntry
	right  *avlTreeEntry
	height int
}

// AVLTreeHT ...
type AVLTreeHT struct {
	root *avlTreeEntry
	aux  *stateTable
	len  uint64
	mu   sync.RWMutex
	logData
}

// NewAVLTreeHT ...
func NewAVLTreeHT() *AVLTreeHT {
	ht := make(stateTable, 0)
	return &AVLTreeHT{
		aux:     &ht,
		logData: logData{config: DefaultLogConfig()},
	}
}

// NewAVLTreeHTWithConfig ...
func NewAVLTreeHTWithConfig(cfg *LogConfig) (*AVLTreeHT, error) {
	err := cfg.ValidateConfig()
	if err != nil {
		return nil, err
	}

	ht := make(stateTable, 0)
	return &AVLTreeHT{
		aux:     &ht,
		logData: logData{config: cfg},
	}, nil
}

// Str implements a BFS on the AVLTree, returning a string representation for the
// entire struct.
func (av *AVLTreeHT) Str() string {
	if av.Len() < 1 {
		return ""
	}
	av.mu.RLock()
	defer av.mu.RUnlock()

	nodes := []string{fmt.Sprintf("(%v|%v)", av.root.ind, av.root.key)}
	queue := &list{}
	queue.push(av.root)

	for queue.len != 0 {
		u := queue.pop().val.(*avlTreeEntry)
		for _, v := range []*avlTreeEntry{u.left, u.right} {
			if v != nil {
				str := fmt.Sprintf("(%v|%v)", v.ind, v.key)
				nodes = append(nodes, str)
				queue.push(v)
			}
		}
	}
	return strings.Join(nodes, ", ")
}

// Len returns the lenght, number of nodes on the tree.
func (av *AVLTreeHT) Len() uint64 {
	return av.len
}

// Log records the occurence of command 'cmd' on the provided index. Writes are
// mapped into a new node on the AVL tree, with a pointer to the newly inserted
// state update on the update list for its particular key.
func (av *AVLTreeHT) Log(cmd pb.Command) error {
	av.mu.Lock()
	defer av.mu.Unlock()

	if cmd.Op != pb.Command_SET {
		// TODO: treat 'av.first' attribution on GETs
		av.last = cmd.Id
		return av.mayTriggerReduce()
	}

	entry := &avlTreeEntry{
		ind: cmd.Id,
		key: cmd.Key,
	}

	// a write cmd always references a new state on the aux hash table
	st := &State{
		ind: cmd.Id,
		cmd: cmd,
	}

	_, exists := (*av.aux)[cmd.Key]
	if !exists {
		(*av.aux)[cmd.Key] = &list{}
	}

	// add state to the list of updates in that particular key
	lNode := (*av.aux)[cmd.Key].push(st)
	entry.ptr = lNode

	ok := av.insert(entry)
	if !ok {
		return errors.New("cannot insert equal keys on BSTs")
	}

	// adjust last index once inserted
	av.last = cmd.Id

	// Immediately recovery entirely reduces the log to its minimal format
	if av.config.Tick == Immediately {
		return av.ReduceLog(av.first, av.last)
	}
	return av.mayTriggerReduce()
}

// Recov returns a compacted log of commands, following the requested [p, n]
// interval if 'Delayed' reduce is configured. On different period configurations,
// the entire reduced log is always returned. On persistent configuration (i.e.
// 'inmem' false) the entire log is loaded and then unmarshaled, consider using
// 'RecovBytes' calls instead.
func (av *AVLTreeHT) Recov(p, n uint64) ([]pb.Command, error) {
	if n < p {
		return nil, errors.New("invalid interval request, 'n' must be >= 'p'")
	}
	av.mu.RLock()
	defer av.mu.RUnlock()

	if err := av.mayExecuteLazyReduce(p, n); err != nil {
		return nil, err
	}
	return av.retrieveLog()
}

// RecovBytes returns an already serialized log, parsed from persistent storage
// or marshaled from the in-memory state. Its the most efficient approach on persistent
// configuration, avoiding an extra marshaling step during recovery. The command
// interpretation from the byte stream follows a simple slicing protocol, where
// the size of each command is binary encoded before the raw pbuff.
func (av *AVLTreeHT) RecovBytes(p, n uint64) ([]byte, error) {
	if n < p {
		return nil, errors.New("invalid interval request, 'n' must be >= 'p'")
	}
	av.mu.RLock()
	defer av.mu.RUnlock()

	if err := av.mayExecuteLazyReduce(p, n); err != nil {
		return nil, err
	}
	return av.retrieveRawLog(p, n)
}

// ReduceLog applies the configured reduce algorithm and updates the current log state.
// Must only be called within mutual exclusion scope.
func (av *AVLTreeHT) ReduceLog(p, n uint64) error {
	cmds, err := ApplyReduceAlgo(av, av.config.Alg, p, n)
	if err != nil {
		return err
	}
	return av.updateLogState(cmds, p, n, false)
}

// mayTriggerReduce possibly triggers the reduce algorithm based on config params
// (e.g. interval period reached). Must only be called within mutual exclusion scope.
func (av *AVLTreeHT) mayTriggerReduce() error {
	if av.config.Tick != Interval {
		return nil
	}
	av.count++
	if av.count >= av.config.Period {
		av.count = 0
		return av.ReduceLog(av.first, av.last)
	}
	return nil
}

// mayExecuteLazyReduce triggers a reduce procedure if delayed config is set or first
// 'config.Period' wasnt reached yet.
func (av *AVLTreeHT) mayExecuteLazyReduce(p, n uint64) error {
	if av.config.Tick == Delayed {
		err := av.ReduceLog(p, n)
		if err != nil {
			return err
		}

	} else if av.config.Tick == Interval && !av.firstReduceExists() {
		// must reduce the entire structure, just the desired interval would
		// be incoherent with the Interval config
		err := av.ReduceLog(av.first, av.last)
		if err != nil {
			return err
		}
	}
	return nil
}

// insert recursively inserts a node on the tree structure on O(lg n) operations,
// where 'n' is the number of elements in the tree.
func (av *AVLTreeHT) insert(node *avlTreeEntry) bool {
	node.height = 1
	if av.root == nil {
		av.root = node
		av.len++
		av.first = node.ind
		return true
	}

	rt := av.recurInsert(av.root, node)
	if rt != nil {
		av.len++
		av.root = rt
		return true
	}
	return false
}

func (av *AVLTreeHT) rightRotate(root *avlTreeEntry) *avlTreeEntry {
	son := root.left
	gson := son.right

	// rotation
	son.right = root
	root.left = gson

	// update heights
	root.height = max(getHeight(root.left), getHeight(root.right)) + 1
	son.height = max(getHeight(son.left), getHeight(son.right)) + 1
	return son
}

func (av *AVLTreeHT) leftRotate(root *avlTreeEntry) *avlTreeEntry {
	son := root.right
	gson := son.left

	// rotation
	son.left = root
	root.right = gson

	// update heights
	root.height = max(getHeight(root.left), getHeight(root.right)) + 1
	son.height = max(getHeight(son.left), getHeight(son.right)) + 1
	return son
}

// recurInsert is a recursive procedure for insert operation.
// adapted from: https://www.geeksforgeeks.org/avl-tree-set-1-insertion/
func (av *AVLTreeHT) recurInsert(root, node *avlTreeEntry) *avlTreeEntry {
	if root == nil {
		return node
	}

	if node.ind < root.ind {
		root.left = av.recurInsert(root.left, node)

	} else if node.ind > root.ind {
		root.right = av.recurInsert(root.right, node)

	} else {
		// Equal keys are not allowed in BST
		return nil
	}

	root.height = 1 + max(getHeight(root.left), getHeight(root.right))

	// If this node becomes unbalanced, then there are 4 cases
	balance := getBalanceFactor(root)

	// Left Left Case
	if balance > 1 && node.ind < root.left.ind {
		return av.rightRotate(root)
	}

	// Right Right Case
	if balance < -1 && node.ind > root.right.ind {
		return av.leftRotate(root)
	}

	// Left Right Case
	if balance > 1 && node.ind > root.left.ind {
		root.left = av.leftRotate(root.left)
		return av.rightRotate(root)
	}

	// Right Left Case
	if balance < -1 && node.ind < root.right.ind {
		root.right = av.rightRotate(root.right)
		return av.leftRotate(root)
	}
	return root
}

func (av *AVLTreeHT) resetVisitedValues() {
	for _, list := range *av.aux {
		list.visited = false
	}
}

func getHeight(node *avlTreeEntry) int {
	if node == nil {
		return 0
	}
	return node.height
}

func getBalanceFactor(node *avlTreeEntry) int {
	if node == nil {
		return 0
	}
	return getHeight(node.left) - getHeight(node.right)
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func stringRecurBFS(queue []*avlTreeEntry, res []string) []string {
	if len(queue) == 0 {
		return res
	}

	str := fmt.Sprintf("%v", queue[0].ind)
	res = append(res, str)

	if queue[0].left != nil {
		queue = append(queue, queue[0].left)
	}
	if queue[0].right != nil {
		queue = append(queue, queue[0].right)
	}
	return stringRecurBFS(queue[1:], res)
}
