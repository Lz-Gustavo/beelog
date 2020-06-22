package beelog

import (
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/Lz-Gustavo/beelog/pb"
)

// Structure is an abstraction for the different log representation structures
// implemented.
type Structure interface {
	Str() string
	Len() int
	Log(index uint64, cmd pb.Command) error
}

type listNode struct {
	val  interface{}
	next *listNode
}

// List ...
type List struct {
	first   *listNode
	tail    *listNode
	len     int
	visited bool
}

// Str returns a string representation of the list state, used for debug purposes.
func (l *List) Str() string {
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

// Log records the occurence of command 'cmd' on the provided index, as a new
// node on the underlying liked list.
func (l *List) Log(index uint64, cmd pb.Command) error {
	if cmd.Op != pb.Command_SET {
		return nil
	}
	st := State{
		ind: index,
		cmd: cmd,
	}

	l.push(st)
	return nil
}

// push inserts a new node with the argument value on the list, returning a
// reference to it.
func (l *List) push(v interface{}) *listNode {
	nd := &listNode{
		val: v,
	}

	// empty list, first element
	if l.tail == nil {
		l.first = nd
		l.tail = nd
	} else {
		l.tail.next = nd
		l.tail = nd
	}
	l.len++
	return nd
}

// pop removes and returns the first element on the list.
func (l *List) pop() *listNode {
	if l.first == nil {
		return nil
	}

	l.len--
	if l.first == l.tail {
		aux := l.first
		l.first = nil
		l.tail = nil
		return aux
	}
	aux := l.first
	l.first = aux.next
	return aux
}

// State represents a new state, a command execution happening on a certain
// consensus index, analogous to a logical clock event.
type State struct {
	ind uint64
	cmd pb.Command
}

// stateTable stores state updates for particular keys.
type stateTable map[string]*List

type avlTreeNode struct {
	ind uint64
	key string
	ptr *listNode

	left   *avlTreeNode
	right  *avlTreeNode
	height int
}

// AVLTreeHT ...
type AVLTreeHT struct {
	root *avlTreeNode
	aux  *stateTable
	len  int
	mu   sync.RWMutex
}

// NewAVLTreeHT ...
func NewAVLTreeHT() *AVLTreeHT {
	ht := make(stateTable, 0)
	return &AVLTreeHT{
		aux: &ht,
		len: 0,
	}
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
	queue := &List{}
	queue.push(av.root)

	for queue.len != 0 {

		u := queue.pop().val.(*avlTreeNode)
		for _, v := range []*avlTreeNode{u.left, u.right} {
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
func (av *AVLTreeHT) Len() int {
	return av.len
}

// Log records the occurence of command 'cmd' on the provided index. Writes are
// mapped into a new node on the AVL tree, with a pointer to the newly inserted
// state update on the update list for its particular key.
func (av *AVLTreeHT) Log(index uint64, cmd pb.Command) error {
	if cmd.Op != pb.Command_SET {
		return nil
	}
	av.mu.Lock()
	defer av.mu.Unlock()

	aNode := &avlTreeNode{
		ind: index,
		key: cmd.Key,
	}

	// a write cmd always references a new state on the aux hash table
	st := &State{
		ind: index,
		cmd: cmd,
	}

	_, exists := (*av.aux)[cmd.Key]
	if !exists {
		(*av.aux)[cmd.Key] = &List{}
	}

	// add state to the list of updates in that particular key
	lNode := (*av.aux)[cmd.Key].push(st)
	aNode.ptr = lNode

	ok := av.insert(aNode)
	if !ok {
		return errors.New("cannot insert equal keys on BSTs")
	}
	return nil
}

// insert recursively inserts a node on the tree structure on O(lg n) operations,
// where 'n' is the number of elements in the tree.
func (av *AVLTreeHT) insert(node *avlTreeNode) bool {
	node.height = 1
	if av.root == nil {
		av.root = node
		av.len++
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

func (av *AVLTreeHT) rightRotate(root *avlTreeNode) *avlTreeNode {
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

func (av *AVLTreeHT) leftRotate(root *avlTreeNode) *avlTreeNode {
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
func (av *AVLTreeHT) recurInsert(root, node *avlTreeNode) *avlTreeNode {
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

func getHeight(node *avlTreeNode) int {
	if node == nil {
		return 0
	}
	return node.height
}

func getBalanceFactor(node *avlTreeNode) int {
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

func stringRecurBFS(queue []*avlTreeNode, res []string) []string {
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
