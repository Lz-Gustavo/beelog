package beelog

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strings"
	"sync"

	"github.com/Lz-Gustavo/beelog/pb"

	"github.com/golang/protobuf/proto"
)

// Structure is an abstraction for the different log representation structures
// implemented.
type Structure interface {
	Str() string
	Len() uint64
	Log(index uint64, cmd pb.Command) error
	Recov(p, n uint64) ([]pb.Command, error)
	RecovBytes(p, n uint64) ([]byte, error)
}

type listNode struct {
	val  interface{}
	next *listNode
}

// List ...
type List struct {
	first   *listNode
	tail    *listNode
	len     uint64
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
func (l *List) Len() uint64 {
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

// Recov ...
func (l *List) Recov(p, n uint64) ([]pb.Command, error) {
	return nil, nil
}

// RecovBytes ...
func (l *List) RecovBytes(p, n uint64) ([]byte, error) {
	return nil, nil
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
	len  uint64
	mu   sync.RWMutex

	config      *LogConfig
	first, last uint64
	recentLog   *[]pb.Command // used only on Immediately inmem config
}

// NewAVLTreeHT ...
func NewAVLTreeHT() *AVLTreeHT {
	ht := make(stateTable, 0)
	return &AVLTreeHT{
		aux:    &ht,
		len:    0,
		config: DefaultLogConfig(),
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
		aux:    &ht,
		len:    0,
		config: cfg,
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
func (av *AVLTreeHT) Len() uint64 {
	return av.len
}

// Log records the occurence of command 'cmd' on the provided index. Writes are
// mapped into a new node on the AVL tree, with a pointer to the newly inserted
// state update on the update list for its particular key.
func (av *AVLTreeHT) Log(index uint64, cmd pb.Command) error {
	av.mu.Lock()
	defer av.mu.Unlock()

	if cmd.Op != pb.Command_SET {
		// TODO: treat 'av.first' attribution on GETs
		av.last = index
		return nil
	}

	// TODO: Ensure same index for now. Log API will change in time
	cmd.Id = index

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

	// adjust last index once inserted
	av.last = index

	if av.config.Tick == Immediately {
		return av.ReduceLog()
	}
	return nil
}

// Recov ... mention that when 'inmem' is true the persistent way is ineficient,
// considering use RecovBytes instead ...
func (av *AVLTreeHT) Recov(p, n uint64) ([]pb.Command, error) {
	if n < p {
		return nil, errors.New("invalid interval request, 'n' must be >= 'p'")
	}
	av.mu.RLock()
	defer av.mu.RUnlock()

	// postponed log reduce now being executed ...
	if av.config.Tick == Delayed {
		err := av.ReduceLog()
		if err != nil {
			return nil, err
		}
	}

	if av.config.Inmem {
		return RetainLogInterval(av.recentLog, p, n), nil
	}

	// recover from the most recent state at av.config.Fname
	fd, err := os.OpenFile(av.config.Fname, os.O_RDONLY, 0644)
	if err != nil {
		return nil, err
	}
	defer fd.Close()
	return RetainLogIntervalWhileUnmarshaling(fd, p, n)
}

// RecovBytes ... returns an already serialized data, most efficient approach
// when 'av.config.Inmem == false' ... Describe the slicing protocol for pbuffs
func (av *AVLTreeHT) RecovBytes(p, n uint64) ([]byte, error) {
	if n < p {
		return nil, errors.New("invalid interval request, 'n' must be >= 'p'")
	}
	av.mu.RLock()
	defer av.mu.RUnlock()

	// postponed log reduce now being executed ...
	if av.config.Tick == Delayed {
		err := av.ReduceLog()
		if err != nil {
			return nil, err
		}
	}

	var log *[]pb.Command
	if av.config.Inmem {
		cmds := RetainLogInterval(av.recentLog, p, n)
		log = &cmds

	} else {
		fd, err := os.OpenFile(av.config.Fname, os.O_RDONLY, 0644)
		if err != nil {
			return nil, err
		}
		defer fd.Close()

		// TODO: Implement a more efficient way to determine which commands are
		// within the requested interval [p, n] without serializing entire log again
		cmds, err := RetainLogIntervalWhileUnmarshaling(fd, p, n)
		if err != nil {
			return nil, err
		}
		log = &cmds
	}

	buff := bytes.NewBuffer(nil)
	err := MarshalLogIntoWriter(buff, log, p, n)
	if err != nil {
		return nil, err
	}

	logs, err := ioutil.ReadAll(buff)
	if err != nil {
		return nil, err
	}
	return logs, nil
}

// ReduceLog ... is only launched on thread-safe routines ... describe the slicing
// protocol for logs and protobuffs on disk (delimiters, binary size, raw cmd)
func (av *AVLTreeHT) ReduceLog() error {
	cmds, err := ApplyReduceAlgo(av, av.config.Alg, av.first, av.last)
	if err != nil {
		return err
	}

	if av.config.Inmem {
		// update the most recent inmem log state
		av.recentLog = &cmds
		return nil
	}

	// update the current state at av.config.Fname
	fd, err := os.OpenFile(av.config.Fname, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer fd.Close()

	err = MarshalLogIntoWriter(fd, &cmds, av.first, av.last)
	if err != nil {
		return err
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

// RetainLogInterval receives an entire log and returns the corresponding log
// matching [p, n] indexes.
func RetainLogInterval(log *[]pb.Command, p, n uint64) []pb.Command {
	cmds := make([]pb.Command, 0, n-p)

	// TODO: Later improve retrieve algorithm, exploiting the pre-ordering of
	// commands based on c.Id. The idea is to simply identify the first and last
	// underlying indexes and return a subslice copy.
	for _, c := range *log {
		if c.Id >= p && c.Id <= n {
			cmds = append(cmds, c)
		}
	}
	return cmds
}

// RetainLogIntervalWhileUnmarshaling ...
func RetainLogIntervalWhileUnmarshaling(logRd io.Reader, p, n uint64) ([]pb.Command, error) {
	// read the retrieved log interval
	var f, l uint64
	_, err := fmt.Fscanf(logRd, "%d\n%d\n", &f, &l)
	if err != nil {
		return nil, err
	}

	cmds := make([]pb.Command, 0, n-p)
	for {
		var commandLength int32
		err := binary.Read(logRd, binary.BigEndian, &commandLength)
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}

		serializedCmd := make([]byte, commandLength)
		_, err = logRd.Read(serializedCmd)
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}

		c := &pb.Command{}
		err = proto.Unmarshal(serializedCmd, c)
		if err != nil {
			return nil, err
		}

		// Within the requested interval? If not will be later GC
		if c.Id >= p && c.Id <= n {
			cmds = append(cmds, *c)
		}
	}
	return cmds, nil
}

// MarshalLogIntoWriter ...
func MarshalLogIntoWriter(logWr io.Writer, log *[]pb.Command, p, n uint64) error {
	// write requested delimiters for the current state
	_, err := fmt.Fprintf(logWr, "%d\n%d\n", p, n)
	if err != nil {
		return err
	}

	for _, c := range *log {
		raw, err := proto.Marshal(&c)
		if err != nil {
			return err
		}

		// writing size of each serialized message as streaming delimiter
		err = binary.Write(logWr, binary.BigEndian, int32(len(raw)))
		if err != nil {
			return err
		}

		_, err = logWr.Write(raw)
		if err != nil {
			return err
		}
	}
	return nil
}
