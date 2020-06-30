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

type list struct {
	first   *listNode
	tail    *listNode
	len     uint64
	visited bool
}

// push inserts a new node with the argument value on the list, returning a
// reference to it.
func (l *list) push(v interface{}) *listNode {
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
func (l *list) pop() *listNode {
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

// similar to Floyd's tortoise and hare algorithm
func findMidInList(start, last *listNode) *listNode {
	if start == nil || last == nil {
		return nil
	}
	slow := start
	fast := start.next

	for fast != last {
		fast = fast.next
		if fast != last {
			slow = slow.next
			fast = fast.next
		}
	}
	return slow
}

// State represents a new state, a command execution happening on a certain
// consensus index, analogous to a logical clock event.
type State struct {
	ind uint64
	cmd pb.Command
}

// stateTable maps state updates for particular keys, stored as an underlying
// list of State.
type stateTable map[string]*list

// logData is the general data for each implementation of Structure interface
type logData struct {
	config      *LogConfig
	first, last uint64
	recentLog   *[]pb.Command // used only on Immediately inmem config
	count       uint32        // used on Interval config
}

func (ld *logData) retrieveLog() ([]pb.Command, error) {
	if ld.config.Inmem {
		return *ld.recentLog, nil
	}

	// recover from the most recent state at ld.config.Fname
	fd, err := os.OpenFile(ld.config.Fname, os.O_RDONLY, 0644)
	if err != nil {
		return nil, err
	}
	defer fd.Close()
	return UnmarshalLogFromReader(fd)
}

func (ld *logData) retrieveRawLog(p, n uint64) ([]byte, error) {
	var rd io.Reader
	if ld.config.Inmem {
		buff := bytes.NewBuffer(nil)
		err := MarshalLogIntoWriter(buff, ld.recentLog, p, n)
		if err != nil {
			return nil, err
		}
		rd = buff

	} else {
		fd, err := os.OpenFile(ld.config.Fname, os.O_RDONLY, 0644)
		if err != nil {
			return nil, err
		}
		defer fd.Close()
		rd = fd
	}

	logs, err := ioutil.ReadAll(rd)
	if err != nil {
		return nil, err
	}
	return logs, nil
}

func (ld *logData) updateLogState(lg []pb.Command, p, n uint64) error {
	if ld.config.Inmem {
		// update the most recent inmem log state
		ld.recentLog = &lg
		return nil
	}

	// update the current state at ld.config.Fname
	fd, err := os.OpenFile(ld.config.Fname, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer fd.Close()

	err = MarshalLogIntoWriter(fd, &lg, p, n)
	if err != nil {
		return err
	}
	return nil
}

// firstReduceExists is execute on Interval tick config, and checks if a ReduceLog
// procedure was already executed. False is returned if no recent reduced state is
// found (i.e. first 'ld.config.Period' wasnt reached yet).
func (ld *logData) firstReduceExists() bool {
	if ld.config.Inmem {
		return ld.recentLog != nil
	}

	// disk config, found any state file
	// TODO: verify if the found file has a matching interval?
	if _, exists := os.Stat(ld.config.Fname); exists == nil {
		return true
	}
	return false
}

type listEntry struct {
	ind uint64
	key string
	ptr *listNode
}

// ListHT ...
type ListHT struct {
	lt  *list
	aux *stateTable
	mu  sync.RWMutex
	logData
}

// NewListHT ...
func NewListHT() *ListHT {
	ht := make(stateTable, 0)
	return &ListHT{
		logData: logData{config: DefaultLogConfig()},
		lt:      &list{},
		aux:     &ht,
	}
}

// NewListHTWithConfig ...
func NewListHTWithConfig(cfg *LogConfig) (*ListHT, error) {
	err := cfg.ValidateConfig()
	if err != nil {
		return nil, err
	}

	ht := make(stateTable, 0)
	return &ListHT{
		logData: logData{config: cfg},
		lt:      &list{},
		aux:     &ht,
	}, nil
}

// Str returns a string representation of the list state, used for debug purposes.
func (l *ListHT) Str() string {
	l.mu.RLock()
	defer l.mu.RUnlock()

	var strs []string
	for i := l.lt.first; i != nil; i = i.next {
		strs = append(strs, fmt.Sprintf("%v->", i.val))
	}
	return strings.Join(strs, " ")
}

// Len returns the list length.
func (l *ListHT) Len() uint64 {
	return l.lt.len
}

// Log records the occurence of command 'cmd' on the provided index. Writes are as
//  a new node on the underlying liked list,  with a pointer to the newly inserted
// state update on the update list for its particular key..
func (l *ListHT) Log(index uint64, cmd pb.Command) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if cmd.Op != pb.Command_SET {
		// TODO: treat 'l.first' attribution on GETs
		l.last = index
		return l.mayTriggerReduce()
	}

	// TODO: Ensure same index for now. Log API will change in time
	cmd.Id = index

	entry := &listEntry{
		ind: index,
		key: cmd.Key,
	}

	// a write cmd always references a new state on the aux hash table
	st := &State{
		ind: index,
		cmd: cmd,
	}

	_, exists := (*l.aux)[cmd.Key]
	if !exists {
		(*l.aux)[cmd.Key] = &list{}
	}

	// add state to the list of updates in that particular key
	lNode := (*l.aux)[cmd.Key].push(st)
	entry.ptr = lNode

	// adjust first structure index
	if l.lt.tail == nil {
		l.first = entry.ind
	}

	// insert new entry on the main list
	l.lt.push(entry)

	// adjust last index once inserted
	l.last = index

	// immediately recovery entirely reduces the log to its minimal format
	if l.config.Tick == Immediately {
		return l.ReduceLog(l.first, l.last)
	}
	return l.mayTriggerReduce()
}

// Recov ... mention that when 'inmem' is true the persistent way is ineficient,
// considering use RecovBytes instead ...
func (l *ListHT) Recov(p, n uint64) ([]pb.Command, error) {
	if n < p {
		return nil, errors.New("invalid interval request, 'n' must be >= 'p'")
	}
	l.mu.RLock()
	defer l.mu.RUnlock()

	if err := l.mayExecuteLazyReduce(p, n); err != nil {
		return nil, err
	}
	return l.retrieveLog()
}

// RecovBytes ... returns an already serialized data, most efficient approach
// when 'l.config.Inmem == false' ... Describe the slicing protocol for pbuffs
func (l *ListHT) RecovBytes(p, n uint64) ([]byte, error) {
	if n < p {
		return nil, errors.New("invalid interval request, 'n' must be >= 'p'")
	}
	l.mu.RLock()
	defer l.mu.RUnlock()

	if err := l.mayExecuteLazyReduce(p, n); err != nil {
		return nil, err
	}
	return l.retrieveRawLog(p, n)
}

// ReduceLog ... is only launched on thread-safe routines ...
func (l *ListHT) ReduceLog(p, n uint64) error {
	cmds, err := ApplyReduceAlgo(l, l.config.Alg, p, n)
	if err != nil {
		return err
	}
	return l.updateLogState(cmds, p, n)
}

// mayTriggerReduce ... unsafe ... must be called from mutual exclusion ...
func (l *ListHT) mayTriggerReduce() error {
	if l.config.Tick != Interval {
		return nil
	}
	l.count++
	if l.count >= l.config.Period {
		l.count = 0
		return l.ReduceLog(l.first, l.last)
	}
	return nil
}

func (l *ListHT) mayExecuteLazyReduce(p, n uint64) error {
	// reduced if delayed config or first 'av.config.Period' wasnt reached yet
	if l.config.Tick == Delayed {
		err := l.ReduceLog(p, n)
		if err != nil {
			return err
		}

	} else if l.config.Tick == Interval && !l.firstReduceExists() {
		// must reduce the entire structure, just the desired interval would
		// be incoherent with the Interval config
		err := l.ReduceLog(l.first, l.last)
		if err != nil {
			return err
		}
	}
	return nil
}

func (l *ListHT) searchEntryNodeByIndex(ind uint64) *listNode {
	start := l.lt.first
	last := l.lt.tail

	var mid *listNode
	for last != start {

		mid = findMidInList(start, last)
		ent := mid.val.(*listEntry)

		// found in mid
		if ent.ind == ind {
			return mid

			// greater
		} else if ind > ent.ind {
			start = mid.next

			// less
		} else {
			last = mid
		}
	}
	// instead of nil, return the nearest element
	return mid
}

func (l *ListHT) resetVisitedValues() {
	for _, list := range *l.aux {
		list.visited = false
	}
}

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
func (av *AVLTreeHT) Log(index uint64, cmd pb.Command) error {
	av.mu.Lock()
	defer av.mu.Unlock()

	if cmd.Op != pb.Command_SET {
		// TODO: treat 'av.first' attribution on GETs
		av.last = index
		return av.mayTriggerReduce()
	}

	// TODO: Ensure same index for now. Log API will change in time
	cmd.Id = index

	entry := &avlTreeEntry{
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
	av.last = index

	// Immediately recovery entirely reduces the log to its minimal format
	if av.config.Tick == Immediately {
		return av.ReduceLog(av.first, av.last)
	}
	return av.mayTriggerReduce()
}

// Recov ... mention that when 'inmem' is true the persistent way is ineficient,
// considering use RecovBytes instead ...
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

// RecovBytes ... returns an already serialized data, most efficient approach
// when 'av.config.Inmem == false' ... Describe the slicing protocol for pbuffs
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

// ReduceLog ... is only launched on thread-safe routines ...
func (av *AVLTreeHT) ReduceLog(p, n uint64) error {
	cmds, err := ApplyReduceAlgo(av, av.config.Alg, p, n)
	if err != nil {
		return err
	}
	return av.updateLogState(cmds, p, n)
}

// mayTriggerReduce ... unsafe ... must be called from mutual exclusion ...
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

func (av *AVLTreeHT) mayExecuteLazyReduce(p, n uint64) error {
	// reduced if delayed config or first 'av.config.Period' wasnt reached yet
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

// UnmarshalLogFromReader ...
func UnmarshalLogFromReader(logRd io.Reader) ([]pb.Command, error) {
	// read the retrieved log interval
	var f, l uint64
	_, err := fmt.Fscanf(logRd, "%d\n%d\n", &f, &l)
	if err != nil {
		return nil, err
	}

	cmds := make([]pb.Command, 0, l-f)
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
		cmds = append(cmds, *c)
	}
	return cmds, nil
}

// MarshalLogIntoWriter ... describe the slicing protocol for logs and protobuffs
// on disk (delimiters, binary size, raw cmd)
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
