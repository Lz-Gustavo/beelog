package main

import (
	"fmt"
	"strings"
)

// Structure ...
type Structure interface {
	Str() string
	Len() int
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

// Push ...
func (l *List) push(v interface{}) *listNode {
	nd := &listNode{
		val: v,
	}
	l.tail.next = nd
	l.tail = nd
	l.len++
	return nd
}

// State represents a ...
type State struct {
	ind int
	cmd KVCommand
}

// stateTable stores ...
type stateTable map[int]*List

type avlTreeNode struct {
	ind int
	key int
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
}

// Str ...
func (av *AVLTreeHT) Str() string {
	return ""
}

// Len ...
func (av *AVLTreeHT) Len() int {
	return av.len
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

func (av *AVLTreeHT) insert(node *avlTreeNode) bool {
	if av.root == nil {
		node.height = 1
		av.len++
		av.root = node
		return true
	}

	ok := av.recurInsert(av.root, node) != nil
	if ok {
		av.len++
	}
	return ok
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
