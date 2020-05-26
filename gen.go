package main

import (
	"bufio"
	"errors"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"time"
)

// StructID ...
type StructID int8

const (
	// LogList ...
	LogList StructID = iota

	// LogAVL ...
	LogAVL

	// LogDAG ...
	LogDAG
)

// Generator ...
type Generator func(n, wrt, dif int) (Structure, error)

// TranslateGen ...
func TranslateGen(id StructID) Generator {
	switch id {
	case LogList:
		return ListGen

	case LogAVL:
		return AVLTreeHTGen

	default:
		return nil
	}
}

// Operation ...
type Operation uint8

const (
	// Read ...
	Read Operation = iota

	// Write ...
	Write
)

// KVCommand ...
type KVCommand struct {
	op    Operation
	key   int
	value uint32
}

// ListGen ...
func ListGen(n, wrt, dif int) (Structure, error) {

	srand := rand.NewSource(time.Now().UnixNano())
	r := rand.New(srand)
	l := &List{len: n}

	for i := 0; i < n; i++ {

		if cn := r.Intn(100); cn < wrt {
			cmd := KVCommand{
				key:   r.Intn(dif),
				value: r.Uint32(),
				op:    Write,
			}

			// the list is represented on the oposite order
			l.Log(n-1-i, cmd)

		} else {
			continue
		}
	}
	return l, nil
}

// AVLTreeHTGen ...
func AVLTreeHTGen(n, wrt, dif int) (Structure, error) {

	srand := rand.NewSource(time.Now().UnixNano())
	r := rand.New(srand)
	avl := NewAVLTreeHT()

	for i := 0; i < n; i++ {

		// only WRITE operations are recorded on the tree
		if cn := r.Intn(100); cn < wrt {
			cmd := KVCommand{
				key:   r.Intn(dif),
				value: r.Uint32(),
				op:    Write,
			}

			err := avl.Log(i, cmd)
			if err != nil {
				return nil, err
			}

		} else {
			continue
		}
	}
	return avl, nil
}

// Constructor ...
type Constructor func(fn string) (Structure, int, error)

// TranslateConst ...
func TranslateConst(id StructID) Constructor {
	switch id {
	case LogList:
		// TODO:
		return nil

	case LogAVL:
		return AVLTreeHTConst

	default:
		return nil
	}
}

// AVLTreeHTConst ...
func AVLTreeHTConst(fn string) (Structure, int, error) {

	log, err := parseLog(fn)
	if err != nil {
		return nil, 0, err
	}

	ln := len(log)
	if ln == 0 {
		return nil, 0, errors.New("empty logfile informed")
	}
	avl := NewAVLTreeHT()

	for i, cmd := range log {

		err := avl.Log(i, cmd)
		if err != nil {
			return nil, 0, err
		}
	}
	return avl, ln, nil
}

// parseLog interprets the custom defined log format, equivalent to the string
// representation of the KVCommand struct.
func parseLog(fn string) ([]KVCommand, error) {
	fd, err := os.Open(fn)
	if err != nil {
		return nil, err
	}
	defer fd.Close()

	log := make([]KVCommand, 0)
	sc := bufio.NewScanner(fd)

	for sc.Scan() {
		line := sc.Text()
		fields := strings.Split(line, " ")

		op, _ := strconv.Atoi(fields[0])
		key, _ := strconv.Atoi(fields[1])
		value, _ := strconv.ParseInt(fields[2], 10, 32)

		cmd := KVCommand{
			op:    Operation(op),
			key:   key,
			value: uint32(value),
		}
		log = append(log, cmd)
	}
	return log, nil
}
