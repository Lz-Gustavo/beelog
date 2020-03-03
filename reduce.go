package main

import (
	"errors"
)

// Reducer indexes different log compact strategies ...
type Reducer int8

const (
	// Bubbler ...
	Bubbler Reducer = iota

	// Greedy ...
	Greedy
)

// ApplyReduceAlgo ...
func ApplyReduceAlgo(s Structure, r Reducer) error {
	switch r {
	case Bubbler:
		BubblerList(s)
		break

	case Greedy:
		GreedyList(s)
		break

	default:
		return errors.New("unsupported reduce algorithm")
	}
	return nil
}

// BubblerList is the simpliest algorithm.
func BubblerList(s Structure) {
}

// GreedyList returns an optimal solution...
func GreedyList(s Structure) {
}
