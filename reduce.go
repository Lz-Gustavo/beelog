package main

// Reducer indexes different log compact strategies ...
type Reducer int8

const (
	// Bubbler ...
	Bubbler Reducer = iota

	// Greedy ...
	Greedy
)

// BubblerList is the simpliest algorithm.
func BubblerList(s Structure) {

}
