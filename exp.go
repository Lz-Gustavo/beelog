package main

// TestCase ...
type TestCase struct {
	entries  Generator
	strategy Reducer
}

func newTestCase(g GenID, r Reducer) *TestCase {
	return &TestCase{
		entries:  TranslateGen(g),
		strategy: r,
	}
}

func (tc *TestCase) run() {
}
