package main

// Generator ...
type Generator interface {
	Seed()
	Gen() Structure
}

// GenID ...
type GenID int8

const (
	// LogList ...
	LogList GenID = iota

	// LogDAG ...
	LogDAG
)

// TranslateGen ...
func TranslateGen(id GenID) Generator {
	switch id {
	case LogList:
		return &ListGenerator{}

	default:
		return nil
	}
}

// ListGenerator ...
type ListGenerator struct {
}

// Seed ...
func (lg *ListGenerator) Seed() {
}

// Gen ...
func (lg *ListGenerator) Gen() Structure {
	return nil
}

// Structure ...
type Structure interface {
	Str()
	Len()
	Hash()
}

// List ...
type List struct {
}
