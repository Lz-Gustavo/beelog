package main

import (
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"
)

var (
	testCases []*TestCase
)

func init() {
	fs, err := readCurrentDir()
	if err != nil {
		log.Fatalln("could not load current dir:", err.Error())
	}
	err = initTestCases(fs)
	if err != nil {
		log.Fatalln("could not init test cases:", err.Error())
	}
}

func main() {

	for _, t := range testCases {
		err := t.run()
		if err != nil {
			log.Printf("Error on testcase %s: %s\n", t.Name, err.Error())
		}
	}
}

func readCurrentDir() ([]string, error) {

	ent, err := ioutil.ReadDir("./")
	if err != nil {
		return nil, err
	}

	var fns []string
	for _, f := range ent {
		if !f.IsDir() && strings.Compare(filepath.Ext(f.Name()), ".toml") == 0 {
			fns = append(fns, f.Name())
		}
	}
	return fns, nil
}

func initTestCases(filenames []string) error {

	for _, f := range filenames {
		fd, err := os.Open(f)
		if err != nil {
			return err
		}

		c, err := ioutil.ReadAll(fd)
		if err != nil {
			return err
		}

		tc, err := newTestCase(c)
		if err != nil {
			return err
		}
		testCases = append(testCases, tc)
	}
	return nil
}
