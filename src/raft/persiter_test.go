package raft

import (
	"bytes"
	"labgob"
	"reflect"
	"testing"
)

func TestPersister(t *testing.T) {

	term1 :=1
	voted1 := 2
	logs := []int{0,1,2}

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(term1)
	e.Encode(voted1)
	e.Encode(logs)
	data := w.Bytes()

	//decode
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var term2, vote2 int
	var logs2 []int
	d.Decode(&term2)
	d.Decode(&vote2)
	d.Decode(&logs2)
	if term2 != term1 || voted1 != vote2 || !reflect.DeepEqual(logs, logs2) {
		t.Fatalf("error")
	}
}
