package driver

import (
	"context"
	"testing"
)

func TestMemoryTxnAbortNotVisible(t *testing.T) {
	m := NewMemory()
	ctx := context.Background()
	txn, err := m.BeginTxn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := txn.Publish(ctx, Record{Topic: "t", Value: []byte("x")}); err != nil {
		t.Fatal(err)
	}
	if err := txn.Abort(ctx); err != nil {
		t.Fatal(err)
	}
	if n := len(m.TopicRecords("t")); n != 0 {
		t.Fatalf("aborted records visible: %d", n)
	}
}

func TestMemoryTxnCommitVisible(t *testing.T) {
	m := NewMemory()
	ctx := context.Background()
	txn, err := m.BeginTxn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := txn.Publish(ctx, Record{Topic: "t", Value: []byte("x")}); err != nil {
		t.Fatal(err)
	}
	if err := txn.SendOffsets(ctx, []Offset{{Topic: "src", Offset: 0}}, "g"); err != nil {
		t.Fatal(err)
	}
	if err := txn.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if n := len(m.TopicRecords("t")); n != 1 {
		t.Fatalf("got %d", n)
	}
	if m.GroupOffset("g", "src", 0) != 1 {
		t.Fatal("offset not in txn commit")
	}
}
