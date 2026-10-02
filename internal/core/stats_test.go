package core

import (
	"context"
	"reflect"
	"testing"
)

func TestStats(t *testing.T) {
	e, admin, _ := newEngine(t, Spec{Limits: map[Class]int{"db": 1}})
	a := mustNode(t, e, admin, RootID, Spec{Name: "a", Quotas: map[Resource]int64{"http": 10}})
	b := mustNode(t, e, admin, RootID, Spec{Name: "b"})
	gone := mustNode(t, e, admin, RootID, Spec{Name: "gone"})
	task := mustNode(t, e, admin, a, Spec{})
	if _, err := e.Consume(admin, task, "http", 3); err != nil {
		t.Fatalf("Consume: %v", err)
	}
	if _, err := e.Cancel(admin, gone); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	mustAcquire(t, e, admin, task, "db")
	out := make(chan grant, 2)
	acquireAsync(t, e, context.Background(), admin, b, "db", 2, out)

	got := e.Stats()
	want := Stats{
		Nodes:    5,
		Sessions: 1,
		Waiting:  map[Class]int{"db": 2},
		Root: NodeStats{
			ID:     RootID,
			Used:   map[Resource]int64{"http": 3},
			Limits: map[Class]int{"db": 1},
			Held:   map[Class]int{"db": 1},
		},
		Tenants: []NodeStats{
			{
				ID:     a,
				Name:   "a",
				Quotas: map[Resource]int64{"http": 10},
				Used:   map[Resource]int64{"http": 3},
				Limits: map[Class]int{},
				Held:   map[Class]int{"db": 1},
			},
			{ID: b, Name: "b", Used: map[Resource]int64{}, Limits: map[Class]int{}, Held: map[Class]int{}},
		},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Stats() =\n%+v\nwant\n%+v", got, want)
	}

	// The reading is a copy, so changing it does not reach the engine.
	got.Root.Held["db"] = 99
	if h := held(e, RootID, "db"); h != 1 {
		t.Errorf("held after changing the reading = %d, want 1", h)
	}
	if _, err := e.CloseSession(admin); err != nil {
		t.Fatalf("CloseSession: %v", err)
	}
	for range 2 {
		<-out
	}
}
