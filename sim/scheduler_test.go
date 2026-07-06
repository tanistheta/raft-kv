package sim

import(
	"testing"
	"time"
)

func TestDeterminism(t *testing.T){
	runOnce := func() []string{
		var trace []string
		s:= NewScheduler()
		s.Schedule(50*time.Millisecond, func(){
			trace = append(trace, "A")
		})
		s.Schedule(50*time.Millisecond, func(){
			trace = append(trace, "B")
		})
		s.Run()
		return trace
	}		

first := runOnce()
second := runOnce()
	if len(first) != len(second){
		t.Fatalf("traces differ in length: %v vs %v", first, second)
	}
	for i := range first{
		if first[i] != second[i]{
			t.Fatalf("traces differ at index %d: %v vs %v", i, first, second)
		}
	}
}