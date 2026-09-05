package queue

import "testing"

func FuzzMutationEnvelope(f *testing.F) {
	f.Add([]byte("SNKQ"))
	f.Add([]byte{83, 78, 75, 81, 1, 1})
	f.Add([]byte{83, 78, 75, 81, 1, 2})
	f.Fuzz(func(t *testing.T, payload []byte) {
		if len(payload) > 1<<20 {
			t.Skip()
		}
		mutation, err := UnmarshalMutation(payload)
		if err != nil {
			return
		}
		encoded, err := MarshalMutation(mutation)
		if err != nil {
			t.Fatal(err)
		}
		if len(encoded) != MutationSize(mutation) {
			t.Fatal("preflight size differs from envelope size")
		}
		if _, err := UnmarshalMutation(encoded); err != nil {
			t.Fatal(err)
		}
	})
}
