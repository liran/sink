package kafka

import (
	"slices"
	"testing"
)

func TestDesiredReplicasPreservesAvailableReplicasAndConvergesCount(t *testing.T) {
	tests := []struct {
		name      string
		partition int32
		current   []int32
		brokers   []int32
		count     int
		want      []int32
	}{
		{
			name:      "increase replication factor",
			partition: 0,
			current:   []int32{1},
			brokers:   []int32{0, 1, 2},
			count:     2,
			want:      []int32{1, 0},
		},
		{
			name:      "decrease replication factor",
			partition: 1,
			current:   []int32{2, 1, 0},
			brokers:   []int32{0, 1, 2},
			count:     2,
			want:      []int32{2, 1},
		},
		{
			name:      "replace unavailable broker",
			partition: 2,
			current:   []int32{9, 1},
			brokers:   []int32{0, 1, 2},
			count:     2,
			want:      []int32{1, 2},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := desiredReplicas(test.partition, test.current, test.brokers, test.count)
			if !slices.Equal(got, test.want) {
				t.Fatalf("desiredReplicas() = %v, want %v", got, test.want)
			}
		})
	}
}
