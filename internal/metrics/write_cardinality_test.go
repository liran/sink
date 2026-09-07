package metrics_test

import (
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	sink "github.com/liran/sink/gen/sink"
	sinkmetrics "github.com/liran/sink/internal/metrics"
)

func TestWriteDiagnosticsSeriesBudget(t *testing.T) {
	// Count actual exported series, including bucket/+Inf/sum/count expansion.
	// A label or bucket expansion must be reviewed against this fixed budget.
	for _, stores := range []int{1, 3, 16} {
		t.Run(fmt.Sprintf("stores_%d", stores), func(t *testing.T) {
			observed, err := sinkmetrics.New("test")
			if err != nil {
				t.Fatal(err)
			}
			for _, method := range []string{"Read", "Write", "Delete"} {
				for _, outcome := range []string{"execute", "canceled", "shutdown"} {
					observation := sinkmetrics.RequestQueueObservation{Method: method, Outcome: outcome, Duration: 12 * time.Second}
					observed.ObserveRequestQueue(observation)
				}
			}
			names := []string{"_multiple", "_unconfigured"}
			for index := range stores {
				names = append(names, fmt.Sprintf("configured-%d", index))
			}
			for _, store := range names {
				for _, mode := range []sink.CompletionMode{sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED, sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_VISIBLE} {
					for _, phase := range []string{"admission", "parse", "storage_read", "lua", "storage_write"} {
						observation := sinkmetrics.WritePhaseObservation{Store: store, Completion: mode, Phase: phase, Duration: 12 * time.Second}
						observed.ObserveWritePhase(observation)
					}
					observed.ObserveWriteRounds(32, 32)
				}
			}
			// Invalid categories must not create new dimensions.
			for index := range 50 {
				unknown := fmt.Sprintf("unbounded-%d", index)
				queue := sinkmetrics.RequestQueueObservation{Method: unknown, Outcome: "execute"}
				observed.ObserveRequestQueue(queue)
				queue.Method, queue.Outcome = "Write", unknown
				observed.ObserveRequestQueue(queue)
				phase := sinkmetrics.WritePhaseObservation{Store: "configured-0", Completion: sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED, Phase: unknown, Duration: 12 * time.Second}
				observed.ObserveWritePhase(phase)
				phase.Phase, phase.Completion = "storage_write", sink.CompletionMode(index+100)
				observed.ObserveWritePhase(phase)
			}
			for _, accept := range []string{"text/plain", "application/openmetrics-text; version=1.0.0"} {
				request := httptest.NewRequest("GET", "/metrics", nil)
				request.Header.Set("Accept", accept)
				recorder := httptest.NewRecorder()
				observed.Handler().ServeHTTP(recorder, request)
				body := recorder.Body.String()
				counts := make(map[string]int)
				for _, line := range strings.Split(body, "\n") {
					if !strings.HasPrefix(line, "sink_write_") && !strings.HasPrefix(line, "sink_batcher_request_queue_") {
						continue
					}
					name, _, _ := strings.Cut(line, "{")
					for _, suffix := range []string{"_bucket", "_sum", "_count"} {
						name = strings.TrimSuffix(name, suffix)
					}
					counts[name]++
				}
				wanted := map[string]int{
					"sink_batcher_request_queue_duration_seconds": 30,
					"sink_batcher_request_queue_exits_total":      9,
					"sink_write_phase_duration_seconds":           60,
					"sink_write_execution_rounds":                 18,
					"sink_write_slow_phases_total":                6 * (stores + 2),
				}
				total := 0
				for name, count := range counts {
					total += count
					if count != wanted[name] {
						t.Errorf("%s: %s has %d series, budget %d", accept, name, count, wanted[name])
					}
				}
				if len(counts) != len(wanted) || total != 129+6*stores || strings.Contains(body, "unbounded-") {
					t.Fatalf("%s: unexpected series cardinality: %v, total %d", accept, counts, total)
				}
				t.Logf("%s: %d configured stores, %d added series per Pod", accept, stores, total)
			}
		})
	}
}
