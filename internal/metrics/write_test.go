package metrics_test

import (
	"strings"
	"testing"
	"time"

	sink "github.com/liran/sink/gen/sink"
	sinkmetrics "github.com/liran/sink/internal/metrics"
)

func TestRequestQueueMetricsCoverLongTailAndExitOutcomes(t *testing.T) {
	observed, err := sinkmetrics.New("test")
	if err != nil {
		t.Fatal(err)
	}
	for _, outcome := range []string{"execute", "canceled", "shutdown"} {
		observation := sinkmetrics.RequestQueueObservation{Method: "Write", Outcome: outcome, Duration: 12 * time.Second}
		observed.ObserveRequestQueue(observation)
	}
	body := scrape(t, observed)
	wanted := []string{
		`sink_batcher_request_queue_duration_seconds_count{method="Write"} 3`,
		`sink_batcher_request_queue_duration_seconds_sum{method="Write"} 36`,
		`sink_batcher_request_queue_duration_seconds_bucket{method="Write",le="10"} 0`,
		`sink_batcher_request_queue_duration_seconds_bucket{method="Write",le="30"} 3`,
		`sink_batcher_request_queue_exits_total{method="Write",outcome="execute"} 1`,
		`sink_batcher_request_queue_exits_total{method="Write",outcome="canceled"} 1`,
		`sink_batcher_request_queue_exits_total{method="Write",outcome="shutdown"} 1`,
	}
	for _, line := range wanted {
		if !strings.Contains(body, line) {
			t.Errorf("missing metric: %s", line)
		}
	}
}

func TestWriteSlowPhaseCounterKeepsStoreAndVisibilityAttribution(t *testing.T) {
	observed, err := sinkmetrics.New("test")
	if err != nil {
		t.Fatal(err)
	}
	for _, mode := range []sink.CompletionMode{sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED, sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_VISIBLE} {
		for _, duration := range []time.Duration{5 * time.Second, 5*time.Second + time.Nanosecond} {
			observation := sinkmetrics.WritePhaseObservation{Store: "search", Completion: mode, Phase: "storage_write", Duration: duration}
			observed.ObserveWritePhase(observation)
		}
	}
	body := scrape(t, observed)
	for _, phase := range []string{"storage_write_applied", "storage_write_visible"} {
		wanted := []string{
			`sink_write_slow_phases_total{phase="` + phase + `",store="search"} 1`,
			`sink_write_phase_duration_seconds_count{phase="` + phase + `"} 2`,
			`sink_write_phase_duration_seconds_bucket{phase="` + phase + `",le="5"} 1`,
		}
		for _, line := range wanted {
			if !strings.Contains(body, line) {
				t.Errorf("missing metric: %s", line)
			}
		}
	}
}
