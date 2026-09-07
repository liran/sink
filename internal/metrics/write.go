package metrics

import (
	"time"

	sink "github.com/liran/sink/gen/sink"
)

const slowWritePhaseThreshold = 5 * time.Second

type RequestQueueObservation struct {
	Method   string
	Outcome  string
	Duration time.Duration
}

type WritePhaseObservation struct {
	// Store must be a configured name or a fixed fallback from the service.
	Store      string
	Completion sink.CompletionMode
	Phase      string
	Duration   time.Duration
}

func (m *Metrics) ObserveRequestQueue(observation RequestQueueObservation) {
	if m == nil {
		return
	}
	switch observation.Method {
	case "Read", "Write", "Delete":
	default:
		return
	}
	switch observation.Outcome {
	case "execute", "canceled", "shutdown":
	default:
		return
	}
	m.requestQueueDuration.WithLabelValues(observation.Method).Observe(observation.Duration.Seconds())
	m.requestQueueExits.WithLabelValues(observation.Method, observation.Outcome).Inc()
}

func (m *Metrics) ObserveWritePhase(observation WritePhaseObservation) {
	if m == nil {
		return
	}
	if observation.Completion != sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_APPLIED && observation.Completion != sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_VISIBLE {
		return
	}
	phase := observation.Phase
	switch phase {
	case "admission", "parse", "storage_read", "lua":
	case "storage_write":
		if observation.Completion == sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_VISIBLE {
			phase = "storage_write_visible"
		} else {
			phase = "storage_write_applied"
		}
	default:
		return
	}
	m.writePhaseDuration.WithLabelValues(phase).Observe(observation.Duration.Seconds())
	// Store attribution is needed for slow phases, without multiplying the
	// entire histogram by the number of stores.
	if observation.Duration > slowWritePhaseThreshold {
		m.writeSlowPhases.WithLabelValues(observation.Store, phase).Inc()
	}
}

func (m *Metrics) ObserveWriteRounds(reads, writes int) {
	if m != nil {
		m.writeExecutionRounds.WithLabelValues("storage_read").Observe(float64(reads))
		m.writeExecutionRounds.WithLabelValues("storage_write").Observe(float64(writes))
	}
}
