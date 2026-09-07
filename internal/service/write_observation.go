package service

import (
	"time"

	sink "github.com/liran/sink/gen/sink"
	sinkmetrics "github.com/liran/sink/internal/metrics"
)

type writeObservation struct {
	metrics    *sinkmetrics.Metrics
	store      string
	completion sink.CompletionMode
	reads      int
	writes     int
}

func (s *Server) newWriteObservation(req *sink.WriteRequest) *writeObservation {
	if s.metrics == nil || req.GetCompletionMode() == sink.CompletionMode_COMPLETION_MODE_RETURN_AFTER_ACCEPTED {
		return nil
	}
	store, single := requestStore(req.GetOperations())
	if !single {
		store = "_multiple"
	} else {
		// Never use an arbitrary client-supplied store as a metric label.
		s.admissionMu.Lock()
		_, configured := s.storeRequests[store]
		s.admissionMu.Unlock()
		if !configured {
			store = "_unconfigured"
		}
	}
	observation := &writeObservation{metrics: s.metrics, store: store, completion: req.GetCompletionMode()}
	return observation
}

func (o *writeObservation) phase(phase string, started time.Time) {
	if o == nil {
		return
	}
	switch phase {
	case "storage_read":
		o.reads++
	case "storage_write":
		o.writes++
	}
	observation := sinkmetrics.WritePhaseObservation{Store: o.store, Completion: o.completion, Phase: phase, Duration: time.Since(started)}
	o.metrics.ObserveWritePhase(observation)
}

func (o *writeObservation) finish() {
	if o == nil {
		return
	}
	o.metrics.ObserveWriteRounds(o.reads, o.writes)
}
