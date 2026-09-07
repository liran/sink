package service

import (
	sink "github.com/liran/sink/gen/sink"
)

type mutationRequest[Operation addressedOperation] interface {
	GetOperations() []Operation
	GetCompletionMode() sink.CompletionMode
}

type mutationWave[Request any, Response any] struct {
	applied []*batchCall[Request, Response]
	visible []*batchCall[Request, Response]
}

type mutationPosition struct {
	wave int
	mode sink.CompletionMode
}

// Partition collected RPCs without strengthening their completion requirements.
// Different modes can execute together only when their record addresses are
// disjoint. An RPC touching several records waits for every predecessor; keeping
// the RPC intact also preserves its result and admission boundaries.
func planMutationWaves[Operation addressedOperation, Request mutationRequest[Operation], Response any](
	calls []*batchCall[Request, Response],
) []mutationWave[Request, Response] {
	waves := make([]mutationWave[Request, Response], 0)
	last := make(map[string]mutationPosition)
	for _, call := range calls {
		mode := call.request.GetCompletionMode()
		wave := 0
		keys := make([]string, 0, call.operationCount)
		for _, operation := range call.request.GetOperations() {
			address, err := convertAddress(operation.GetAddress())
			if err != nil {
				// The core returns invalid addresses as per-operation failures.
				continue
			}
			key := address.RoutingKey()
			keys = append(keys, key)
			if previous, exists := last[key]; exists {
				required := previous.wave
				if previous.mode != mode {
					required++
				}
				wave = max(wave, required)
			}
		}
		for len(waves) <= wave {
			var next mutationWave[Request, Response]
			waves = append(waves, next)
		}
		if mode == sink.CompletionMode_COMPLETION_MODE_WAIT_UNTIL_VISIBLE {
			waves[wave].visible = append(waves[wave].visible, call)
		} else {
			waves[wave].applied = append(waves[wave].applied, call)
		}
		position := mutationPosition{wave: wave, mode: mode}
		for _, key := range keys {
			last[key] = position
		}
	}
	return waves
}

func liveMutationCalls[Request any, Response any](calls []*batchCall[Request, Response]) []*batchCall[Request, Response] {
	live := make([]*batchCall[Request, Response], 0, len(calls))
	for _, call := range calls {
		if err := contextError(call.ctx); err != nil {
			completeCall(call, emptyResponse[Response](), err)
			continue
		}
		live = append(live, call)
	}
	return live
}
