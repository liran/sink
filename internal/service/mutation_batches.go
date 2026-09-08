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

// Requests spanning datasets retain their RPC boundary but execute alone.
// Partitioning never permits a later same-record request to pass a predecessor.
type batchPartition struct {
	namespace string
	dataset   string
	mode      sink.CompletionMode
	isolated  bool
}

func mutationRequestPartition[Operation addressedOperation, Request mutationRequest[Operation]](request Request) batchPartition {
	partition := batchPartition{mode: request.GetCompletionMode()}
	if write, ok := any(request).(*sink.WriteRequest); ok {
		for _, operation := range write.GetOperations() {
			if operation.GetOperationId() != "" {
				// Capability/expiry failures must not poison unrelated original
				// RPCs. Protected writes also retain separate receipt budgets.
				partition.isolated = true
				return partition
			}
		}
	}
	for index, operation := range request.GetOperations() {
		address := operation.GetAddress()
		if address.GetNamespace() == "" || address.GetDataset() == "" {
			partition.isolated = true
			break
		}
		if index == 0 {
			partition.namespace = address.GetNamespace()
			partition.dataset = address.GetDataset()
		} else if partition.namespace != address.GetNamespace() || partition.dataset != address.GetDataset() {
			partition.isolated = true
			break
		}
	}
	return partition
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

func mutationRequestRecords[Operation addressedOperation, Request mutationRequest[Operation]](request Request) []recordIdentity {
	records := make([]recordIdentity, 0, len(request.GetOperations()))
	for _, operation := range request.GetOperations() {
		address, err := convertAddress(operation.GetAddress())
		if err == nil {
			records = append(records, identityOf(address))
		}
	}
	return records
}
