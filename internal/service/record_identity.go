package service

import (
	sink "github.com/liran/sink/gen/sink"
	"github.com/liran/sink/internal/queue"
	"github.com/liran/sink/internal/storage"
)

// The comparable representation keeps string, integer, binary, and opaque
// identities distinct. Server paths place a backend-aware routing key in
// keyData; the free helper retains field-by-field identity for unit tests.
type recordIdentity struct {
	store, namespace, dataset string
	keyType, keyData          string
}

func identityOf(address storage.Address) recordIdentity {
	identity := recordIdentity{
		store: address.Store, namespace: address.Namespace, dataset: address.Dataset,
		keyType: address.Key.Type, keyData: string(address.Key.Data),
	}
	return identity
}

func (s *Server) identityOf(address storage.Address) recordIdentity {
	return recordIdentity{keyData: storage.IdentityKey(s.storage, address)}
}

// MutationKey lets the asynchronous worker preserve backend physical ordering
// when a logical address contains metadata not used by that backend.
func (s *Server) MutationKey(mutation queue.Mutation) ([]byte, error) {
	if _, err := queue.MutationKey(mutation); err != nil {
		return nil, err
	}
	var address *sink.RecordAddress
	if mutation.Write != nil {
		address = mutation.Write.GetAddress()
	} else {
		address = mutation.Delete.GetAddress()
	}
	converted, err := convertAddress(address)
	if err != nil {
		return nil, err
	}
	return []byte(storage.IdentityKey(s.storage, converted)), nil
}
