package service

import "github.com/liran/sink/internal/storage"

// Comparable fields avoid constructing an encoded routing key for each lookup.
// The key type keeps string, integer, binary, and opaque identities distinct.
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
