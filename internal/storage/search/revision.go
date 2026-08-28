package search

import (
	"encoding/binary"
	"errors"

	"github.com/liran/sink/internal/storage"
)

const revisionSize = 16

type revisionValues struct {
	sequenceNumber int64
	primaryTerm    int64
}

func encodeRevision(sequenceNumber int64, primaryTerm int64) (storage.Revision, error) {
	var empty storage.Revision
	if sequenceNumber < 0 || primaryTerm <= 0 {
		return empty, errors.New("search response contains an invalid sequence number or primary term")
	}
	data := make([]byte, revisionSize)
	binary.BigEndian.PutUint64(data[:8], uint64(sequenceNumber))
	binary.BigEndian.PutUint64(data[8:], uint64(primaryTerm))
	revision := storage.Revision{Data: data}
	return revision, nil
}

func decodeRevision(revision storage.Revision) (revisionValues, error) {
	var values revisionValues
	if len(revision.Data) != revisionSize {
		return values, errors.New("search revision must contain 16 bytes")
	}
	values.sequenceNumber = int64(binary.BigEndian.Uint64(revision.Data[:8]))
	values.primaryTerm = int64(binary.BigEndian.Uint64(revision.Data[8:]))
	if values.sequenceNumber < 0 || values.primaryTerm <= 0 {
		return values, errors.New("search revision contains an invalid sequence number or primary term")
	}
	return values, nil
}
