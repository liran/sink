package storage

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"io"
)

const MaxScanCursorBytes = 64 << 10

// ScanBackendBytes bounds a search page plus one full-sized lookahead hit and
// response metadata. The service reserves both wire and decoded buffers.
func ScanBackendBytes(maximum int) int {
	if maximum <= 0 {
		maximum = DefaultMaxReadBytes
	}
	return 2*maximum + MaxScanCursorBytes
}

type scanToken struct {
	Version  int      `json:"v"`
	Query    [32]byte `json:"q"`
	Position []byte   `json:"p"`
}

// ScanCursor carries a caller-owned seek position, not a database session or
// an authorization capability. The checksum detects corrupt tokens; backend
// authentication and query validation still apply to every request.
type ScanCursor struct {
	Position []byte
	query    [32]byte
}

func (r ScanRequest) Resume() (ScanCursor, error) {
	var cursor ScanCursor
	if r.BatchSize < 1 || r.BatchSize > 1000 {
		return cursor, InvalidArgumentError(errors.New("scan batch size must be between 1 and 1000"))
	}
	command := r.Request
	command.MaxBytes = 0
	encoded, err := json.Marshal(command)
	if err != nil {
		return cursor, InvalidArgumentError(err)
	}
	cursor.query = sha256.Sum256(encoded)
	if len(r.Cursor) != 0 {
		invalid := InvalidArgumentError(errors.New("invalid scan cursor or command changed"))
		if len(r.Cursor) <= sha256.Size || len(r.Cursor) > MaxScanCursorBytes {
			return cursor, invalid
		}
		payload := r.Cursor[sha256.Size:]
		checksum := sha256.Sum256(payload)
		if !bytes.Equal(r.Cursor[:sha256.Size], checksum[:]) {
			return cursor, invalid
		}
		var token scanToken
		decoder := json.NewDecoder(bytes.NewReader(payload))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&token); err != nil {
			return cursor, invalid
		}
		var trailing any
		if err := decoder.Decode(&trailing); err != io.EOF || token.Version != 1 || token.Query != cursor.query || len(token.Position) == 0 {
			return cursor, invalid
		}
		cursor.Position = token.Position
	}

	return cursor, nil
}

func (c ScanCursor) Page(documents []Document, position []byte) (ScanResponse, error) {
	response := ScanResponse{Documents: documents}
	if len(position) == 0 {
		return response, nil
	}
	token := scanToken{Version: 1, Query: c.query, Position: position}
	payload, err := json.Marshal(token)
	if err != nil {
		var empty ScanResponse
		return empty, err
	}
	checksum := sha256.Sum256(payload)
	response.NextCursor = append(checksum[:], payload...)
	if len(response.NextCursor) > MaxScanCursorBytes {
		var empty ScanResponse
		return empty, ResourceExhaustedError(errors.New("scan position exceeds cursor byte limit"))
	}
	return response, nil
}
