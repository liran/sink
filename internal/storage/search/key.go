package search

import (
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/liran/sink/internal/storage"
)

const (
	escapedKeyPrefix  = "~sink~"
	maximumDocumentID = 512
)

func documentID(key storage.Key) (string, error) {
	var id string
	switch key.Type {
	case "string":
		id = string(key.Data)
		if strings.HasPrefix(id, escapedKeyPrefix) {
			id = escapedKeyPrefix + "string~" + encodeKeyPart(key.Data)
		}
	case "int64":
		if len(key.Data) != 8 {
			return "", errors.New("int64 record key must contain 8 bytes")
		}
		value := int64(binary.BigEndian.Uint64(key.Data))
		id = escapedKeyPrefix + "int64~" + strconv.FormatInt(value, 10)
	case "bytes":
		id = escapedKeyPrefix + "bytes~" + encodeKeyPart(key.Data)
	default:
		if !strings.HasPrefix(key.Type, "opaque:") {
			return "", fmt.Errorf("unsupported search record key type %q", key.Type)
		}
		opaqueType := strings.TrimPrefix(key.Type, "opaque:")
		if opaqueType == "" {
			return "", errors.New("opaque record key type is required")
		}
		id = escapedKeyPrefix + "opaque~" + encodeKeyPart([]byte(opaqueType)) + "~" + encodeKeyPart(key.Data)
	}
	if len([]byte(id)) > maximumDocumentID {
		return "", fmt.Errorf("search document ID exceeds %d bytes", maximumDocumentID)
	}
	return id, nil
}

func encodeKeyPart(value []byte) string {
	return base64.RawURLEncoding.EncodeToString(value)
}
