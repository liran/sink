package mongodb

import (
	"fmt"
	"strings"

	"github.com/liran/sink/internal/storage"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// Prepare the entire command before sending any writes. A later unsafe member
// must not leave an earlier member applied, even for unordered commands.
func (s *Store) prepareNativeWrite(command bson.D) (bson.D, error) {
	switch command[0].Key {
	case "insert", "update", "delete", "findAndModify", "findandmodify":
	default:
		return command, nil
	}
	collection, ok := command[0].Value.(string)
	if !ok || strings.TrimSpace(collection) == "" || strings.HasPrefix(collection, "system.") {
		return nil, invalidNativeWrite("writes require a non-system collection name")
	}
	if err := validateNativeWriteFields(command, 0); err != nil {
		return nil, err
	}
	switch command[0].Key {
	case "insert", "update":
		field := "documents"
		if command[0].Key == "update" {
			field = "updates"
		}
		index := nativeFieldIndex(command, field)
		if index < 0 {
			return nil, invalidNativeWrite("missing %s", field)
		}
		members, ok := command[index].Value.(bson.A)
		if !ok {
			return nil, invalidNativeWrite("%s must be an array", field)
		}
		for i, member := range members {
			document, ok := member.(bson.D)
			if !ok {
				return nil, invalidNativeWrite("%s[%d] must be a document", field, i)
			}
			var prepared bson.D
			var err error
			if field == "documents" {
				prepared, err = s.nativeReplacement(document)
			} else {
				prepared, err = s.nativeUpdateMember(document, "u")
			}
			if err != nil {
				return nil, err
			}
			members[i] = prepared
		}
	case "findAndModify", "findandmodify":
		remove := nativeFieldIndex(command, "remove")
		if remove >= 0 && command[remove].Value == true {
			if nativeFieldIndex(command, "update") >= 0 {
				return nil, invalidNativeWrite("findAndModify cannot combine remove and update")
			}
			return command, nil
		}
		return s.nativeUpdateMember(command, "update")
	}
	// Hard deletes need no version bump: a stale CAS no longer matches, and
	// every supported insertion/replacement creates a fresh revision on reuse.
	return command, nil
}

func invalidNativeWrite(format string, args ...any) error {
	return storage.InvalidArgumentError(fmt.Errorf("revision-protected MongoDB write: "+format, args...))
}

func nativeFieldIndex(document bson.D, name string) int {
	for i, field := range document {
		if field.Key == name {
			return i
		}
	}
	return -1
}

func validateNativeWriteFields(value any, depth int) error {
	if depth > 100 {
		return invalidNativeWrite("document nesting exceeds 100")
	}
	switch value := value.(type) {
	case bson.D:
		seen := make(map[string]bool, len(value))
		for _, field := range value {
			if seen[field.Key] {
				return invalidNativeWrite("duplicate field %q", field.Key)
			}
			seen[field.Key] = true
			if err := validateNativeWriteFields(field.Value, depth+1); err != nil {
				return err
			}
		}
	case bson.A:
		for _, member := range value {
			if err := validateNativeWriteFields(member, depth+1); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *Store) nativeWritePath(path string) error {
	if path == s.metadataField || strings.HasPrefix(path, s.metadataField+".") {
		return invalidNativeWrite("field %q is reserved by Sink", path)
	}
	return nil
}

func nativeRevisionMetadata() (bson.D, error) {
	revision, err := newRevision()
	if err != nil {
		return nil, storage.BackendError(err)
	}
	metadata := bson.D{{Key: "revision", Value: bson.Binary{Subtype: 0, Data: revision.Data}}}
	return metadata, nil
}

func (s *Store) nativeReplacement(document bson.D) (bson.D, error) {
	for _, field := range document {
		if strings.HasPrefix(field.Key, "$") {
			return nil, invalidNativeWrite("replacement field %q cannot be an operator", field.Key)
		}
		if err := s.nativeWritePath(field.Key); err != nil {
			return nil, err
		}
	}
	metadata, err := nativeRevisionMetadata()
	if err != nil {
		return nil, err
	}
	field := bson.E{Key: s.metadataField, Value: metadata}
	return append(document, field), nil
}

func (s *Store) nativeUpdateMember(member bson.D, name string) (bson.D, error) {
	index := nativeFieldIndex(member, name)
	if index < 0 {
		return nil, invalidNativeWrite("missing %s", name)
	}
	var update any
	var err error
	switch value := member[index].Value.(type) {
	case bson.D:
		if len(value) == 0 || !strings.HasPrefix(value[0].Key, "$") {
			update, err = s.nativeReplacement(value)
		} else {
			update, err = s.nativeOperatorUpdate(value)
		}
	case bson.A:
		update, err = s.nativePipelineUpdate(value)
	default:
		return nil, invalidNativeWrite("%s must be an update document or pipeline", name)
	}
	if err != nil {
		return nil, err
	}
	member[index].Value = update
	return member, nil
}

func (s *Store) nativeOperatorUpdate(update bson.D) (bson.D, error) {
	for _, operator := range update {
		switch operator.Key {
		case "$currentDate", "$inc", "$min", "$max", "$mul", "$rename", "$set", "$setOnInsert", "$unset",
			"$addToSet", "$pop", "$pull", "$push", "$pullAll", "$bit":
		default:
			return nil, invalidNativeWrite("unsupported update operator %q", operator.Key)
		}
		fields, ok := operator.Value.(bson.D)
		if !ok {
			return nil, invalidNativeWrite("%s must contain a document", operator.Key)
		}
		for _, field := range fields {
			if err := s.nativeWritePath(field.Key); err != nil {
				return nil, err
			}
			if operator.Key == "$rename" {
				destination, ok := field.Value.(string)
				if !ok {
					return nil, invalidNativeWrite("$rename destination must be a string")
				}
				if err := s.nativeWritePath(destination); err != nil {
					return nil, err
				}
			}
		}
	}
	metadata, err := nativeRevisionMetadata()
	if err != nil {
		return nil, err
	}
	field := bson.E{Key: s.metadataField, Value: metadata}
	index := nativeFieldIndex(update, "$set")
	if index >= 0 {
		update[index].Value = append(update[index].Value.(bson.D), field)
	} else {
		fields := bson.D{field}
		operator := bson.E{Key: "$set", Value: fields}
		update = append(update, operator)
	}
	return update, nil
}

func (s *Store) nativePipelineUpdate(pipeline bson.A) (bson.A, error) {
	for _, value := range pipeline {
		stage, ok := value.(bson.D)
		if !ok || len(stage) != 1 {
			return nil, invalidNativeWrite("each update pipeline stage must contain exactly one operator")
		}
		switch stage[0].Key {
		case "$set", "$addFields", "$project":
			fields, ok := stage[0].Value.(bson.D)
			if !ok {
				return nil, invalidNativeWrite("%s must contain a document", stage[0].Key)
			}
			for _, field := range fields {
				if err := s.nativeWritePath(field.Key); err != nil {
					return nil, err
				}
			}
		case "$unset":
			paths, ok := stage[0].Value.(bson.A)
			if !ok {
				paths = bson.A{stage[0].Value}
			}
			for _, value := range paths {
				path, ok := value.(string)
				if !ok {
					return nil, invalidNativeWrite("$unset paths must be strings")
				}
				if err := s.nativeWritePath(path); err != nil {
					return nil, err
				}
			}
		case "$replaceRoot", "$replaceWith":
			// These may replace the entire document, including through dynamic
			// expressions. The final stage always replaces internal metadata.
		default:
			return nil, invalidNativeWrite("unsupported update pipeline stage %q", stage[0].Key)
		}
	}
	metadata, err := nativeRevisionMetadata()
	if err != nil {
		return nil, err
	}
	literal := bson.D{{Key: "$literal", Value: metadata}}
	fields := bson.D{{Key: s.metadataField, Value: literal}}
	stage := bson.D{{Key: "$set", Value: fields}}
	return append(pipeline, stage), nil
}
