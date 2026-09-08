//go:build integration

package mongodb_test

import (
	"testing"

	"github.com/liran/sink/internal/storage"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestNativeMongoPagesSortProjectionAndCount(t *testing.T) {
	fixture := newIntegrationFixture(t)
	documents := bson.A{}
	for number := range 7 {
		document := bson.D{{Key: "number", Value: number}, {Key: "name", Value: "item"}}
		documents = append(documents, document)
	}
	insert := bson.D{{Key: "insert", Value: "documents"}, {Key: "documents", Value: documents}}
	seed := mongoNativeRequest(t, fixture.database, insert)
	if result, err := fixture.store.Execute(t.Context(), seed); err != nil || !result.Success {
		t.Fatalf("seed=%+v err=%v", result, err)
	}
	filter := bson.D{{Key: "number", Value: bson.D{{Key: "$gte", Value: 2}}}}
	find := bson.D{{Key: "find", Value: "documents"}, {Key: "filter", Value: filter}, {Key: "skip", Value: 999},
		{Key: "limit", Value: 1}, {Key: "sort", Value: bson.D{{Key: "number", Value: 1}}}, {Key: "projection", Value: bson.D{{Key: "name", Value: 1}}}}
	command := mongoNativeRequest(t, fixture.database, find)
	projection := &storage.Projection{Fields: []string{"number"}}
	query := storage.QueryRequest{Request: command, Offset: 2, PageSize: 2, Sort: []storage.SortField{{Field: "number", Descending: true}}, Projection: projection}
	for _, offset := range []int64{2, 4, 6} {
		query.Offset = offset
		page, err := fixture.store.Query(t.Context(), query)
		expected := max(0, min(2, 5-int(offset)))
		if err != nil || len(page.Documents) != expected || page.HasMore != (offset == 2) {
			t.Fatalf("offset=%d page=%+v err=%v", offset, page, err)
		}
		for index, document := range page.Documents {
			raw := bson.Raw(document.Payload)
			if raw.Lookup("number").AsInt64() != 6-offset-int64(index) || raw.Lookup("name").Type != 0 {
				t.Fatalf("sort or projection lost: %s", raw)
			}
		}
	}
	if count, err := fixture.store.Count(t.Context(), command); err != nil || count != 5 {
		t.Fatalf("find count=%d err=%v", count, err)
	}
	match := bson.D{{Key: "$match", Value: filter}}
	limit := bson.D{{Key: "$limit", Value: 4}}
	aggregate := bson.D{{Key: "aggregate", Value: "documents"}, {Key: "pipeline", Value: bson.A{match, limit}}}
	command = mongoNativeRequest(t, fixture.database, aggregate)
	query.Request, query.Offset = command, 0
	page, err := fixture.store.Query(t.Context(), query)
	if err != nil || len(page.Documents) != 2 || !page.HasMore || bson.Raw(page.Documents[0].Payload).Lookup("number").AsInt64() != 5 {
		t.Fatalf("aggregate page=%+v err=%v", page, err)
	}
	if count, err := fixture.store.Count(t.Context(), command); err != nil || count != 4 {
		t.Fatalf("pipeline count=%d err=%v", count, err)
	}
	missing := bson.D{{Key: "find", Value: "absent"}}
	command = mongoNativeRequest(t, fixture.database, missing)
	if count, err := fixture.store.Count(t.Context(), command); err != nil || count != 0 {
		t.Fatalf("empty count=%d err=%v", count, err)
	}
}
