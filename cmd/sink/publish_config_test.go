package main

import (
	"strings"
	"testing"
)

func TestPublishAdmissionConfiguration(t *testing.T) {
	for _, value := range []int{0, -1, 10001} {
		loaded := config{grpcMaxSendBytes: 64 << 20}
		file := serviceConfigFile{MaxPublishRequests: &value}
		err := loaded.loadReliabilityConfig(file)
		if err == nil || !strings.Contains(err.Error(), "service.max_publish_requests") {
			t.Fatalf("invalid publish request limit %d: %v", value, err)
		}
	}
	for _, value := range []int{0, -1, 17 << 30} {
		loaded := config{grpcMaxSendBytes: 64 << 20}
		file := serviceConfigFile{MaxPublishBytes: &value}
		err := loaded.loadReliabilityConfig(file)
		if err == nil || !strings.Contains(err.Error(), "service.max_publish_bytes") {
			t.Fatalf("invalid publish byte limit %d: %v", value, err)
		}
	}
	loaded := config{grpcMaxSendBytes: 64 << 20}
	file := serviceConfigFile{}
	if err := loaded.loadReliabilityConfig(file); err != nil {
		t.Fatal(err)
	}
	if loaded.maxPublishRequests != 32 || loaded.maxPublishBytes != 256<<20 {
		t.Fatal("unexpected default publish capacity")
	}
	requests, bytes := 4, 8<<20
	file.MaxPublishRequests = &requests
	file.MaxPublishBytes = &bytes
	if err := loaded.loadReliabilityConfig(file); err != nil {
		t.Fatal(err)
	}
	if loaded.maxPublishRequests != requests || loaded.maxPublishBytes != bytes {
		t.Fatal("explicit publish capacity was not applied")
	}
}
