package service

import (
	"context"
	"testing"
	"testing/synctest"

	"github.com/liran/sink/internal/storage/memory"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestAdmissionPreservesCapacityForWaitingLargeBatch(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		server := completionServer(t, memory.New()).server
		server.maxInFlightBytes = 100
		initial := admissionRequest{encodedBytes: 20}
		_, releaseInitial, err := server.admitRequest(t.Context(), initial)
		if err != nil {
			t.Fatal(err)
		}
		large := admissionRequest{encodedBytes: 90, wait: true}
		admitted := make(chan context.CancelFunc, 1)
		go func() {
			_, release, admitErr := server.admitRequest(t.Context(), large)
			if admitErr != nil {
				t.Error(admitErr)
			}
			admitted <- release
		}()
		synctest.Wait()
		// An unlimited stream of these smaller requests must not keep the
		// older batch waiting until its caller deadline.
		small := admissionRequest{encodedBytes: 1}
		_, releaseSmall, err := server.admitRequest(t.Context(), small)
		if releaseSmall != nil {
			releaseSmall()
		}
		if status.Code(err) != codes.ResourceExhausted {
			t.Errorf("new request bypassed an older byte reservation: %v", err)
		}
		releaseInitial()
		synctest.Wait()
		releaseLarge := <-admitted
		if releaseLarge != nil {
			releaseLarge()
		}
	})
}

func TestAdmissionCancellationWakesNextReservation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		server := completionServer(t, memory.New()).server
		server.maxInFlightBytes = 100
		initial := admissionRequest{encodedBytes: 20}
		_, releaseInitial, err := server.admitRequest(t.Context(), initial)
		if err != nil {
			t.Fatal(err)
		}
		defer releaseInitial()
		ctx, cancel := context.WithCancel(t.Context())
		large := admissionRequest{encodedBytes: 90, wait: true}
		canceled := make(chan error, 1)
		go func() {
			_, _, admitErr := server.admitRequest(ctx, large)
			canceled <- admitErr
		}()
		synctest.Wait()
		small := admissionRequest{encodedBytes: 1, wait: true}
		admitted := make(chan context.CancelFunc, 1)
		go func() {
			_, release, admitErr := server.admitRequest(t.Context(), small)
			if admitErr != nil {
				t.Error(admitErr)
			}
			admitted <- release
		}()
		synctest.Wait()
		if len(admitted) != 0 {
			t.Error("later waiter bypassed the large batch")
		}
		cancel()
		synctest.Wait()
		if status.Code(<-canceled) != codes.Canceled {
			t.Error("canceled reservation did not return cancellation")
		}
		releaseSmall := <-admitted
		if releaseSmall != nil {
			releaseSmall()
		}
		if len(server.admissionWaiters) != 0 || server.inFlightBytes != 20 {
			t.Fatal("completed or canceled reservation leaked admission state")
		}
	})
}

func TestAdmissionBusyStoreDoesNotBlockOtherStores(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		server := completionServer(t, memory.New()).server
		server.maxStoreRequests = 1
		server.storeRequests = map[string]int{"a": 0, "b": 0}
		first := admissionRequest{encodedBytes: 1, stores: []string{"a"}}
		_, releaseFirst, err := server.admitRequest(t.Context(), first)
		if err != nil {
			t.Fatal(err)
		}
		defer releaseFirst()
		ctx, cancel := context.WithCancel(t.Context())
		first.wait = true
		canceled := make(chan error, 1)
		go func() {
			_, _, admitErr := server.admitRequest(ctx, first)
			canceled <- admitErr
		}()
		synctest.Wait()
		second := admissionRequest{encodedBytes: 1, stores: []string{"b"}}
		_, releaseSecond, err := server.admitRequest(t.Context(), second)
		if err != nil {
			t.Errorf("unrelated store blocked by waiting store: %v", err)
		}
		if releaseSecond != nil {
			releaseSecond()
		}
		cancel()
		synctest.Wait()
		if status.Code(<-canceled) != codes.Canceled {
			t.Error("waiting store did not release on cancellation")
		}
	})
}
