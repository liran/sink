package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	sink "github.com/liran/sink/gen/sink"
	"github.com/liran/sink/internal/merge"
	queuekafka "github.com/liran/sink/internal/queue/kafka"
	"github.com/liran/sink/internal/service"
	storagecontract "github.com/liran/sink/internal/storage"
	"github.com/liran/sink/internal/storage/mongodb"
	searchstorage "github.com/liran/sink/internal/storage/search"
	"github.com/liran/sink/internal/worker"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

var version = "dev"

func main() {
	if len(os.Args) == 2 && os.Args[1] == "version" {
		fmt.Println(version)
		return
	}
	if err := run(); err != nil {
		slog.Error("sink stopped", "error", err)
		os.Exit(1)
	}
}

func run() error {
	loaded, err := loadConfig()
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	app, err := newApplication(ctx, loaded)
	if err != nil {
		return err
	}
	defer app.close()
	slog.Info("starting sink", "version", version, "mode", loaded.mode)
	return app.run(ctx)
}

type application struct {
	config      config
	mongoClient *mongo.Client
	publisher   *queuekafka.Publisher
	worker      *queuekafka.Worker
	grpcServer  *grpc.Server
	health      *health.Server
	listener    net.Listener
}

func newApplication(ctx context.Context, loaded config) (*application, error) {
	opened, err := openConfiguredStorage(ctx, loaded)
	if err != nil {
		return nil, err
	}
	app := &application{config: loaded, mongoClient: opened.mongoClient}
	if len(loaded.kafkaBrokers) > 0 && (loaded.mode == modeServer || loaded.mode == modeAll) {
		publisherOptions := queuekafka.PublisherOptions{
			Brokers: loaded.kafkaBrokers,
			Topic:   loaded.kafkaTopic,
		}
		app.publisher, err = queuekafka.NewPublisher(publisherOptions)
		if err != nil {
			app.close()
			return nil, err
		}
		if err := app.publisher.Ping(ctx); err != nil {
			app.close()
			return nil, fmt.Errorf("ping Kafka: %w", err)
		}
	}

	registry := merge.NewRegistry()
	serverOptions := service.Options{
		Storage:          opened.value,
		Merges:           registry,
		Publisher:        app.publisher,
		MaxOperations:    loaded.maxOperations,
		MaxMergeAttempts: loaded.maxMergeAttempts,
	}
	sinkServer, err := service.New(serverOptions)
	if err != nil {
		app.close()
		return nil, err
	}
	if loaded.mode == modeServer || loaded.mode == modeAll {
		if err := app.configureGRPC(sinkServer); err != nil {
			app.close()
			return nil, err
		}
	}
	if loaded.mode == modeWorker || loaded.mode == modeAll {
		processor, processorErr := worker.NewProcessor(sinkServer)
		if processorErr != nil {
			app.close()
			return nil, processorErr
		}
		workerOptions := queuekafka.WorkerOptions{
			Brokers:        loaded.kafkaBrokers,
			Topic:          loaded.kafkaTopic,
			GroupID:        loaded.kafkaGroupID,
			Handler:        processor,
			MaxPollRecords: loaded.kafkaMaxPollRecords,
		}
		app.worker, err = queuekafka.NewWorker(workerOptions)
		if err != nil {
			app.close()
			return nil, err
		}
	}
	return app, nil
}

type openedStorage struct {
	value       storagecontract.Storage
	mongoClient *mongo.Client
}

func openConfiguredStorage(ctx context.Context, loaded config) (openedStorage, error) {
	switch loaded.storageDriver {
	case driverMongoDB:
		return openMongoStorage(ctx, loaded)
	case driverElasticsearch, driverOpenSearch:
		return openSearchStorage(ctx, loaded)
	default:
		var empty openedStorage
		return empty, fmt.Errorf("unsupported storage driver %q", loaded.storageDriver)
	}
}

func openMongoStorage(ctx context.Context, loaded config) (openedStorage, error) {
	var opened openedStorage
	clientOptions := options.Client().ApplyURI(loaded.mongoURI)
	mongoClient, err := mongo.Connect(clientOptions)
	if err != nil {
		return opened, fmt.Errorf("connect to MongoDB: %w", err)
	}
	if err := mongoClient.Ping(ctx, nil); err != nil {
		disconnectContext, cancel := context.WithTimeout(context.Background(), loaded.shutdownTimeout)
		defer cancel()
		_ = mongoClient.Disconnect(disconnectContext)
		return opened, fmt.Errorf("ping MongoDB: %w", err)
	}

	storageOptions := mongodb.Options{
		Store:       loaded.mongoStore,
		HiddenField: loaded.mongoHiddenField,
		Bindings:    loaded.mongoBindings,
	}
	store, err := mongodb.New(mongoClient, storageOptions)
	if err != nil {
		disconnectContext, cancel := context.WithTimeout(context.Background(), loaded.shutdownTimeout)
		defer cancel()
		_ = mongoClient.Disconnect(disconnectContext)
		return opened, err
	}
	opened.value = store
	opened.mongoClient = mongoClient
	return opened, nil
}

func openSearchStorage(ctx context.Context, loaded config) (openedStorage, error) {
	var opened openedStorage
	searchOptions := searchstorage.Options{
		Driver:    loaded.searchDriver,
		Endpoints: loaded.searchEndpoints,
		Store:     loaded.searchStore,
		Bindings:  loaded.searchBindings,
		Username:  loaded.searchUsername,
		Password:  loaded.searchPassword,
		APIKey:    loaded.searchAPIKey,
	}
	store, err := searchstorage.New(searchOptions)
	if err != nil {
		return opened, err
	}
	if err := store.Ping(ctx); err != nil {
		return opened, err
	}
	opened.value = store
	return opened, nil
}

func (a *application) configureGRPC(server *service.Server) error {
	listener, err := net.Listen("tcp", a.config.grpcAddress)
	if err != nil {
		return fmt.Errorf("listen for gRPC: %w", err)
	}
	grpcServer := grpc.NewServer()
	sink.RegisterSinkServer(grpcServer, server)
	healthServer := health.NewServer()
	healthServer.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	healthpb.RegisterHealthServer(grpcServer, healthServer)
	a.listener = listener
	a.grpcServer = grpcServer
	a.health = healthServer
	return nil
}

func (a *application) run(ctx context.Context) error {
	runContext, cancel := context.WithCancel(ctx)
	defer cancel()
	runErrors := make(chan error, 2)
	if a.grpcServer != nil {
		go func() {
			err := a.grpcServer.Serve(a.listener)
			if err != nil && !errors.Is(err, grpc.ErrServerStopped) {
				runErrors <- fmt.Errorf("serve gRPC: %w", err)
			}
		}()
	}
	if a.worker != nil {
		go func() {
			if err := a.worker.Run(runContext); err != nil {
				runErrors <- fmt.Errorf("run Kafka worker: %w", err)
			}
		}()
	}
	select {
	case <-runContext.Done():
		return nil
	case err := <-runErrors:
		return err
	}
}

func (a *application) close() {
	if a.health != nil {
		a.health.SetServingStatus("", healthpb.HealthCheckResponse_NOT_SERVING)
	}
	if a.grpcServer != nil {
		stopped := make(chan struct{})
		go func() {
			a.grpcServer.GracefulStop()
			close(stopped)
		}()
		timer := time.NewTimer(a.config.shutdownTimeout)
		select {
		case <-stopped:
			if !timer.Stop() {
				<-timer.C
			}
		case <-timer.C:
			a.grpcServer.Stop()
		}
	}
	if a.worker != nil {
		a.worker.Close()
	}
	if a.publisher != nil {
		a.publisher.Close()
	}
	if a.mongoClient != nil {
		ctx, cancel := context.WithTimeout(context.Background(), a.config.shutdownTimeout)
		defer cancel()
		if err := a.mongoClient.Disconnect(ctx); err != nil {
			slog.Error("disconnect MongoDB", "error", err)
		}
	}
}
