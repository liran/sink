package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"strings"
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
	configPath, err := parseConfigPath(os.Args[1:])
	if err == nil {
		err = run(configPath)
	}
	if err != nil {
		slog.Error("sink stopped", "error", err)
		os.Exit(1)
	}
}

func parseConfigPath(args []string) (string, error) {
	flags := flag.NewFlagSet("sink", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	configPath := flags.String("config", "", "path to the YAML configuration file")
	if err := flags.Parse(args); err != nil {
		return "", fmt.Errorf("parse command arguments: %w", err)
	}
	if flags.NArg() != 0 {
		return "", fmt.Errorf("unexpected command argument %q", flags.Arg(0))
	}
	trimmed := strings.TrimSpace(*configPath)
	if trimmed == "" {
		return "", errors.New("--config is required")
	}
	return trimmed, nil
}

func run(configPath string) error {
	loaded, err := loadConfig(configPath)
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
	config       config
	mongoClients map[string]*mongo.Client
	publisher    *queuekafka.Publisher
	worker       *queuekafka.Worker
	grpcServer   *grpc.Server
	health       *health.Server
	listener     net.Listener
}

func newApplication(ctx context.Context, loaded config) (*application, error) {
	opened, err := openConfiguredStorage(ctx, loaded)
	if err != nil {
		return nil, err
	}
	app := &application{config: loaded, mongoClients: opened.mongoClients}
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
	value        storagecontract.Storage
	mongoClients map[string]*mongo.Client
}

func openConfiguredStorage(ctx context.Context, loaded config) (openedStorage, error) {
	var opened openedStorage
	opened.mongoClients = make(map[string]*mongo.Client)
	backends := make(map[string]storagecontract.Storage, len(loaded.storages))
	for _, configured := range loaded.storages {
		backend, err := openStorageBackend(ctx, configured, loaded.shutdownTimeout)
		if err != nil {
			disconnectMongoClients(opened.mongoClients, loaded.shutdownTimeout)
			return opened, fmt.Errorf("open storage %q: %w", configured.name, err)
		}
		backends[configured.name] = backend.value
		if backend.mongoClient != nil {
			opened.mongoClients[configured.name] = backend.mongoClient
		}
	}
	router, err := storagecontract.NewRouter(backends)
	if err != nil {
		disconnectMongoClients(opened.mongoClients, loaded.shutdownTimeout)
		return opened, err
	}
	opened.value = router
	return opened, nil
}

type openedBackend struct {
	value       storagecontract.Storage
	mongoClient *mongo.Client
}

func openStorageBackend(ctx context.Context, configured backendConfig, shutdownTimeout time.Duration) (openedBackend, error) {
	switch configured.driver {
	case driverMongoDB:
		return openMongoStorage(ctx, configured, shutdownTimeout)
	case driverElasticsearch, driverOpenSearch:
		return openSearchStorage(ctx, configured)
	default:
		var empty openedBackend
		return empty, fmt.Errorf("unsupported storage driver %q", configured.driver)
	}
}

func openMongoStorage(ctx context.Context, configured backendConfig, shutdownTimeout time.Duration) (openedBackend, error) {
	var opened openedBackend
	clientOptions := options.Client().ApplyURI(configured.mongoURI)
	mongoClient, err := mongo.Connect(clientOptions)
	if err != nil {
		return opened, fmt.Errorf("connect to MongoDB: %w", err)
	}
	if err := mongoClient.Ping(ctx, nil); err != nil {
		disconnectContext, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()
		_ = mongoClient.Disconnect(disconnectContext)
		return opened, fmt.Errorf("ping MongoDB: %w", err)
	}

	storageOptions := mongodb.Options{
		Store:         configured.name,
		MetadataField: configured.mongoMetadataField,
	}
	store, err := mongodb.New(mongoClient, storageOptions)
	if err != nil {
		disconnectContext, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()
		_ = mongoClient.Disconnect(disconnectContext)
		return opened, err
	}
	opened.value = store
	opened.mongoClient = mongoClient
	return opened, nil
}

func openSearchStorage(ctx context.Context, configured backendConfig) (openedBackend, error) {
	var opened openedBackend
	searchOptions := searchstorage.Options{
		Driver:    configured.searchDriver,
		Endpoints: configured.searchEndpoints,
		Store:     configured.name,
		Username:  configured.searchUsername,
		Password:  configured.searchPassword,
		APIKey:    configured.searchAPIKey,
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
	disconnectMongoClients(a.mongoClients, a.config.shutdownTimeout)
}

func disconnectMongoClients(clients map[string]*mongo.Client, timeout time.Duration) {
	if len(clients) == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	for name, client := range clients {
		if err := client.Disconnect(ctx); err != nil {
			slog.Error("disconnect MongoDB", "storage", name, "error", err)
		}
	}
}
