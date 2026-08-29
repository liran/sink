package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	sink "github.com/liran/sink/gen/sink"
	"github.com/liran/sink/internal/merge"
	sinkmetrics "github.com/liran/sink/internal/metrics"
	"github.com/liran/sink/internal/queue"
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

const (
	healthCheckInterval = 5 * time.Second
	healthCheckTimeout  = 3 * time.Second
)

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
	config          config
	mongoClients    map[string]*mongo.Client
	storage         storagecontract.Storage
	publisher       queue.Publisher
	kafkaPublishers []*queuekafka.Publisher
	workers         []configuredWorker
	batchingServer  *service.BatchingServer
	grpcServer      *grpc.Server
	health          *health.Server
	listener        net.Listener
	metricsServer   *http.Server
	metricsListener net.Listener
}

type configuredWorker struct {
	store  string
	worker *queuekafka.Worker
}

func newApplication(ctx context.Context, loaded config) (*application, error) {
	opened, err := openConfiguredStorage(ctx, loaded)
	if err != nil {
		return nil, err
	}
	app := &application{config: loaded, mongoClients: opened.mongoClients, storage: opened.value}
	var observed *sinkmetrics.Metrics
	if loaded.prometheusAddress != "" {
		observed, err = sinkmetrics.New(version)
		if err != nil {
			app.close()
			return nil, err
		}
		if err := app.configurePrometheus(observed.Handler()); err != nil {
			app.close()
			return nil, err
		}
	}
	if loaded.mode == modeServer || loaded.mode == modeAll {
		storePublishers := make(map[string]queue.Publisher)
		for _, configured := range loaded.storages {
			if !configured.kafka.configured {
				continue
			}
			publisherOptions := queuekafka.PublisherOptions{
				Brokers: configured.kafka.brokers,
				Topic:   configured.kafka.topic,
				Metrics: observed,
			}
			publisher, publisherErr := queuekafka.NewPublisher(publisherOptions)
			if publisherErr != nil {
				app.close()
				return nil, fmt.Errorf("create Kafka publisher for store %q: %w", configured.name, publisherErr)
			}
			app.kafkaPublishers = append(app.kafkaPublishers, publisher)
			if pingErr := publisher.Ping(ctx); pingErr != nil {
				app.close()
				return nil, fmt.Errorf("ping Kafka for store %q: %w", configured.name, pingErr)
			}
			storePublishers[configured.name] = publisher
		}
		if len(storePublishers) > 0 {
			app.publisher, err = queue.NewRoutingPublisher(storePublishers)
			if err != nil {
				app.close()
				return nil, err
			}
		}
	}

	luaEngine, err := merge.NewLuaEngine(loaded.luaOptions)
	if err != nil {
		app.close()
		return nil, err
	}
	serverOptions := service.Options{
		Storage:          opened.value,
		Lua:              luaEngine,
		Publisher:        app.publisher,
		MaxOperations:    loaded.maxOperations,
		MaxMergeAttempts: loaded.maxMergeAttempts,
		Metrics:          observed,
	}
	sinkServer, err := service.New(serverOptions)
	if err != nil {
		app.close()
		return nil, err
	}
	if loaded.mode == modeServer || loaded.mode == modeAll {
		var grpcService sink.SinkServer = sinkServer
		if loaded.batchingEnabled {
			storeNames := make([]string, len(loaded.storages))
			for index, configured := range loaded.storages {
				storeNames[index] = configured.name
			}
			batchingOptions := service.BatchingOptions{
				StoreNames:          storeNames,
				MaxWait:             loaded.batchingMaxWait,
				MaxOperations:       loaded.batchingMaxOperations,
				MaxBytes:            loaded.batchingMaxBytes,
				MaxQueuedOperations: loaded.batchingMaxQueuedOps,
				MaxQueuedBytes:      loaded.batchingMaxQueuedBytes,
				Metrics:             observed,
			}
			app.batchingServer, err = service.NewBatchingServer(sinkServer, batchingOptions)
			if err != nil {
				app.close()
				return nil, err
			}
			grpcService = app.batchingServer
		}
		if err := app.configureGRPC(grpcService, observed); err != nil {
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
		for _, configured := range loaded.storages {
			if !configured.kafka.configured {
				continue
			}
			workerOptions := queuekafka.WorkerOptions{
				Brokers:          configured.kafka.brokers,
				Store:            configured.name,
				Topic:            configured.kafka.topic,
				GroupID:          configured.kafka.groupID,
				DeadLetterTopic:  configured.kafka.deadLetterTopic,
				Handler:          processor,
				MaxPollRecords:   configured.kafka.maxPollRecords,
				MaxRetryAttempts: configured.kafka.maxRetryAttempts,
				RetryBackoff:     configured.kafka.retryBackoff,
				MaxRetryBackoff:  configured.kafka.maxRetryBackoff,
				Metrics:          observed,
			}
			kafkaWorker, workerErr := queuekafka.NewWorker(workerOptions)
			if workerErr != nil {
				app.close()
				return nil, fmt.Errorf("create Kafka worker for store %q: %w", configured.name, workerErr)
			}
			workerInstance := configuredWorker{store: configured.name, worker: kafkaWorker}
			app.workers = append(app.workers, workerInstance)
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
		Store:               configured.name,
		MetadataField:       configured.mongoMetadataField,
		MaxConcurrentWrites: configured.mongoMaxConcurrentWrites,
		MaxConcurrentGroups: configured.mongoMaxConcurrentGroups,
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

func (a *application) configureGRPC(server sink.SinkServer, observed *sinkmetrics.Metrics) error {
	listener, err := net.Listen("tcp", a.config.grpcAddress)
	if err != nil {
		return fmt.Errorf("listen for gRPC: %w", err)
	}
	serverOptions := make([]grpc.ServerOption, 0, 3)
	serverOptions = append(serverOptions, grpc.MaxRecvMsgSize(a.config.grpcMaxReceiveBytes))
	serverOptions = append(serverOptions, grpc.MaxSendMsgSize(a.config.grpcMaxSendBytes))
	if observed != nil {
		interceptor := observed.UnaryServerInterceptor()
		serverOptions = append(serverOptions, grpc.UnaryInterceptor(interceptor))
	}
	grpcServer := grpc.NewServer(serverOptions...)
	sink.RegisterSinkServer(grpcServer, server)
	healthServer := health.NewServer()
	healthServer.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	healthpb.RegisterHealthServer(grpcServer, healthServer)
	a.listener = listener
	a.grpcServer = grpcServer
	a.health = healthServer
	return nil
}

func (a *application) configurePrometheus(handler http.Handler) error {
	listener, err := net.Listen("tcp", a.config.prometheusAddress)
	if err != nil {
		return fmt.Errorf("listen for Prometheus metrics: %w", err)
	}
	mux := http.NewServeMux()
	mux.Handle("/metrics", handler)
	server := &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	a.metricsListener = listener
	a.metricsServer = server
	return nil
}

func (a *application) run(ctx context.Context) error {
	runContext, cancel := context.WithCancel(ctx)
	defer cancel()
	runErrors := make(chan error, 2+len(a.workers))
	if a.grpcServer != nil {
		go func() {
			err := a.grpcServer.Serve(a.listener)
			if err != nil && !errors.Is(err, grpc.ErrServerStopped) {
				runErrors <- fmt.Errorf("serve gRPC: %w", err)
			}
		}()
	}
	if a.metricsServer != nil {
		go func() {
			err := a.metricsServer.Serve(a.metricsListener)
			if err != nil && !errors.Is(err, http.ErrServerClosed) {
				runErrors <- fmt.Errorf("serve Prometheus metrics: %w", err)
			}
		}()
	}
	for _, configured := range a.workers {
		go func() {
			if err := configured.worker.Run(runContext); err != nil {
				runErrors <- fmt.Errorf("run Kafka worker for store %q: %w", configured.store, err)
			}
		}()
	}
	if a.health != nil {
		go a.runHealthChecks(runContext)
	}
	select {
	case <-runContext.Done():
		return nil
	case err := <-runErrors:
		return err
	}
}

func (a *application) runHealthChecks(ctx context.Context) {
	ticker := time.NewTicker(healthCheckInterval)
	defer ticker.Stop()
	for {
		a.updateHealth(ctx)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (a *application) updateHealth(parent context.Context) {
	ctx, cancel := context.WithTimeout(parent, healthCheckTimeout)
	defer cancel()
	status := healthpb.HealthCheckResponse_SERVING
	if err := a.storage.Ping(ctx); err != nil {
		status = healthpb.HealthCheckResponse_NOT_SERVING
	}
	for _, publisher := range a.kafkaPublishers {
		if status != healthpb.HealthCheckResponse_SERVING {
			break
		}
		if err := publisher.Ping(ctx); err != nil {
			status = healthpb.HealthCheckResponse_NOT_SERVING
		}
	}
	a.health.SetServingStatus("", status)
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
	if a.listener != nil {
		_ = a.listener.Close()
	}
	if a.batchingServer != nil {
		a.batchingServer.Close()
	}
	if a.metricsServer != nil {
		ctx, cancel := context.WithTimeout(context.Background(), a.config.shutdownTimeout)
		defer cancel()
		if err := a.metricsServer.Shutdown(ctx); err != nil {
			slog.Error("shut down Prometheus metrics", "error", err)
			_ = a.metricsServer.Close()
		}
	}
	if a.metricsListener != nil {
		_ = a.metricsListener.Close()
	}
	for _, configured := range a.workers {
		configured.worker.Close()
	}
	for _, publisher := range a.kafkaPublishers {
		publisher.Close()
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
