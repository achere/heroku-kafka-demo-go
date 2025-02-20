package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/IBM/sarama"
	sqlc "github.com/achere/heroku-kafka-demo-go/db/sqlc"
	"github.com/achere/heroku-kafka-demo-go/internal/api"
	"github.com/achere/heroku-kafka-demo-go/internal/config"
	"github.com/achere/heroku-kafka-demo-go/internal/inventory"
	"github.com/achere/heroku-kafka-demo-go/internal/transport"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

const (
	// MaxBufferSize is the maximum number of messages to keep in the buffer
	MaxBufferSize = 10
)

func main() {
	if os.Getenv("KAFKA_DEBUG") != "" {
		sarama.Logger = log.New(os.Stdout, "[sarama] ", log.LstdFlags)
	}

	ctx, cancel := context.WithCancel(context.Background())

	appconfig, err := config.NewAppConfig()
	if err != nil {
		slog.Error(
			"error loading config",
			"at", "main",
			"err", err,
		)
	}

	port := appconfig.Web.Port

	slog.Info(
		"starting up",
		"at", "main",
		"topic", appconfig.Topic(),
		"port", port,
		"consumer_group", appconfig.Group(),
		"broker_addresses", appconfig.BrokerAddresses(),
		"prefix:", appconfig.Kafka.Prefix,
	)

	db, err := pgxpool.New(ctx, appconfig.DatabaseURL)
	if err != nil {
		slog.Error("error connecting to db", "err", err)
	} else {
		slog.Info("db connected")
	}
	defer db.Close()

	redisUrl := appconfig.RedisURL
	opts, err := redis.ParseURL(redisUrl)
	if strings.HasPrefix(redisUrl, "rediss") {
		opts.TLSConfig = &tls.Config{
			InsecureSkipVerify: true,
		}
	}
	if err != nil {
		slog.Error("error parsing Redis URL", "at", "main", "err", err)
	}

	rdb := redis.NewClient(opts)

	_, err = rdb.Ping(ctx).Result()
	if err != nil {
		slog.Error("could not connect to Redis", "err", err)
	} else {
		slog.Info("redis connected")
	}

	client, err := transport.NewKafkaClient(appconfig)
	if err != nil {
		log.Fatal(err)
	}

	topic := appconfig.Topic()
	buffer := transport.MessageBuffer{
		MaxSize: MaxBufferSize,
	}
	consumerHandler := transport.NewMessageHandler(&buffer, newStockUpdateHandler(
		ctx,
		appconfig,
		client,
		db,
		inventory.NewRedisCache(rdb),
	))

	go client.ConsumeMessages(ctx, []string{topic}, consumerHandler)

	slog.Info(
		"waiting for consumer to be ready",
		"at", "main",
		"topic", topic,
	)

	start := time.Now()

	<-consumerHandler.Ready

	slog.Info(
		"consumer is ready",
		"at", "main",
		"topic", topic,
		"duration_ms", time.Since(start).Milliseconds(),
	)

	inventoryHandler := api.NewInventoryHandler(sqlc.New(db), rdb)
	http.HandleFunc("GET /inventory", inventoryHandler.HandleGetInventory)

	server := &http.Server{
		Addr:         fmt.Sprintf(":%s", port),
		Handler:      nil,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  15 * time.Second,
	}

	slog.Info("starting server", "at", "main", "port", port)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatal(err)
	}

	client.Close()
	cancel()
}
