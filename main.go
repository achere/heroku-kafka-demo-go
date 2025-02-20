package main

import (
	"context"
	"crypto/tls"
	"log"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/IBM/sarama"
	"github.com/achere/heroku-kafka-demo-go/internal/config"
	"github.com/achere/heroku-kafka-demo-go/internal/inventory"
	"github.com/achere/heroku-kafka-demo-go/internal/transport"
	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

const (
	// MaxBufferSize is the maximum number of messages to keep in the buffer
	MaxBufferSize = 10
)

type IndexHandler struct {
	Topic string
}

func (i *IndexHandler) GetIndex(c *gin.Context) {
	h := gin.H{
		"baseurl": "https://" + c.Request.Host,
		"topic":   i.Topic,
	}
	c.HTML(http.StatusOK, "index.tmpl.html", h)
}

func slogger() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		path := c.Request.URL.Path

		c.Next()

		slog.Info(
			"request",
			"method", c.Request.Method,
			"ip", c.ClientIP(),
			"ua", c.Request.UserAgent(),
			"path", path,
			"status", c.Writer.Status(),
			"duration_ms", time.Since(start).Milliseconds(),
		)
	}
}

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
			"error", err,
		)
	}

	slog.Info(
		"starting up",
		"at", "main",
		"topic", appconfig.Topic(),
		"port", appconfig.Web.Port,
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
		slog.Error("error parsing Redis URL", "err", err)
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

	router := gin.New()
	router.Use(slogger())
	router.LoadHTMLGlob("templates/*.tmpl.html")
	router.Static("/public", "public")

	ih := &IndexHandler{Topic: topic}

	router.GET("/", ih.GetIndex)
	router.GET("/messages", buffer.GetMessages)
	router.POST("/messages/:topic", client.PostMessage)
	router.POST("/async-messages/:topic", client.PostAsyncMessage)

	err = router.Run(":" + appconfig.Web.Port)
	if err != nil {
		client.Close()
		cancel()
		log.Fatal(err)
	}

	client.Close()
	cancel()
}
