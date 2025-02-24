package transport

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"github.com/achere/heroku-kafka-demo-go/internal/config"
)

// Message represents a Kafka message
type Message struct {
	Metadata  MessageMetadata `json:"metadata"`
	Value     string          `json:"value"`
	Partition int32           `json:"partition"`
	Offset    int64           `json:"offset"`
}

// MessageMetadata represents metadata about a Kafka message
type MessageMetadata struct {
	ReceivedAt time.Time `json:"receivedAt"`
}

// MessageBuffer is a buffer of Kafka messages
// Concurrent access to the buffer is protected via a RWMutex
type MessageBuffer struct {
	receivedMessages []Message
	MaxSize          int

	ml sync.RWMutex
}

// SaveMessage saves a message to the buffer
func (mb *MessageBuffer) SaveMessage(msg Message) {
	mb.ml.Lock()
	defer mb.ml.Unlock()

	if len(mb.receivedMessages) >= mb.MaxSize {
		// Remove the oldest message
		mb.receivedMessages = mb.receivedMessages[1:]
	}

	mb.receivedMessages = append(mb.receivedMessages, msg)
}

type MessageHandlerFunc func(*sarama.ConsumerMessage) error

// MessageHandler is a Sarama consumer group handler
type MessageHandler struct {
	Ready         chan bool
	buffer        *MessageBuffer
	handleMessage MessageHandlerFunc
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (c *MessageHandler) Setup(sarama.ConsumerGroupSession) error {
	close(c.Ready)

	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (c *MessageHandler) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// saveMessage saves a consumer message to the buffer
func (c *MessageHandler) saveMessage(msg *sarama.ConsumerMessage) {
	c.buffer.SaveMessage(Message{
		Partition: msg.Partition,
		Offset:    msg.Offset,
		Value:     string(msg.Value),
		Metadata: MessageMetadata{
			ReceivedAt: time.Now(),
		},
	})
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (c *MessageHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case msg, ok := <-claim.Messages():
			if !ok {
				slog.Error("message channel was closed")
				return nil
			}

			c.saveMessage(msg)
			err := c.handleMessage(msg)
			if err != nil {
				slog.Error("error processing msg",
					"err", err,
					"timestamp", msg.Timestamp,
					"val", sarama.StringEncoder(msg.Value),
				)
			} else {
				session.MarkMessage(msg, "")
			}
		case <-session.Context().Done():
			return nil
		}
	}
}

// NewMessageHandler creates a new MessageHandler
func NewMessageHandler(buffer *MessageBuffer, handler MessageHandlerFunc) *MessageHandler {
	return &MessageHandler{
		Ready:         make(chan bool),
		buffer:        buffer,
		handleMessage: handler,
	}
}

// CreateKafkaAsyncProducer creates a new Sarama AsyncProducer
func CreateKafkaAsyncProducer(ac *config.AppConfig) (sarama.AsyncProducer, error) {
	const flushFrequency = 500 * time.Millisecond

	kafkaConfig := sarama.NewConfig()
	kafkaConfig.Producer.RequiredAcks = sarama.WaitForAll
	kafkaConfig.Producer.Retry.Max = 10
	kafkaConfig.Producer.Compression = sarama.CompressionZSTD
	kafkaConfig.Producer.Flush.Frequency = flushFrequency
	kafkaConfig.ClientID = "heroku-kafka-demo-go/asyncproducer"

	if !ac.Kafka.SkipTLS {
		tlsConfig := ac.CreateTLSConfig()
		kafkaConfig.Net.TLS.Enable = true
		kafkaConfig.Net.TLS.Config = tlsConfig
	}

	err := kafkaConfig.Validate()
	if err != nil {
		return nil, err
	}

	producer, err := sarama.NewAsyncProducer(ac.BrokerAddresses(), kafkaConfig)
	if err != nil {
		return nil, err
	}

	return producer, nil
}

// CreateKafkaProducer creates a new Sarama SyncProducer
func CreateKafkaProducer(ac *config.AppConfig) (sarama.SyncProducer, error) {
	kafkaConfig := sarama.NewConfig()
	kafkaConfig.Producer.RequiredAcks = sarama.WaitForAll
	kafkaConfig.Producer.Retry.Max = 10
	kafkaConfig.Producer.Return.Successes = true
	kafkaConfig.Producer.Compression = sarama.CompressionZSTD
	kafkaConfig.ClientID = "heroku-kafka-demo-go/producer"

	if !ac.Kafka.SkipTLS {
		tlsConfig := ac.CreateTLSConfig()
		kafkaConfig.Net.TLS.Enable = true
		kafkaConfig.Net.TLS.Config = tlsConfig
	}

	err := kafkaConfig.Validate()
	if err != nil {
		return nil, err
	}

	producer, err := sarama.NewSyncProducer(ac.BrokerAddresses(), kafkaConfig)
	if err != nil {
		return nil, err
	}

	return producer, nil
}

// CreateKafkaConsumer creates a new Sarama ConsumerGroup
func CreateKafkaConsumer(ac *config.AppConfig) (sarama.ConsumerGroup, error) {
	kafkaConfig := sarama.NewConfig()

	if !ac.Kafka.SkipTLS {
		tlsConfig := ac.CreateTLSConfig()
		kafkaConfig.Net.TLS.Enable = true
		kafkaConfig.Net.TLS.Config = tlsConfig
	}

	kafkaConfig.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategyRoundRobin()}
	kafkaConfig.ClientID = "heroku-kafka-demo-go/consumer"
	kafkaConfig.Consumer.Offsets.AutoCommit.Enable = true
	kafkaConfig.Consumer.Offsets.AutoCommit.Interval = 1 * time.Second

	err := kafkaConfig.Validate()
	if err != nil {
		return nil, err
	}

	consumer, err := sarama.NewConsumerGroup(ac.BrokerAddresses(), ac.Group(), kafkaConfig)
	if err != nil {
		return nil, err
	}

	return consumer, nil
}

// KafkaClient is a wrapper around a Sarama SyncProducer, AsyncProducer and ConsumerGroup
type KafkaClient struct {
	Producer      sarama.SyncProducer
	AsyncProducer sarama.AsyncProducer
	Consumer      sarama.ConsumerGroup
}

// NewKafkaClient creates a new KafkaClient
func NewKafkaClient(ac *config.AppConfig) (*KafkaClient, error) {
	if !ac.Kafka.SkipTLS {
		tlsConfig := ac.CreateTLSConfig()

		ok, err := verifyBrokers(tlsConfig, ac.Kafka.TrustedCert, ac.BrokerAddresses())
		if err != nil {
			return nil, err
		}

		if !ok {
			return nil, errors.New("unable to verify brokers")
		}
	}

	producer, err := CreateKafkaProducer(ac)
	if err != nil {
		return nil, err
	}

	asyncProducer, err := CreateKafkaAsyncProducer(ac)
	if err != nil {
		return nil, err
	}

	consumer, err := CreateKafkaConsumer(ac)
	if err != nil {
		return nil, err
	}

	return &KafkaClient{
		AsyncProducer: asyncProducer,
		Producer:      producer,
		Consumer:      consumer,
	}, nil
}

// Close closes the KafkaClient
func (kc *KafkaClient) Close() {
	kc.Producer.Close()
	kc.AsyncProducer.Close()
	kc.Consumer.Close()
}

// SendAsyncMessage sends a message to Kafka asynchronously
func (kc *KafkaClient) SendAsyncMessage(topic, key string, message []byte) error {
	kc.AsyncProducer.Input() <- &sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.ByteEncoder(key),
		Value: sarama.ByteEncoder(message),
	}

	return nil
}

// SendMessage sends a message to Kafka
func (kc *KafkaClient) SendMessage(topic, key string, message []byte) error {
	_, _, err := kc.Producer.SendMessage(&sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.ByteEncoder(key),
		Value: sarama.ByteEncoder(message),
	})

	return err
}

// ConsumeMessages consumes messages from Kafka
func (kc *KafkaClient) ConsumeMessages(ctx context.Context, topics []string, handler *MessageHandler) {
	for {
		if err := kc.Consumer.Consume(ctx, topics, handler); err != nil {
			if errors.Is(err, sarama.ErrClosedConsumerGroup) {
				return
			}

			slog.Error(
				"error consuming",
				"err", err,
			)
		}

		if ctx.Err() != nil {
			return
		}

		handler.Ready = make(chan bool)
	}
}

func verifyBrokers(tc *tls.Config, caCert string, urls []string) (bool, error) {
	for _, url := range urls {
		ok, err := verifyServerCert(tc, caCert, url)
		if err != nil {
			return false, err
		}

		if !ok {
			return false, nil
		}
	}

	return true, nil
}

func verifyServerCert(tc *tls.Config, caCert, url string) (bool, error) {
	// Create connection to server
	conn, err := tls.Dial("tcp", url, tc)
	if err != nil {
		return false, err
	}

	// Pull servers cert
	serverCert := conn.ConnectionState().PeerCertificates[0]

	roots := x509.NewCertPool()

	ok := roots.AppendCertsFromPEM([]byte(caCert))
	if !ok {
		return false, errors.New("unable to parse trusted cert")
	}

	// Verify Server Cert
	opts := x509.VerifyOptions{Roots: roots}
	if _, err := serverCert.Verify(opts); err != nil {
		slog.Error("Unable to verify Server Cert")
		return false, err
	}

	return true, nil
}
