package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/IBM/sarama"
	sqlc "github.com/achere/heroku-kafka-demo-go/db/sqlc"
	"github.com/achere/heroku-kafka-demo-go/internal/config"
	"github.com/achere/heroku-kafka-demo-go/internal/inventory"
	"github.com/achere/heroku-kafka-demo-go/internal/transport"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type LowStockAlert struct {
	ProductID    int `json:"product_id"`
	WarehouseID  int `json:"warehouse_id"`
	CurrentStock int `json:"current_stock"`
	Threshold    int `json:"threshold"`
}

type StockUpdate struct {
	ProductID   int `json:"product_id"`
	WarehouseID int `json:"warehouse_id"`
	StockDelta  int `json:"stock_delta"`
}

func newStockUpdateHandler(
	ctx context.Context,
	appconfig *config.AppConfig,
	client *transport.KafkaClient,
	dbpool *pgxpool.Pool,
	cache inventory.Cache,
) transport.MessageHandlerFunc {
	return func(cm *sarama.ConsumerMessage) error {
		slog.Info(
			"handling msg",
			"topic", cm.Topic,
			"key", cm.Key,
			"value", cm.Value,
		)

		var su StockUpdate
		err := json.Unmarshal(cm.Value, &su)
		if err != nil {
			return fmt.Errorf("error unmarshalling stock update: %v", err)
		}

		// TODO: extract transaction handling to db package? Pass return variables in a closure
		tx, err := dbpool.BeginTx(ctx, pgx.TxOptions{})
		if err != nil {
			return fmt.Errorf("error initiating transaction, %v", err)
		}

		stock, threshold, err := inventory.UpdateInventory(
			su.ProductID, su.WarehouseID, su.StockDelta, sqlc.New(tx), ctx, cache,
		)
		if err != nil {
			if txErr := tx.Rollback(ctx); txErr != nil {
				return fmt.Errorf(
					"tried to roll back transaction due to error updating stock %v, error rolling back the transaction: %v",
					err, txErr,
				)
			}

			return fmt.Errorf("error updating stock: %v", err)
		}

		if err = tx.Commit(ctx); err != nil {
			return fmt.Errorf("error committing to DB: %v", err)
		}

		if stock < threshold {
			alert := LowStockAlert{
				ProductID:    su.ProductID,
				WarehouseID:  su.WarehouseID,
				CurrentStock: stock,
				Threshold:    threshold,
			}
			alertMessage, err := json.Marshal(alert)
			if err != nil {
				return fmt.Errorf("error marshalling low-stock alert: %v", err)
			}

			topic := appconfig.ProducerTopic()
			value := sarama.ByteEncoder(alertMessage)

			err = client.SendMessage(topic, "", value)
			if err != nil {
				return fmt.Errorf("error sending low-stock alert: %v", err)
			} else {
				slog.Info("alert sent", "topic", topic, "value", value)
			}
		}
		return nil
	}
}

type CachePlaceholder struct{}

func (cp *CachePlaceholder) Get(ctx context.Context, key string) (string, error) {
	slog.Info("get from cache", "key", key)
	return "", nil
}

func (cp *CachePlaceholder) Set(ctx context.Context, key, value string, expiration time.Duration) error {
	slog.Info("set cache", "key", key, "value", value)
	return nil
}

func newCachePlaceholder() inventory.Cache {
	return &CachePlaceholder{}
}
