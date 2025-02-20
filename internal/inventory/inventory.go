package inventory

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/achere/heroku-kafka-demo-go/db/sqlc"
)

type inventoryStore interface {
	inventoryGetter
	UpdateInventory(ctx context.Context, arg db.UpdateInventoryParams) error
	InsertStockLog(ctx context.Context, arg db.InsertStockLogParams) error
}

type inventoryGetter interface {
	GetInventory(ctx context.Context, arg db.GetInventoryParams) (db.GetInventoryRow, error)
}

type Cache interface {
	Get(ctx context.Context, key string) (string, error)
	Set(ctx context.Context, key, value string, expiration time.Duration) error
}

// UpdateInventory function takes product and warehouse IDs along with the stock delta, updates the
// stock if possible and returns the updated stock along with the threshold for low stock alert
func UpdateInventory(
	productID int,
	warehouseID int,
	stockDelta int,
	store inventoryStore,
	ctx context.Context,
	c Cache,
) (int, int, error) {
	var stock, threshold int

	whID := int32(warehouseID)
	prodID := int32(productID)

	cacheKey := fmt.Sprintf("%d:%d", whID, prodID)

	stock, threshold, ok := getInvFromCache(ctx, c, cacheKey)

	if !ok {
		inv, err := store.GetInventory(
			ctx,
			db.GetInventoryParams{
				WarehouseID: whID,
				ProductID:   prodID,
			},
		)
		if err != nil {
			return 0, 0, err
		}
		slog.Info("db get", "at", "inventory", "query", "GetInventory", "value", fmt.Sprintf("%+v", inv))

		stock = int(inv.StockLevel)
		threshold = int(inv.AlertThreshold)
	}

	newStock := stock + stockDelta
	if newStock < 0 {
		return 0, 0, fmt.Errorf("applying delta %d to stock %d would make it negative", stockDelta, stock)
	}

	slog.Info("db dml", "at", "inventory", "action", "UpdateInvenotry", "value", newStock)
	updStock := int32(newStock)
	err := store.UpdateInventory(
		ctx,
		db.UpdateInventoryParams{
			StockLevel:  updStock,
			WarehouseID: whID,
			ProductID:   prodID,
		},
	)
	if err != nil {
		return 0, 0, err
	}

	cacheVal := fmt.Sprintf("%d,%d", newStock, threshold)
	err = c.Set(ctx, cacheKey, cacheVal, 0)
	if err != nil {
		slog.Error("cache set err", "at", "inventory", "err", err)
	} else {
		slog.Info("cache set", "at", "inventory", "key", cacheKey, "value", cacheVal)
	}

	err = store.InsertStockLog(
		ctx,
		db.InsertStockLogParams{
			PreviousStock: int32(stock),
			UpdatedStock:  updStock,
			WarehouseID:   whID,
			ProductID:     prodID,
		},
	)
	if err != nil {
		return 0, 0, err
	}

	return newStock, threshold, nil
}

// FetchInventory function takes product and warehouse IDs as a paramater and returns matching stock
func FetchInventory(
	productID int,
	warehouseID int,
	store inventoryGetter,
	ctx context.Context,
	c Cache,
) (int, error) {
	cacheKey := fmt.Sprintf("%d:%d", warehouseID, productID)
	stock, threshold, ok := getInvFromCache(ctx, c, cacheKey)

	if !ok {
		inv, err := store.GetInventory(
			ctx,
			db.GetInventoryParams{
				WarehouseID: int32(warehouseID),
				ProductID:   int32(productID),
			},
		)
		if err != nil {
			return 0, err
		}
		slog.Info("db get", "at", "inventory", "query", "GetInventory", "value", fmt.Sprintf("%+v", inv))

		stock = int(inv.StockLevel)
		threshold = int(inv.AlertThreshold)
	}

	cacheVal := fmt.Sprintf("%d,%d", stock, threshold)
	err := c.Set(ctx, cacheKey, cacheVal, 0)
	if err != nil {
		slog.Error("cache set err", "at", "inventory", "err", err)
	} else {
		slog.Info("cache set", "at", "inventory", "key", cacheKey, "value", cacheVal)
	}

	return stock, nil
}

func getInvFromCache(ctx context.Context, c Cache, key string) (int, int, bool) {
	cacheValid := false
	var stock, threshold int
	invCached, err := c.Get(ctx, key)
	if err != nil {
		slog.Error("cache get err", "at", "inventory", "err", err)
	}
	slog.Info("cache get", "at", "inventory", "value", invCached)

	if invCached != "" {
		vals := strings.Split(invCached, ",")
		if len(vals) != 2 {
			slog.Error("cache parse err", "at", "inventory", "err", "invalid cache format")
			return 0, 0, false
		}

		cachedStock, errStock := strconv.Atoi(vals[0])
		cachedThreshold, errThresh := strconv.Atoi(vals[1])

		if errStock != nil && errThresh != nil {
			slog.Error("cache parse err", "at", "inventory", "err", "invalid cache format")
			return 0, 0, false
		} else {
			cacheValid = true
			stock, threshold = cachedStock, cachedThreshold
		}
	}

	return stock, threshold, cacheValid
}
