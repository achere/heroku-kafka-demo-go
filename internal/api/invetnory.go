package api

import (
	"encoding/json"
	"log"
	"net/http"
	"strconv"

	sqlc "github.com/achere/heroku-kafka-demo-go/db/sqlc"
	"github.com/achere/heroku-kafka-demo-go/internal/inventory"
	"github.com/redis/go-redis/v9"
)

type InventoryHandler struct {
	queries *sqlc.Queries
	rdb     *redis.Client
}

func NewInventoryHandler(queries *sqlc.Queries, rdb *redis.Client) *InventoryHandler {
	return &InventoryHandler{queries: queries, rdb: rdb}
}

func (h *InventoryHandler) HandleGetInventory(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	productID, err := strconv.Atoi(r.URL.Query().Get("product_id"))
	if err != nil {
		http.Error(w, "Invalid product_id", http.StatusBadRequest)
		return
	}

	warehouseID, err := strconv.Atoi(r.URL.Query().Get("warehouse_id"))
	if err != nil {
		http.Error(w, "Invalid warehouse_id", http.StatusBadRequest)
		return
	}

	stock, err := inventory.FetchInventory(productID, warehouseID, h.queries, ctx, inventory.NewRedisCache(h.rdb))
	if err != nil {
		log.Printf("Error fetching inventory: %v", err)
		http.Error(w, "Error fetching inventory", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(Inventory{
		ProductID:   productID,
		WarehouseID: warehouseID,
		Stock:       stock,
	})
}

type Inventory struct {
	ProductID   int `json:"product_id"`
	WarehouseID int `json:"warehouse_id"`
	Stock       int `json:"stock"`
}
