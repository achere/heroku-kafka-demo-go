-- name: GetInventory :one
SELECT
	i.product_id,
	i.warehouse_id,
	stock_level,
	p.name as product_name,
	w.name as warehouse_name,
	alert_threshold
FROM inventory AS i
INNER JOIN products as p on p.product_id = i.product_id
INNER JOIN warehouses as w on w.warehouse_id = i.warehouse_id
WHERE i.warehouse_id = $1 AND i.product_id = $2
LIMIT 1;

-- name: UpdateInventory :exec
UPDATE inventory
SET stock_level = $1
WHERE warehouse_id = $2 AND product_id = $3;

-- name: InsertStockLog :exec
INSERT INTO stock_logs (product_id, warehouse_id, previous_stock, updated_stock)
VALUES ($1, $2, $3, $4);

