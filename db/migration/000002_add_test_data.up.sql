INSERT INTO warehouses (
    name, location
) VALUES ('slaughterhouse 5', 'dresden');

INSERT INTO products (
    name, description, price
) VALUES ('banana', 'yellow', 4.20);

INSERT INTO inventory (
    product_id, warehouse_id, stock_level, alert_threshold
) VALUES (1, 1, 10, 5);
