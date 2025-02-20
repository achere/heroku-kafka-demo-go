CREATE TABLE products (
    product_id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    price DECIMAL(10, 2)
);

CREATE TABLE warehouses (
    warehouse_id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    location VARCHAR(255)
);

CREATE TABLE inventory (
    product_id INT REFERENCES products(product_id),
    warehouse_id INT REFERENCES warehouses(warehouse_id),
    stock_level INT NOT NULL,
    alert_threshold INT NOT NULL,
    PRIMARY KEY (product_id, warehouse_id)
);

CREATE TABLE stock_logs (
    log_id SERIAL PRIMARY KEY,
    product_id INT REFERENCES products(product_id) NOT NULL,
    warehouse_id INT REFERENCES warehouses(warehouse_id) NOT NULL,
    previous_stock INT NOT NULL,
    updated_stock INT NOT NULL,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
