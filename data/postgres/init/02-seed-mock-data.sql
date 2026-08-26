-- Seed source_db with 3 mock tables.
-- This script is safe to rerun manually.

CREATE TABLE IF NOT EXISTS customers (
    customer_id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    full_name TEXT NOT NULL,
    email TEXT NOT NULL UNIQUE,
    city TEXT NOT NULL,
    signup_date DATE NOT NULL,
    is_active BOOLEAN NOT NULL DEFAULT true
);

CREATE TABLE IF NOT EXISTS products (
    product_id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    product_name TEXT NOT NULL,
    category TEXT NOT NULL,
    unit_price NUMERIC(10,2) NOT NULL,
    in_stock_qty INT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS orders (
    order_id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    customer_id INT NOT NULL REFERENCES customers(customer_id),
    product_id INT NOT NULL REFERENCES products(product_id),
    quantity INT NOT NULL,
    order_status TEXT NOT NULL,
    order_ts TIMESTAMP NOT NULL,
    total_amount NUMERIC(12,2) NOT NULL
);

TRUNCATE TABLE orders RESTART IDENTITY;
TRUNCATE TABLE products RESTART IDENTITY CASCADE;
TRUNCATE TABLE customers RESTART IDENTITY CASCADE;

-- 60 rows
INSERT INTO customers (full_name, email, city, signup_date, is_active)
SELECT
    CASE
        WHEN (g % 11) = 0 THEN UPPER('Customer ' || g)
        WHEN (g % 9) = 0 THEN '  Customer ' || g || '  '
        WHEN (g % 7) = 0 THEN 'Customer-' || g
        ELSE 'Customer ' || g
    END,
    CASE
        WHEN (g % 19) = 0 THEN 'customer..' || g || '@example..com'
        WHEN (g % 17) = 0 THEN ' customer' || g || '@example.com '
        WHEN (g % 13) = 0 THEN 'customer' || g || 'example.com'
        WHEN (g % 8) = 0 THEN 'CUSTOMER' || g || '@EXAMPLE.COM'
        ELSE 'customer' || g || '@example.com'
    END,
    CASE
        WHEN (g % 14) = 0 THEN 'SF'
        WHEN (g % 10) = 0 THEN ' New york '
        WHEN (g % 9) = 0 THEN 'chicago'
        ELSE (ARRAY['New York','Chicago','San Francisco','Austin','Seattle','Denver'])[1 + (g % 6)]
    END,
    DATE '2023-06-01' + (g * 5),
    (g % 10) <> 0
FROM generate_series(1, 60) AS g;

-- 75 rows
INSERT INTO products (product_name, category, unit_price, in_stock_qty, created_at)
SELECT
    CASE
        WHEN (g % 12) = 0 THEN '  Product ' || g || '  '
        WHEN (g % 10) = 0 THEN 'Product-' || g
        WHEN (g % 9) = 0 THEN 'PRODUCT ' || g
        ELSE 'Product ' || g
    END,
    CASE
        WHEN (g % 16) = 0 THEN ' electronics '
        WHEN (g % 11) = 0 THEN 'GROCERY'
        WHEN (g % 7) = 0 THEN 'home'
        ELSE (ARRAY['Electronics','Grocery','Books','Home','Sports'])[1 + (g % 5)]
    END,
    ROUND((9 + (g * 1.53))::numeric, 2),
    5 + (g % 220),
    NOW() - ((g % 120) || ' days')::interval - ((g % 23) || ' hours')::interval
FROM generate_series(1, 75) AS g;

-- 90 rows
WITH generated AS (
    SELECT
        g,
        1 + (g % 60) AS customer_id,
        1 + (g % 75) AS product_id,
        1 + (g % 5) AS quantity,
        CASE
            WHEN (g % 18) = 0 THEN 'paid'
            WHEN (g % 15) = 0 THEN 'SHIPPED '
            WHEN (g % 12) = 0 THEN ' delivered'
            WHEN (g % 10) = 0 THEN 'CANCELED'
            WHEN (g % 8) = 0 THEN 'NEW '
            ELSE (ARRAY['NEW','PAID','SHIPPED','DELIVERED','CANCELLED'])[1 + (g % 5)]
        END AS order_status,
        NOW() - ((g % 90) || ' days')::interval - ((g % 24) || ' hours')::interval AS order_ts
    FROM generate_series(1, 90) AS g
)
INSERT INTO orders (customer_id, product_id, quantity, order_status, order_ts, total_amount)
SELECT
    generated.customer_id,
    generated.product_id,
    generated.quantity,
    generated.order_status,
    generated.order_ts,
    ROUND(
        (
            generated.quantity * p.unit_price *
            CASE
                WHEN (generated.g % 20) = 0 THEN 0.95
                WHEN (generated.g % 9) = 0 THEN 1.03
                ELSE 1.00
            END
        )::numeric,
        2
    )
FROM generated
JOIN products p ON p.product_id = generated.product_id;
