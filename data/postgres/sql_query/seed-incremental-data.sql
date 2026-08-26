-- Incremental seed for source_db.
-- Appends new data only (no TRUNCATE), simulating new monthly arrivals.

BEGIN;

-- Add 150 new customers.
WITH base AS (
    SELECT
        COALESCE(MAX(customer_id), 0) AS max_id,
        COALESCE(MAX(signup_date), DATE '2025-01-01') AS max_signup_date
    FROM customers
)
INSERT INTO customers (full_name, email, city, signup_date, is_active)
SELECT
    'Customer ' || (base.max_id + g),
    'customer_new_' || (base.max_id + g) || '@example.com',
    (ARRAY['New York','Chicago','San Francisco','Austin','Seattle','Denver'])[1 + (g % 6)],
    base.max_signup_date + g,
    (g % 12) <> 0
FROM base, generate_series(1, 150) AS g;

-- Add 130 new products.
WITH base AS (
    SELECT
        COALESCE(MAX(product_id), 0) AS max_id,
        COALESCE(MAX(created_at), NOW()) AS max_created_at
    FROM products
)
INSERT INTO products (product_name, category, unit_price, in_stock_qty, created_at)
SELECT
    'Product ' || (base.max_id + g),
    (ARRAY['Electronics','Grocery','Books','Home','Sports'])[1 + (g % 5)],
    ROUND((12 + ((base.max_id + g) * 1.19))::numeric, 2),
    30 + (g % 220),
    base.max_created_at + (g * INTERVAL '15 minutes')
FROM base, generate_series(1, 130) AS g;

-- Add 170 new orders using currently available customer/product IDs.
WITH bounds AS (
    SELECT
        COALESCE(MIN(customer_id), 1) AS min_customer_id,
        COALESCE(MAX(customer_id), 1) AS max_customer_id,
        COALESCE(MIN(product_id), 1) AS min_product_id,
        COALESCE(MAX(product_id), 1) AS max_product_id
    FROM customers
    CROSS JOIN products
),
base AS (
    SELECT COALESCE(MAX(order_ts), NOW()) AS max_order_ts
    FROM orders
),
generated AS (
    SELECT
        g,
        bounds.min_customer_id + ((g - 1) % (bounds.max_customer_id - bounds.min_customer_id + 1)) AS customer_id,
        bounds.min_product_id + ((g - 1) % (bounds.max_product_id - bounds.min_product_id + 1)) AS product_id,
        1 + (g % 6) AS quantity,
        base.max_order_ts + (g * INTERVAL '10 minutes') AS order_ts
    FROM bounds
    CROSS JOIN base
    CROSS JOIN generate_series(1, 170) AS g
)
INSERT INTO orders (customer_id, product_id, quantity, order_status, order_ts, total_amount)
SELECT
    generated.customer_id,
    generated.product_id,
    generated.quantity,
    (ARRAY['NEW','PAID','SHIPPED','DELIVERED','CANCELLED'])[1 + (g % 5)],
    generated.order_ts,
    ROUND((generated.quantity * COALESCE(products.unit_price, 10))::numeric, 2)
FROM generated
LEFT JOIN products ON products.product_id = generated.product_id;

COMMIT;
