-- Invalid seed for source_db.
-- Inserts rows that satisfy DB constraints but violate ETL data-quality checks.

BEGIN;

-- Invalid products: negative price and negative stock.
WITH product_base AS (
    SELECT
        COALESCE(MAX(product_id), 0) AS max_id,
        COALESCE(MAX(created_at), NOW()) AS max_created_at
    FROM products
)
INSERT INTO products (product_name, category, unit_price, in_stock_qty, created_at)
SELECT
    'DQ_BAD_Product_' || (product_base.max_id + g),
    'Electronics',
    CASE WHEN g = 1 THEN -5.00 ELSE 25.00 END,
    CASE WHEN g = 1 THEN 20 ELSE -10 END,
    product_base.max_created_at + (g * INTERVAL '5 minutes')
FROM product_base, generate_series(1, 2) AS g;

-- Invalid orders: zero quantity and negative total amount.
WITH ids AS (
    SELECT
        (SELECT MIN(customer_id) FROM customers) AS customer_id,
        (SELECT MAX(product_id) FROM products) AS product_id,
        COALESCE((SELECT MAX(order_ts) FROM orders), NOW()) AS max_order_ts
)
INSERT INTO orders (customer_id, product_id, quantity, order_status, order_ts, total_amount)
SELECT customer_id, product_id, 0, 'PAID', max_order_ts + INTERVAL '1 minute', 0.00
FROM ids
UNION ALL
SELECT customer_id, product_id, 2, 'SHIPPED', max_order_ts + INTERVAL '2 minutes', -99.99
FROM ids;

COMMIT;
