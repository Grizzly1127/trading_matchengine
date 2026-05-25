-- ListOrders：按 user_id 分页（ORDER BY id DESC）
CREATE INDEX IF NOT EXISTS idx_orders_user_id_desc ON orders (user_id, id DESC);

-- ListOrders：按 user_id + 时间范围筛选
CREATE INDEX IF NOT EXISTS idx_orders_user_created_at ON orders (user_id, created_at DESC);
