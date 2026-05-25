# migrations

SQL 迁移文件（建议使用 [golang-migrate](https://github.com/golang-migrate/migrate)）。

第 4 步 Order Service 起添加，例如：

- `001_create_orders.up.sql` / `001_create_orders.down.sql`
- `007_create_orders_list_index.up.sql` — `ListOrders` 列表查询索引（`user_id + id DESC` / `user_id + created_at DESC`）

也可通过 Order Service 启动项 `migrate_on_start=true` 自动执行（内嵌 SQL 与根目录 migration 一致）。

手动迁移（需安装 `psql`）：

```bash
make migrate-up
# 或 DATABASE_URL=... bash scripts/migrate-up.sh
```
