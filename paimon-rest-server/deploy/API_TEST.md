# Paimon REST Catalog Server API 测试文档

> 以下示例假设服务地址为 `http://localhost:18080`，prefix 为 `paimon`。
> 请根据实际部署替换 `HOST` 和 `PREFIX`。

```bash
export BASE=http://localhost:18080/v1/paimon
```

---

## 1. 健康检查

```bash
curl -s "${BASE%/*}/config" | python -m json.tool
```

---

## 2. Database 操作

### 创建 Database

```bash
curl -s -X POST ${BASE}/databases \
    -H "Content-Type: application/json" \
    -d '{
        "name": "test_db",
        "options": {"env": "test", "owner": "data-team"}
    }' -w "\nHTTP %{http_code}\n"
```

### 查看 Database

```bash
curl -s ${BASE}/databases/test_db | python -m json.tool
```

### 列出所有 Database

```bash
curl -s ${BASE}/databases | python -m json.tool
```

### 列出 Database（带分页）

```bash
curl -s "${BASE}/databases?maxResults=10&pageToken=" | python -m json.tool
```

### 列出 Database（带模式匹配）

```bash
curl -s "${BASE}/databases?databaseNamePattern=test%" | python -m json.tool
```

### 修改 Database

```bash
curl -s -X POST ${BASE}/databases/test_db \
    -H "Content-Type: application/json" \
    -d '{
        "removals": ["env"],
        "updates": {"owner": "new-team", "description": "updated"}
    }' | python -m json.tool
```

### 删除 Database

```bash
curl -s -X DELETE ${BASE}/databases/test_db -w "HTTP %{http_code}\n"
```

---

## 3. Table 操作

### 创建 Table

```bash
curl -s -X POST ${BASE}/databases/test_db/tables \
    -H "Content-Type: application/json" \
    -d '{
        "identifier": {"database": "test_db", "object": "users"},
        "schema": {
            "fields": [
                {"id": 0, "name": "id", "type": "INT"},
                {"id": 1, "name": "name", "type": "STRING"},
                {"id": 2, "name": "age", "type": "INT"},
                {"id": 3, "name": "dt", "type": "STRING"}
            ],
            "partitionKeys": ["dt"],
            "primaryKeys": ["id"],
            "options": {"bucket": "4"},
            "comment": "User table"
        }
    }' -w "\nHTTP %{http_code}\n"
```

### 查看 Table

```bash
curl -s ${BASE}/databases/test_db/tables/users | python -m json.tool
```

### 列出 Table

```bash
curl -s ${BASE}/databases/test_db/tables | python -m json.tool
```

### 列出 Table 详情

```bash
curl -s ${BASE}/databases/test_db/table-details | python -m json.tool
```

### 全局列出 Table

```bash
curl -s "${BASE}/tables?databaseNamePattern=%&tableNamePattern=%" | python -m json.tool
```

### 删除 Table

```bash
curl -s -X DELETE ${BASE}/databases/test_db/tables/users -w "HTTP %{http_code}\n"
```

### 重命名 Table

```bash
curl -s -X POST ${BASE}/tables/rename \
    -H "Content-Type: application/json" \
    -d '{
        "source": {"database": "test_db", "table": "users"},
        "destination": {"database": "test_db", "table": "users_v2"}
    }' -w "\nHTTP %{http_code}\n"
```

---

## 4. Branch 操作

### 创建 Branch

```bash
curl -s -X POST ${BASE}/databases/test_db/tables/users/branches \
    -H "Content-Type: application/json" \
    -d '{
        "branch": "dev-branch",
        "fromTag": null
    }' -w "\nHTTP %{http_code}\n"
```

### 从 Snapshot 创建 Branch

```bash
curl -s -X POST ${BASE}/databases/test_db/tables/users/branches \
    -H "Content-Type: application/json" \
    -d '{
        "branch": "snap-branch",
        "fromTag": null,
        "fromSnapshotId": 1
    }' -w "\nHTTP %{http_code}\n"
```

### 列出 Branch

```bash
curl -s ${BASE}/databases/test_db/tables/users/branches | python -m json.tool
```

### 查看 Branch

```bash
curl -s ${BASE}/databases/test_db/tables/users/branches/dev-branch | python -m json.tool
```

### Fast-Forward Branch

```bash
curl -s -X POST ${BASE}/databases/test_db/tables/users/branches/dev-branch/forward \
    -w "HTTP %{http_code}\n"
```

### 删除 Branch

```bash
curl -s -X DELETE ${BASE}/databases/test_db/tables/users/branches/dev-branch \
    -w "HTTP %{http_code}\n"
```

---

## 5. Tag 操作

### 创建 Tag

```bash
curl -s -X POST ${BASE}/databases/test_db/tables/users/tags \
    -H "Content-Type: application/json" \
    -d '{
        "tagName": "v1.0",
        "snapshotId": 1,
        "timeRetained": null
    }' -w "\nHTTP %{http_code}\n"
```

### 查看 Tag

```bash
curl -s ${BASE}/databases/test_db/tables/users/tags/v1.0 | python -m json.tool
```

### 列出 Tag

```bash
curl -s ${BASE}/databases/test_db/tables/users/tags | python -m json.tool
```

### 删除 Tag

```bash
curl -s -X DELETE ${BASE}/databases/test_db/tables/users/tags/v1.0 \
    -w "HTTP %{http_code}\n"
```

---

## 6. Snapshot 操作

### 获取最新 Snapshot

```bash
curl -s ${BASE}/databases/test_db/tables/users/snapshot | python -m json.tool
```

### 按版本获取 Snapshot

```bash
# 按 snapshot ID
curl -s ${BASE}/databases/test_db/tables/users/snapshots/1 | python -m json.tool

# 获取最新
curl -s ${BASE}/databases/test_db/tables/users/snapshots/LATEST | python -m json.tool

# 获取最早
curl -s ${BASE}/databases/test_db/tables/users/snapshots/EARLIEST | python -m json.tool
```

### 列出 Snapshot

```bash
curl -s "${BASE}/databases/test_db/tables/users/snapshots?maxResults=10" | python -m json.tool
```

### Rollback

```bash
curl -s -X POST ${BASE}/databases/test_db/tables/users/rollback \
    -H "Content-Type: application/json" \
    -d '{
        "instant": {"type": "snapshot", "snapshotId": 1},
        "fromSnapshot": null
    }' -w "\nHTTP %{http_code}\n"
```

---

## 7. Commit 操作

### 列出 Commit（基于 Snapshot）

```bash
curl -s "${BASE}/databases/test_db/tables/users/commits?maxResults=10" | python -m json.tool
```

### 查看单个 Commit

```bash
curl -s ${BASE}/databases/test_db/tables/users/commits/1 | python -m json.tool
```

### Reset Commit（回滚到指定 Snapshot）

```bash
curl -s -X POST ${BASE}/databases/test_db/tables/users/commits/1/reset \
    -w "\nHTTP %{http_code}\n"
```

---

## 8. Schema 历史

### 列出 Schema 版本

```bash
curl -s ${BASE}/databases/test_db/tables/users/schemas | python -m json.tool
```

### 查看指定 Schema

```bash
curl -s ${BASE}/databases/test_db/tables/users/schemas/0 | python -m json.tool
```

---

## 9. Partition 操作

### 列出 Partition

```bash
curl -s ${BASE}/databases/test_db/tables/users/partitions | python -m json.tool
```

### 按名称查询 Partition

```bash
curl -s -X POST ${BASE}/databases/test_db/tables/users/partitions/list-by-names \
    -H "Content-Type: application/json" \
    -d '{
        "specs": [{"dt": "2024-01-01"}, {"dt": "2024-01-02"}]
    }' | python -m json.tool
```

### 标记 Partition 完成

```bash
curl -s -X POST ${BASE}/databases/test_db/tables/users/partitions/mark \
    -H "Content-Type: application/json" \
    -d '{
        "specs": [{"dt": "2024-01-01"}]
    }' -w "HTTP %{http_code}\n"
```

---

## 10. Consumer 操作

### 列出 Consumer

```bash
curl -s ${BASE}/databases/test_db/tables/users/consumers | python -m json.tool
```

### 重置 Consumer

```bash
curl -s -X POST ${BASE}/databases/test_db/tables/users/consumers/reset \
    -H "Content-Type: application/json" \
    -d '{
        "consumerId": "my-consumer",
        "nextSnapshotId": 1
    }' -w "HTTP %{http_code}\n"
```

---

## 11. Table Token

### 获取 Token

```bash
curl -s ${BASE}/databases/test_db/tables/users/token | python -m json.tool
```

---

## 12. 端到端测试脚本

完整的自动化测试流程：

```bash
#!/bin/bash
set -e
BASE=http://localhost:18080/v1/paimon

echo "=== 1. Health Check ==="
curl -sf ${BASE%/*}/config | python -m json.tool

echo "=== 2. Create Database ==="
curl -sf -X POST ${BASE}/databases \
    -H "Content-Type: application/json" \
    -d '{"name":"e2e_db","options":{"env":"test"}}'
echo " OK"

echo "=== 3. Get Database ==="
curl -sf ${BASE}/databases/e2e_db | python -m json.tool

echo "=== 4. List Databases ==="
curl -sf ${BASE}/databases | python -m json.tool

echo "=== 5. Create Table ==="
curl -sf -X POST ${BASE}/databases/e2e_db/tables \
    -H "Content-Type: application/json" \
    -d '{
        "identifier":{"database":"e2e_db","object":"orders"},
        "schema":{
            "fields":[
                {"id":0,"name":"id","type":"INT"},
                {"id":1,"name":"amount","type":"DOUBLE"},
                {"id":2,"name":"dt","type":"STRING"}
            ],
            "partitionKeys":["dt"],
            "primaryKeys":["id"],
            "options":{"bucket":"2"},
            "comment":"E2E test table"
        }
    }'
echo " OK"

echo "=== 6. Get Table ==="
curl -sf ${BASE}/databases/e2e_db/tables/orders | python -m json.tool

echo "=== 7. List Tables ==="
curl -sf ${BASE}/databases/e2e_db/tables | python -m json.tool

echo "=== 8. List Schemas ==="
curl -sf ${BASE}/databases/e2e_db/tables/orders/schemas | python -m json.tool

echo "=== 9. Get Table Token ==="
curl -sf ${BASE}/databases/e2e_db/tables/orders/token | python -m json.tool

echo "=== 10. Cleanup: Drop Table ==="
curl -sf -X DELETE ${BASE}/databases/e2e_db/tables/orders
echo " OK"

echo "=== 11. Cleanup: Drop Database ==="
curl -sf -X DELETE ${BASE}/databases/e2e_db
echo " OK"

echo ""
echo "=== All tests passed ==="
```

---

## 错误码说明

| HTTP 状态码 | 说明 |
|------------|------|
| 200 | 成功 |
| 201 | 创建成功（仅 Create Table） |
| 400 | 参数错误（如 maxResults <= 0） |
| 404 | 资源不存在（Database / Table / Branch / Tag / Snapshot / Commit） |
| 409 | 资源已存在（Database / Table / Branch / Tag） |
| 500 | 服务端内部错误 |
| 501 | 功能未实现（如 Branch Merge / Diff） |
