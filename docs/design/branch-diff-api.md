# Branch Diff API 文档

## 1. 概述

Branch Diff 接口用于比较同一张表的两个分支，展示各自独有的 commit（snapshot）。类似 Git 的 `git log --left-right`，帮助用户在 merge 前了解两个分支的差异。

**端点**：`GET /v1/{prefix}/databases/{database}/tables/{table}/diff?left={branch}&right={branch}`

**实现原理**：通过 ForkInfo 链找到两个分支的公共祖先（merge-base），然后分别列出各自在公共祖先之后产生的 snapshot。

---

## 2. 请求参数

| 参数 | 位置 | 必填 | 说明 |
|------|------|------|------|
| `database` | path | 是 | 数据库名 |
| `table` | path | 是 | 表名 |
| `left` | query | 是 | 左侧分支名（空值或省略视为 `main`） |
| `right` | query | 是 | 右侧分支名（空值或省略视为 `main`） |

---

## 3. 响应结构

```json
{
  "left_ref": "分支名",
  "right_ref": "分支名",
  "merge_base": {
    "branch": "公共祖先所在分支",
    "snapshot_id": 5
  },
  "left_only": [ /* 左侧独有的 commit 列表 */ ],
  "right_only": [ /* 右侧独有的 commit 列表 */ ]
}
```

每个 commit 条目的字段：

| 字段 | 类型 | 说明 |
|------|------|------|
| `snapshot_id` | long | 快照 ID |
| `schema_id` | long | Schema 版本 |
| `commit_kind` | string | `APPEND` / `COMPACT` / `OVERWRITE` / `ANALYZE` |
| `commit_user` | string | 提交者 |
| `commit_uuid` | string | 快照的唯一标识（UUID v4），跨 rollback 稳定，可用于精确比对 |
| `time_millis` | long | 提交时间戳（毫秒） |
| `total_record_count` | long | 表当前总记录数 |
| `delta_record_count` | long | 本次提交的增量记录数 |

---

## 4. 各场景返回示例

### 4.1 功能分支有新提交，主分支无变化

场景：从 main（snapshot 1）fork 出 feature 分支，feature 写入 2 条新数据，main 无新写入。

```
main:     s1
              \
feature:       s1 → s2 → s3
```

请求：`GET .../diff?left=feature&right=main`

```json
{
  "left_ref": "feature",
  "right_ref": "main",
  "merge_base": {
    "branch": "main",
    "snapshot_id": 1
  },
  "left_only": [
    {
      "snapshot_id": 2,
      "schema_id": 0,
      "commit_kind": "APPEND",
      "commit_user": "user-a",
      "commit_uuid": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
      "time_millis": 1713091200000,
      "total_record_count": 2,
      "delta_record_count": 1
    },
    {
      "snapshot_id": 3,
      "schema_id": 0,
      "commit_kind": "APPEND",
      "commit_user": "user-a",
      "commit_uuid": "b2c3d4e5-f6a7-8901-bcde-f12345678901",
      "time_millis": 1713091260000,
      "total_record_count": 3,
      "delta_record_count": 1
    }
  ],
  "right_only": []
}
```

### 4.2 两个分支都有新提交

场景：从 main（snapshot 1）fork 出 feature，之后双方各自写入。

```
main:     s1 → s2
              \
feature:       s1 → s2 → s3
```

请求：`GET .../diff?left=feature&right=main`

```json
{
  "left_ref": "feature",
  "right_ref": "main",
  "merge_base": {
    "branch": "main",
    "snapshot_id": 1
  },
  "left_only": [
    {
      "snapshot_id": 2,
      "schema_id": 0,
      "commit_kind": "APPEND",
      "commit_user": "user-a",
      "commit_uuid": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
      "time_millis": 1713091200000,
      "total_record_count": 2,
      "delta_record_count": 1
    },
    {
      "snapshot_id": 3,
      "schema_id": 0,
      "commit_kind": "APPEND",
      "commit_user": "user-a",
      "commit_uuid": "b2c3d4e5-f6a7-8901-bcde-f12345678901",
      "time_millis": 1713091260000,
      "total_record_count": 3,
      "delta_record_count": 1
    }
  ],
  "right_only": [
    {
      "snapshot_id": 2,
      "schema_id": 0,
      "commit_kind": "APPEND",
      "commit_user": "user-b",
      "commit_uuid": "c3d4e5f6-a7b8-9012-cdef-123456789012",
      "time_millis": 1713091230000,
      "total_record_count": 2,
      "delta_record_count": 1
    }
  ]
}
```

> 注意：left_only 和 right_only 中的 `snapshot_id` 是各自分支上独立编号的，两侧出现相同的 ID（如都有 snapshot 2）并不意味着是同一个 snapshot。

### 4.3 相同分支比较

请求：`GET .../diff?left=main&right=main`

```json
{
  "left_ref": "main",
  "right_ref": "main",
  "merge_base": null,
  "left_only": [],
  "right_only": []
}
```

两侧都没有差异，`merge_base` 为 null（短路返回，不做祖先查找）。

### 4.4 空分支（fork 后无写入）

场景：从 main（snapshot 1）fork 出 empty-branch，不做任何写入；main 继续写了 1 条。

```
main:          s1 → s2
                  \
empty-branch:      s1 (无新增)
```

请求：`GET .../diff?left=empty-branch&right=main`

```json
{
  "left_ref": "empty-branch",
  "right_ref": "main",
  "merge_base": {
    "branch": "main",
    "snapshot_id": 1
  },
  "left_only": [],
  "right_only": [
    {
      "snapshot_id": 2,
      "schema_id": 0,
      "commit_kind": "APPEND",
      "commit_user": "user-b",
      "commit_uuid": "d4e5f6a7-b8c9-0123-defa-234567890123",
      "time_millis": 1713091200000,
      "total_record_count": 2,
      "delta_record_count": 1
    }
  ]
}
```

### 4.5 兄弟分支比较

场景：从 main（snapshot 1）同时 fork 出 branch-a 和 branch-b，各自独立写入。

```
                branch-a: s1 → s2
              /
main:     s1
              \
                branch-b: s1 → s2 → s3
```

请求：`GET .../diff?left=branch-a&right=branch-b`

```json
{
  "left_ref": "branch-a",
  "right_ref": "branch-b",
  "merge_base": {
    "branch": "main",
    "snapshot_id": 1
  },
  "left_only": [
    {
      "snapshot_id": 2,
      "schema_id": 0,
      "commit_kind": "APPEND",
      "commit_user": "user-a",
      "commit_uuid": "e5f6a7b8-c9d0-1234-efab-345678901234",
      "time_millis": 1713091200000,
      "total_record_count": 2,
      "delta_record_count": 1
    }
  ],
  "right_only": [
    {
      "snapshot_id": 2,
      "schema_id": 0,
      "commit_kind": "APPEND",
      "commit_user": "user-b",
      "commit_uuid": "f6a7b8c9-d0e1-2345-fabc-456789012345",
      "time_millis": 1713091230000,
      "total_record_count": 2,
      "delta_record_count": 1
    },
    {
      "snapshot_id": 3,
      "schema_id": 0,
      "commit_kind": "APPEND",
      "commit_user": "user-b",
      "commit_uuid": "a7b8c9d0-e1f2-3456-abcd-567890123456",
      "time_millis": 1713091260000,
      "total_record_count": 3,
      "delta_record_count": 1
    }
  ]
}
```

公共祖先在 main 上，即两个 fork 的交汇点。

### 4.6 深链分支（a → b → main）

场景：main → branch-a → branch-b，形成两级 fork 链。branch-b 上有新写入。

```
main:       s1 → s2
                \
branch-a:        s1 → s2
                          \
branch-b:                  s1 → s2
```

请求：`GET .../diff?left=branch-b&right=main`

```json
{
  "left_ref": "branch-b",
  "right_ref": "main",
  "merge_base": {
    "branch": "main",
    "snapshot_id": 1
  },
  "left_only": [
    {
      "snapshot_id": 2,
      "schema_id": 0,
      "commit_kind": "APPEND",
      "commit_user": "user-a",
      "commit_uuid": "11111111-aaaa-bbbb-cccc-111111111111",
      "time_millis": 1713091200000,
      "total_record_count": 2,
      "delta_record_count": 1
    },
    {
      "snapshot_id": 2,
      "schema_id": 0,
      "commit_kind": "APPEND",
      "commit_user": "user-b",
      "commit_uuid": "22222222-aaaa-bbbb-cccc-222222222222",
      "time_millis": 1713091260000,
      "total_record_count": 3,
      "delta_record_count": 1
    }
  ],
  "right_only": [
    {
      "snapshot_id": 2,
      "schema_id": 0,
      "commit_kind": "APPEND",
      "commit_user": "user-main",
      "commit_uuid": "33333333-aaaa-bbbb-cccc-333333333333",
      "time_millis": 1713091230000,
      "total_record_count": 2,
      "delta_record_count": 1
    }
  ]
}
```

left_only 包含整个 fork 链上的提交（branch-a 上的 + branch-b 上的），因为这些都是 main 所没有的。

---

## 5. 错误响应

### 5.1 缺少参数

请求：`GET .../diff?left=feature`（缺少 `right`）

```json
{
  "code": 400,
  "message": "Both 'left' and 'right' query parameters are required for diff"
}
```

### 5.2 分支不存在

请求中引用的分支不存在。

```json
{
  "code": 404,
  "resource_type": "BRANCH",
  "resource_name": "non-existent-branch",
  "message": "Branch 'non-existent-branch' does not exist in table 'db.my_table'"
}
```

### 5.3 无公共祖先

两个分支没有任何 fork 关系（历史断裂或在 merge 功能引入前创建的老分支）。

```json
{
  "code": 500,
  "message": "No common ancestor found between 'branch-x' and 'branch-y'. This may indicate disconnected branch histories or branches created before merge support was enabled."
}
```

### 5.4 表类型不支持

对非 FileStoreTable 类型的表调用 diff。

```json
{
  "code": 501,
  "message": "Diff is only supported for FileStoreTable, got: SystemTable"
}
```

---

## 6. 对称性

Diff 结果具有对称性：调换 left 和 right 后，`left_only` 和 `right_only` 的内容互换。

```
diff(A, B).left_only  == diff(B, A).right_only
diff(A, B).right_only == diff(B, A).left_only
```

---

## 7. 注意事项

1. **snapshot_id 是分支内编号**：不同分支上的 snapshot_id 独立编号，不具有跨分支可比性。用 `commit_uuid` 可唯一标识一个 snapshot。
2. **commit_uuid 跨 rollback 稳定**：snapshot_id 在 rollback 后可能被复用，但 commit_uuid（UUID v4）不会。需要精确比对时应使用 uuid 而非 id。
2. **过期的 snapshot 会被跳过**：如果某些老 snapshot 已经被清理，不会报错，只是不出现在结果中。
3. **COMPACT 类型的 snapshot 也会出现**：diff 展示的是完整的 snapshot 列表，包括 compaction 产生的 snapshot。
4. **merge_base 的含义**：`merge_base.branch` 是公共祖先所在的分支，`merge_base.snapshot_id` 是两个分支的分叉点。两侧独有的 commit 都是在这个分叉点之后产生的。
