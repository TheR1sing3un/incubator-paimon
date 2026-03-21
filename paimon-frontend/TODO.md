# Paimon Frontend 现状分析与优化 TODO

## 1. 技术栈概览

| 类别 | 技术 | 版本 |
|------|------|------|
| 框架 | React | 18.3.1 |
| 语言 | TypeScript | 5.5.4 |
| 构建工具 | Vite | 5.4.3 |
| UI 组件库 | Ant Design (antd) | 5.21.0 |
| 图标 | @ant-design/icons | 5.4.0 |
| HTTP 客户端 | axios | 1.7.7 |
| 服务端状态 | @tanstack/react-query | 5.56.0 |
| 路由 | react-router-dom | 6.26.0 |
| 本地状态 | React Context API + localStorage | — |

**架构特点：**
- SPA 应用，路由结构：`/` → `/databases/:db` → `/databases/:db/tables/:table`
- 多 Catalog 支持：本地 Catalog 直连 `/v1/...`，远程 Catalog 通过 `/proxy?target=<baseUrl>/v1/...` 代理
- 代码拆分：vendor（React 栈）和 antd 分离打包
- 分页采用 token-based 方式（自定义 `usePagedData` hook）

---

## 2. 当前能力矩阵

### 2.1 前端已实现的功能（全部为只读 GET 操作）

| 实体 | 已实现操作 | 对应前端文件 |
|------|-----------|-------------|
| Database | 列表、详情 | `api/databases.ts` |
| Table | 列表、详情（含分支切换） | `api/tables.ts` |
| Schema | 历史列表 | `api/schemas.ts` |
| Snapshot | 分页列表 | `api/snapshots.ts` |
| Branch | 列表、详情（含 main 分支） | `api/branches.ts` |
| Tag | 列表、详情 | `api/tags.ts` |
| Partition | 列表 | `api/partitions.ts` |
| Consumer | 列表 | `api/consumers.ts` |

### 2.2 REST Server 已有但前端未覆盖的 API

下表列出所有后端支持但前端**尚未调用**的 API 端点：

#### Database 写操作
| HTTP 方法 | 端点 | 操作 | Handler |
|-----------|------|------|---------|
| POST | `/databases` | 创建数据库 | DatabaseHandler |
| DELETE | `/databases/{database}` | 删除数据库 | DatabaseHandler |
| POST | `/databases/{database}` | 修改数据库属性 | DatabaseHandler |

#### Table 写操作
| HTTP 方法 | 端点 | 操作 | Handler |
|-----------|------|------|---------|
| POST | `/databases/{database}/tables` | 创建表 | TableHandler |
| DELETE | `/databases/{database}/tables/{table}` | 删除表 | TableHandler |
| POST | `/databases/{database}/tables/{table}` | 修改表 Schema | TableHandler |
| POST | `/tables/rename` | 重命名表 | TableHandler |
| POST | `/databases/{database}/register` | 注册外部表 | TableHandler |
| GET | `/tables` | 全局表列表（跨库） | TableHandler |
| GET | `/tables/id/{tableId}` | 按 ID 查表 | TableHandler |
| GET | `/databases/{database}/table-details` | 表详情列表 | TableHandler |

#### View（完全未覆盖）
| HTTP 方法 | 端点 | 操作 | Handler |
|-----------|------|------|---------|
| GET | `/databases/{database}/views` | 列出视图 | ViewHandler |
| GET | `/databases/{database}/views/{view}` | 视图详情 | ViewHandler |
| POST | `/databases/{database}/views` | 创建视图 | ViewHandler |
| POST | `/databases/{database}/views/{view}` | 修改视图 | ViewHandler |
| DELETE | `/databases/{database}/views/{view}` | 删除视图 | ViewHandler |
| POST | `/views/rename` | 重命名视图 | ViewHandler |
| GET | `/views` | 全局视图列表 | ViewHandler |
| GET | `/databases/{database}/view-details` | 视图详情列表 | ViewHandler |

#### Function（完全未覆盖）
| HTTP 方法 | 端点 | 操作 | Handler |
|-----------|------|------|---------|
| GET | `/databases/{database}/functions` | 列出函数 | FunctionHandler |
| GET | `/databases/{database}/functions/{function}` | 函数详情 | FunctionHandler |
| POST | `/databases/{database}/functions` | 创建函数 | FunctionHandler |
| POST | `/databases/{database}/functions/{function}` | 修改函数 | FunctionHandler |
| DELETE | `/databases/{database}/functions/{function}` | 删除函数 | FunctionHandler |
| GET | `/functions` | 全局函数列表 | FunctionHandler |
| GET | `/databases/{database}/function-details` | 函数详情列表 | FunctionHandler |

#### Branch 写操作
| HTTP 方法 | 端点 | 操作 | Handler |
|-----------|------|------|---------|
| POST | `/databases/{database}/tables/{table}/branches` | 创建分支 | BranchHandler |
| DELETE | `/databases/{database}/tables/{table}/branches/{branch}` | 删除分支 | BranchHandler |
| POST | `/.../branches/{branch}/forward` | 分支 fast-forward | BranchHandler |
| POST | `/.../branches/{branch}/merge` | 分支合并（Phase 2，未实现） | BranchHandler |
| GET | `/.../tables/{table}/diff` | 分支 diff（Phase 2，未实现） | BranchHandler |

#### Tag 写操作
| HTTP 方法 | 端点 | 操作 | Handler |
|-----------|------|------|---------|
| POST | `/databases/{database}/tables/{table}/tags` | 创建标签 | TagHandler |
| DELETE | `/databases/{database}/tables/{table}/tags/{tag}` | 删除标签 | TagHandler |

#### Snapshot 高级操作
| HTTP 方法 | 端点 | 操作 | Handler |
|-----------|------|------|---------|
| GET | `/.../tables/{table}/snapshot` | 获取最新快照 | SnapshotHandler |
| GET | `/.../tables/{table}/snapshots/{version}` | 按版本获取快照 | SnapshotHandler |
| POST | `/.../tables/{table}/commit` | 提交快照 | SnapshotHandler |
| POST | `/.../tables/{table}/rollback` | 快照回滚 | SnapshotHandler |

#### Partition 高级操作
| HTTP 方法 | 端点 | 操作 | Handler |
|-----------|------|------|---------|
| POST | `/.../tables/{table}/partitions/mark` | 标记分区完成 | PartitionHandler |
| POST | `/.../tables/{table}/partitions/list-by-names` | 按名称查询分区 | PartitionHandler |

#### Consumer 写操作
| HTTP 方法 | 端点 | 操作 | Handler |
|-----------|------|------|---------|
| POST | `/.../tables/{table}/consumers/reset` | 重置消费者偏移 | ConsumerHandler |

#### Token & Auth
| HTTP 方法 | 端点 | 操作 | Handler |
|-----------|------|------|---------|
| GET | `/.../tables/{table}/token` | 获取表访问令牌 | TableTokenHandler |
| POST | `/.../tables/{table}/auth` | 表查询认证 | TableTokenHandler |

---

## 3. 未来优化 TODO

### P0: 写操作支持（核心 CRUD 完善）

- [ ] **Database 管理**
  - [ ] 创建数据库（POST `/databases`）
  - [ ] 删除数据库（DELETE `/databases/{database}`，需二次确认）
  - [ ] 修改数据库属性（POST `/databases/{database}`）

- [ ] **Table 管理**
  - [ ] 创建表（POST，需支持 Schema 定义、分区键、主键、表属性配置）
  - [ ] 删除表（DELETE，需二次确认）
  - [ ] 修改表 Schema（POST，字段增删改、属性修改）
  - [ ] 重命名表（POST `/tables/rename`）

- [ ] **Branch 管理**
  - [ ] 创建分支（POST，指定源快照）
  - [ ] 删除分支（DELETE，需二次确认）
  - [ ] 分支 fast-forward（POST `/.../forward`）

- [ ] **Tag 管理**
  - [ ] 创建标签（POST，关联快照）
  - [ ] 删除标签（DELETE，需二次确认）

### P1: 缺失实体支持

- [ ] **View 管理**
  - [ ] View 列表页面（嵌入 Database 详情页，类似 Tables tab）
  - [ ] View 详情页面
  - [ ] 创建 / 修改 / 删除 View

- [ ] **Function 管理**
  - [ ] Function 列表页面
  - [ ] Function 详情页面
  - [ ] 创建 / 修改 / 删除 Function

- [ ] **全局搜索视图**
  - [ ] 跨库表搜索（GET `/tables`，支持 pattern 过滤）
  - [ ] 跨库视图搜索（GET `/views`）
  - [ ] 跨库函数搜索（GET `/functions`）

### P2: 高级功能

- [ ] **Snapshot 高级操作**
  - [ ] 查看指定版本快照详情（GET `/.../snapshots/{version}`）
  - [ ] 快照回滚（POST `/.../rollback`，需二次确认 + 选择目标版本）
  - [ ] Commit 管理界面（POST `/.../commit`，面向高级用户）

- [ ] **Partition 高级操作**
  - [ ] 标记分区完成（POST `/.../partitions/mark`）
  - [ ] 按名称批量查询分区

- [ ] **Consumer 管理**
  - [ ] 重置消费者偏移量（POST `/.../consumers/reset`，选择目标 Snapshot ID）

- [ ] **数据预览**
  - [ ] 表数据采样预览（需后端支持 data read API）
  - [ ] Schema 可视化对比（跨版本 diff）

### P3: 用户体验优化

- [ ] **搜索与过滤**
  - [ ] Database 列表搜索（利用 pattern 参数）
  - [ ] Table / View / Function 列表过滤
  - [ ] Snapshot / Tag / Branch 列表搜索

- [ ] **暗色主题**
  - [ ] 利用 Ant Design 的 `darkAlgorithm` 实现主题切换
  - [ ] 用户偏好持久化到 localStorage

- [ ] **国际化 (i18n)**
  - [ ] 引入 react-i18next 或类似方案
  - [ ] 中英文语言包

- [ ] **响应式优化**
  - [ ] 移动端适配（Sidebar 折叠、表格横向滚动）
  - [ ] 小屏幕下的布局调整

- [ ] **权限与安全**
  - [ ] 集成 Token/Auth API（TableTokenHandler）
  - [ ] 基于权限的 UI 元素控制（写操作按钮显隐）
  - [ ] 登录/认证流程

- [ ] **其他体验改进**
  - [ ] 操作结果通知（成功/失败 toast）
  - [ ] 批量操作支持（批量删除等）
  - [ ] 表属性（Options）编辑界面
  - [ ] 外部表注册向导（POST `/databases/{database}/register`）

---

## 4. 统计摘要

| 指标 | 数值 |
|------|------|
| REST Server 总端点数 | ~54 |
| 前端已覆盖端点数 | ~14 |
| 前端未覆盖端点数 | ~40 |
| API 覆盖率 | ~26% |
| 已覆盖 HTTP 方法 | GET |
| 未覆盖 HTTP 方法 | POST, DELETE |
| 完全未覆盖的实体 | View, Function, Token/Auth |
