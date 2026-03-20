# Paimon Frontend 使用指南

Paimon Frontend 是 Apache Paimon 数据湖的 Web 管理界面，提供对 Catalog、数据库、表的可视化浏览和管理能力。通过与 REST Catalog Server 联动，用户可以直观地查看表结构、快照历史、分支拓扑等信息。

---

## 功能特性

### 多 Catalog 管理

支持同时管理多个 REST Catalog Server，通过界面快速添加、切换、删除 Catalog 连接。

### 数据库与表浏览

- 左侧导航栏展示数据库和表的树形结构
- 数据库详情页以卡片和列表形式展示库内所有表

### 表详情 — 9 个功能 Tab

| Tab | 说明 | 分支感知 |
|-----|------|----------|
| **Schema** | 字段信息（ID、名称、类型、描述） | 是 |
| **Options** | 表配置项（键值对） | 是 |
| **Snapshots** | 快照列表，含提交类型、记录数等（分页） | 是 |
| **Branches** | 分支列表及其创建快照信息 | — |
| **Graph** | Git 风格的分支拓扑图（见下方说明） | — |
| **Tags** | 标签列表及关联快照详情 | 是 |
| **Partitions** | 分区列表及文件统计（分页） | 是 |
| **Schema History** | Schema 版本变更历史（可折叠） | 是 |
| **Consumers** | 消费位点追踪 | 是 |

### Git 风格 Branch Graph

以可视化方式展示各分支的快照时间线：

- 每个分支独占一列，分叉点以曲线连接
- **Schema 演进标识**：发生 schema 变更的快照节点带有紫色菱形标记
- **字段级 Diff**：Tooltip 中展示新增（绿）、删除（红）、类型变更（橙）的字段
- **Tag 标注**：快照节点旁显示关联的 Tag 徽章

### 分支感知

表详情页顶部提供分支选择器。切换分支后，Snapshots、Tags、Partitions、Schema History、Consumers 等 Tab 自动展示对应分支的数据。

---

## 快速开始

### 前置条件

- Node.js >= 18
- npm >= 9

### 安装依赖

```bash
cd paimon-frontend
npm install
```

### 开发模式

```bash
npm run dev
```

启动后访问 `http://localhost:5173`。开发服务器会自动将 `/v1/*` 请求代理到 `http://127.0.0.1:8080`（本地 REST Catalog Server）。

### 生产构建

```bash
npm run build
```

构建产物输出到 `dist/` 目录。

---

## 与 REST Catalog Server 联动

### 本地开发

开发模式下，Vite 内置代理会将所有 `/v1/*` 请求转发到 `http://127.0.0.1:8080`，无需额外配置。只需确保本地已启动 REST Catalog Server：

```bash
java -jar paimon-rest-server.jar --warehouse /path/to/warehouse --metastore filesystem
```

然后在前端界面中添加一个 Catalog，`baseUrl` 留空即可连接本地 Server。

### 连接远程 Server

通过 Catalog Manager 配置远程 Server 地址时，前端内置了 `/proxy` 中间件来解决跨域（CORS）问题：

1. 在界面点击添加 Catalog
2. 填入远程 Server 的完整地址（如 `http://remote-server:8080`）
3. 前端自动将 API 请求通过 `/proxy?target=<encoded-url>` 路由，由服务端代理转发

### REST Server 内嵌前端

生产环境推荐的部署方式：REST Catalog Server 将 paimon-frontend 作为运行时依赖打包，直接提供前端页面服务。

- REST Server 自动将非 API 请求（非 `/v1/*`）路由到前端静态资源
- 支持 SPA 路由回退（所有未匹配路径返回 `index.html`）
- 静态资源（JS/CSS）自动配置长期缓存
- 可通过 `rest-server.frontend.enabled` 配置项控制是否启用前端（默认启用）

此模式下无需单独部署前端，访问 REST Server 地址即可同时使用 API 和 Web 界面。

### 典型部署拓扑

```
┌─────────────────────────────────────────────┐
│           REST Catalog Server               │
│  ┌──────────┐    ┌───────────────────────┐  │
│  │ Frontend │    │    REST API (/v1/*)   │  │
│  │ (static) │    │                       │  │
│  └──────────┘    └───────────────────────┘  │
│       :8080                                 │
└─────────────────────────────────────────────┘
         ↑                    ↑
     浏览器访问            Flink / Spark / Java API
```

或者前端独立部署（开发/调试场景）：

```
┌──────────────┐         ┌──────────────────────┐
│   Frontend   │ ──/v1──→│  REST Catalog Server  │
│  (Vite Dev)  │  proxy  │                       │
│    :5173     │         │        :8080           │
└──────────────┘         └──────────────────────┘
```

---

## Catalog 配置

通过界面右上角的 Catalog 管理器添加和切换 Catalog。每个 Catalog 配置包含：

| 字段 | 说明 | 示例 |
|------|------|------|
| `name` | Catalog 名称（唯一标识，添加后不可修改） | `production` |
| `baseUrl` | REST Server 地址。留空表示连接本地 Server | `http://remote-server:8080` |
| `prefix` | REST Catalog 前缀（添加时自动从 `/v1/config` 检测） | `paimon` |

- Catalog 配置保存在浏览器 localStorage 中
- 切换 Catalog 时自动清除查询缓存，确保数据一致性
- 支持连接测试：添加时自动尝试获取 `/v1/config`，验证连接并自动填充 prefix

---

## 技术栈

| 技术 | 版本 | 用途 |
|------|------|------|
| React | 18.3 | UI 框架 |
| Ant Design | 5.21 | 组件库 |
| TanStack React Query | 5.56 | 数据获取与缓存 |
| React Router | 6.26 | 客户端路由 |
| Axios | 1.7 | HTTP 客户端 |
| Vite | 5.4 | 开发服务器与构建工具 |
| TypeScript | 5.5 | 类型系统 |
