# Paimon REST Catalog Server 部署文档

## 打包

```bash
cd paimon/
mvn clean package -DskipTests -pl paimon-rest-server -am
```

产出物在 `paimon-rest-server/target/` 下：

```
paimon-rest-server-1.4-SNAPSHOT-dist.tar.gz
```

## 压缩包结构

解压后的目录结构：

```
paimon-rest-server-1.4-SNAPSHOT/
├── bin/
│   ├── start.sh                    # 启动脚本
│   └── stop.sh                     # 停止脚本
├── conf/
│   ├── log4j2.xml                  # 日志配置 (按天滚动, 保留30天)
│   └── server.properties           # 服务配置模板
├── lib/
│   ├── paimon-rest-server-1.4-SNAPSHOT.jar
│   ├── paimon-api-1.4-SNAPSHOT.jar
│   ├── paimon-common-1.4-SNAPSHOT.jar
│   ├── paimon-core-1.4-SNAPSHOT.jar
│   ├── paimon-format-1.4-SNAPSHOT.jar
│   ├── paimon-shade-netty-4-4.1.100.Final-0.8.0.jar
│   ├── paimon-shade-jackson-2-2.14.2-0.8.0.jar
│   ├── paimon-shade-guava-30-30.1.1-jre-0.8.0.jar
│   ├── slf4j-api-1.7.32.jar
│   ├── log4j-api-2.25.3.jar
│   ├── log4j-core-2.25.3.jar
│   ├── log4j-slf4j-impl-2.25.3.jar
│   ├── HikariCP-4.0.3.jar
│   └── ...                         # 所有 runtime 传递依赖
└── sql/
    └── init.sql                    # MySQL 建表 DDL
```

所有 runtime 依赖（含传递依赖）由 Maven 自动收集到 `lib/`，无需手动管理。

## 部署步骤

### 1. 解压

```bash
tar -xzf paimon-rest-server-1.4-SNAPSHOT-dist.tar.gz -C /opt/
cd /opt/paimon-rest-server-1.4-SNAPSHOT
```

### 2. 修改配置

```bash
vi conf/server.properties
```

#### 最小配置

```properties
warehouse=/data/paimon/warehouse
metastore=filesystem
rest-server.port=8080
rest-server.prefix=paimon
```

#### 生产配置 (HDFS + MySQL)

```properties
warehouse=hdfs:///user/paimon/warehouse
metastore=filesystem
rest-server.host=0.0.0.0
rest-server.port=8080
rest-server.prefix=paimon
rest-server.io-threads=4
rest-server.worker-threads=32

# 方式一: KsDataSource (快手内部)
rest-server.metadata.resource-id=paimon_catalog

# 方式二: HikariCP 直连
# rest-server.metadata.jdbc-url=jdbc:mysql://host:3306/paimon_catalog?useSSL=false&characterEncoding=utf8mb4
# rest-server.metadata.jdbc-user=paimon
# rest-server.metadata.jdbc-password=paimon123
```

### 3. 初始化 MySQL (启用元数据存储时)

```bash
mysql -h <host> -u root -p < sql/init.sql
```

创建 `paimon_catalog` 库及 `paimon_database`、`paimon_table`、`paimon_op_log` 三张表。

### 4. HDFS 配置 (使用 HDFS 时)

```bash
export HADOOP_CONF_DIR=/etc/hadoop/conf
```

脚本会自动将该目录加入 classpath。

### 5. 如使用 KsDataSource

将 `infra-framework-datasource-1.0.666.jar` 放入 `lib/` 目录（该依赖为 provided scope，打包时不包含）。

### 6. 启动

```bash
bin/start.sh                        # 后台启动
DAEMON_MODE=false bin/start.sh      # 前台启动 (调试)
```

### 7. 停止

```bash
bin/stop.sh
```

## 启动原理

`start.sh` 通过 `java -cp` 方式启动，classpath 构成：

```
conf/            ← log4j2.xml 等配置文件
lib/*            ← 所有依赖 JAR
$HADOOP_CONF_DIR ← Hadoop 配置 (可选)
$EXTRA_CLASSPATH ← 用户自定义 (可选)
```

主类：`org.apache.paimon.rest.server.RESTCatalogServer`

## JVM 调优

通过环境变量控制：

```bash
export JAVA_OPTS="-Xms1g -Xmx4g"
export GC_OPTS="-XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:+ParallelRefProcEnabled"
export JMX_PORT=9999              # 开启 JMX 远程监控
export EXTRA_CLASSPATH="/opt/plugins/custom.jar"
bin/start.sh
```

## 日志

日志输出到解压目录下的 `logs/`：

| 文件 | 内容 | 保留策略 |
|------|------|----------|
| `paimon-rest-server.log` | 应用主日志 | 按天滚动, 单文件 2GB, 保留 30 天 |
| `paimon-rest-server-error.log` | WARN 及以上 | 按天滚动, 单文件 1GB, 保留 30 天 |
| `stdout.log` | 后台模式的 stdout/stderr | 追加写入 |

日志配置 `conf/log4j2.xml` 支持热加载（60 秒检测一次），修改后无需重启。

## 健康检查

```bash
curl http://localhost:8080/v1/config
```

HTTP 200 表示服务正常。

## 配置参数一览

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `warehouse` | (必填) | 仓库路径 |
| `metastore` | (必填) | `filesystem` / `hive` / `jdbc` |
| `rest-server.host` | `0.0.0.0` | 监听地址 |
| `rest-server.port` | `8080` | 监听端口 |
| `rest-server.prefix` | — | API 前缀 |
| `rest-server.io-threads` | `4` | Netty IO 线程 |
| `rest-server.worker-threads` | `16` | 业务线程 |
| `rest-server.max-content-length` | `10485760` | 最大请求体 (字节) |
| `rest-server.metadata.resource-id` | — | KsDataSource 资源 ID (优先) |
| `rest-server.metadata.jdbc-url` | — | JDBC URL (HikariCP) |
| `rest-server.metadata.jdbc-user` | — | JDBC 用户名 |
| `rest-server.metadata.jdbc-password` | — | JDBC 密码 |
| `rest-server.metadata.pool.max-size` | `10` | 最大连接数 |
| `rest-server.metadata.pool.min-idle` | `2` | 最小空闲连接 |
| `rest-server.metadata.pool.connection-timeout-ms` | `30000` | 连接超时 (ms) |
