# Plan: HDFS File-Based Distributed Lock for Accelerate Index Build

## Context

当前 `BuildAccelerateIndexProcedure` 没有任何并发控制。两个并发 `CALL sys.build_accelerate_index(table=>'db.t', column=>'captions', algorithm=>'lucene')` 会同时跑，导致：
1. 浪费计算资源（重复构建相同索引）
2. `AccelerateIndexMetaIO.casUpdate()` 的软 CAS 有 TOCTOU 竞态，可能丢失 meta 更新

目标：仅依赖 Spark + HDFS，在 procedure 层保证同一 table + column + algorithm 同时只有一个 build 任务在跑。

## 方案概述

从 `FileBasedBranchLock` 提取通用的 `FileBasedLock`，在 `BuildAccelerateIndexProcedure.call()` 中 acquire/release，用心跳线程续租避免长时间 build 被误判 stale。

```
CALL sys.build_accelerate_index(...)
  │
  ├─ acquire: {tablePath}/.accelerate_index_build_{column}_{algorithm}.lock
  ├─ start heartbeat (每 3 分钟续租)
  │
  ├─ resolveContext → distributedBuild → Spark tasks
  │
  └─ finally: stop heartbeat → release lock
```

## Step 1: 新建 `FileBasedLock`

**文件**: `paimon-core/src/main/java/org/apache/paimon/catalog/FileBasedLock.java`

从 `FileBasedBranchLock` 提取通用锁逻辑，API 接受任意 `Path`：

- `acquire(Path lockFilePath) -> Path` — 阻塞获取锁，指数退避重试，stale 检测
- `release(Path lockFilePath)` — 删除锁文件
- `renew(Path lockFilePath)` — **新增**：重写锁文件时间戳续租，校验 ownerId 防止续租已被其他进程抢走的锁
- 构造参数：`FileIO, acquireTimeout, checkMaxSleep, lockTtl`（与 FileBasedBranchLock 相同）
- 内部：`tryCleanStaleLock`, `lockContent`, `parseTimestamp`, `parseOwnerId` — 逻辑从 FileBasedBranchLock 搬过来

`renew` 实现要点：
```java
public void renew(Path lockFilePath) throws IOException {
    String content = fileIO.readFileUtf8(lockFilePath);
    String fileOwner = parseOwnerId(content);
    if (!ownerId.equals(fileOwner)) {
        throw new IOException("Lock owner mismatch: expected " + ownerId + " but found " + fileOwner);
    }
    fileIO.overwriteFileUtf8(lockFilePath, lockContent());
}
```

## Step 2: 重构 `FileBasedBranchLock` 为委托

**文件**: `paimon-core/src/main/java/org/apache/paimon/catalog/FileBasedBranchLock.java`

改为薄包装，保持现有 public API 不变：

```java
public class FileBasedBranchLock {
    private static final String LOCK_FILE_NAME = ".branch_lock";
    private final FileBasedLock delegate;

    public FileBasedBranchLock(FileIO fileIO, Duration acquireTimeout,
                               Duration checkMaxSleep, Duration lockTtl) {
        this.delegate = new FileBasedLock(fileIO, acquireTimeout, checkMaxSleep, lockTtl);
    }

    static Path lockPath(Path tablePath, String branch) { /* unchanged */ }

    public Path acquire(Path tablePath, String branch) throws IOException {
        return delegate.acquire(lockPath(tablePath, branch));
    }

    public void release(Path lockFilePath) {
        delegate.release(lockFilePath);
    }

    public Duration getLockTtl() { return delegate.getLockTtl(); }
}
```

`RESTFileSystemCatalog` 零改动。

## Step 3: 在 `AccelerateIndexConstants` 添加锁常量

**文件**: `paimon-core/src/main/java/org/apache/paimon/accelerateindex/AccelerateIndexConstants.java`

```java
public static final String BUILD_LOCK_FILE_PATTERN = ".accelerate_index_build_%s_%s.lock";
public static final Duration BUILD_LOCK_TTL = Duration.ofMinutes(10);
public static final Duration BUILD_LOCK_ACQUIRE_TIMEOUT = Duration.ofMinutes(8);
public static final Duration BUILD_LOCK_CHECK_MAX_SLEEP = Duration.ofSeconds(8);
public static final Duration BUILD_LOCK_HEARTBEAT_INTERVAL = Duration.ofMinutes(3);

public static String buildLockFileName(String column, String algorithm) {
    String safeColumn = column.replaceAll("[^a-zA-Z0-9_]", "_");
    return String.format(BUILD_LOCK_FILE_PATTERN, safeColumn, algorithm);
}
```

列名做 sanitize 防止特殊字符导致 HDFS 路径非法。锁文件路径：`{tablePath}/.accelerate_index_build_{column}_{algorithm}.lock`

## Step 4: 修改 `BuildAccelerateIndexProcedure` 集成锁

**文件**: `paimon-spark/paimon-spark-common/src/main/java/org/apache/paimon/spark/procedure/BuildAccelerateIndexProcedure.java`

在 `call()` 方法的 `modifyPaimonTable` lambda 内，wrap 整个 build 流程：

```java
FileIO fileIO = fileStoreTable.fileIO();
Path tablePath = fileStoreTable.location();
Path lockFilePath = new Path(tablePath,
    AccelerateIndexConstants.buildLockFileName(column, algorithm));

FileBasedLock buildLock = new FileBasedLock(fileIO,
    AccelerateIndexConstants.BUILD_LOCK_ACQUIRE_TIMEOUT,
    AccelerateIndexConstants.BUILD_LOCK_CHECK_MAX_SLEEP,
    AccelerateIndexConstants.BUILD_LOCK_TTL);

Path acquiredPath = buildLock.acquire(lockFilePath);
ScheduledExecutorService heartbeat = startHeartbeat(buildLock, acquiredPath);
try {
    // existing resolveContext + distributedBuild logic
} finally {
    stopHeartbeat(heartbeat);
    buildLock.release(acquiredPath);
}
```

新增两个 private 方法：
- `startHeartbeat(FileBasedLock, Path)` — 创建单线程 `ScheduledExecutorService`（daemon），每 `BUILD_LOCK_HEARTBEAT_INTERVAL` 调用 `lock.renew(path)`
- `stopHeartbeat(ScheduledExecutorService)` — `shutdownNow` + `awaitTermination(5s)`

新增 `Logger` 字段和必要 imports。

## Step 5: 测试

### 5a: `FileBasedLockTest`（新建）

**文件**: `paimon-core/src/test/java/org/apache/paimon/catalog/FileBasedLockTest.java`

| 测试 | 验证 |
|------|------|
| `testAcquireAndRelease` | 获取锁 → 锁文件存在 → 释放 → 锁文件消失 |
| `testConcurrentAcquireBlocks` | 两个 lock 实例，第二个超时失败 |
| `testStaleLockDetection` | 手动写入旧时间戳的锁文件 → 新 lock 实例检测到 stale → 成功获取 |
| `testRenewUpdatesTimestamp` | 获取 → sleep → renew → 读取锁文件验证时间戳已更新 |
| `testRenewFailsIfOwnerChanged` | 获取 → 手动改写 ownerId → renew 抛异常 |
| `testReleaseAndReacquire` | 获取 → 释放 → 再次获取成功 |

### 5b: 确认 `FileBasedBranchLock` 委托不回归

运行现有 `RESTFileSystemCatalog` 相关测试：
```bash
mvn test -pl paimon-core -Dtest="MockRESTCatalogTest" -DfailIfNoTests=false
```

### 5c: 编译验证 Spark 模块

```bash
mvn compile -pl paimon-spark/paimon-spark-common -am -DskipTests
```

## 执行顺序

```
Step 1 (FileBasedLock) ─┬─→ Step 2 (重构 FileBasedBranchLock)
                        │
                        └─→ Step 5a (FileBasedLock 测试)
                        
Step 3 (Constants) ─────→ Step 4 (Procedure 集成)

Step 2 完成后 ─────────→ Step 5b (回归测试)
Step 4 完成后 ─────────→ Step 5c (编译验证)
```

## 安全分析

| 场景 | 保障 |
|------|------|
| 两个 Spark job 同时 build 同一 table+column | 第二个在 acquire 阶段阻塞，超时报错 |
| Build 运行超过 10 分钟 | 心跳线程每 3 分钟续租，锁不会被误判 stale |
| Driver 崩溃 / kill -9 | 心跳停止 → 锁文件 10 分钟后过期被自动清理 |
| Reconciler 同时修改 meta | CAS 作为第二层防护仍保留 |
| 列名含特殊字符 | `buildLockFileName` 做 sanitize |

## 涉及文件清单

| 文件 | 操作 |
|------|------|
| `paimon-core/.../catalog/FileBasedLock.java` | **新建** |
| `paimon-core/.../catalog/FileBasedBranchLock.java` | 重构为委托 |
| `paimon-core/.../accelerateindex/AccelerateIndexConstants.java` | 添加锁常量 |
| `paimon-spark/.../procedure/BuildAccelerateIndexProcedure.java` | 集成锁 + 心跳 |
| `paimon-core/src/test/.../catalog/FileBasedLockTest.java` | **新建** |
