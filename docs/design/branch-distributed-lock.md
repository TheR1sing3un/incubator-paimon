# 分支级分布式锁设计文档

## 1. 背景

REST Catalog Server 是无状态分布式部署。原有的分支锁是 JVM 内的 `ReentrantLock`，只能保护单进程内的并发，多实例部署时锁形同虚设。两个 REST Server 实例可以同时对同一个分支执行 commit 或 merge，虽然底层 CAS 能兜底，但 CAS 失败意味着大量计算和写入白费。

需要一个真正跨进程的分布式分支锁。约束条件：唯一可用的外部存储是 HDFS，不能引入 Redis/ZooKeeper 等新依赖。

---

## 2. 方案概述

**基于 HDFS 原子文件创建的两级分支锁。**

- **第一级：JVM 内存锁**——线程级，在进程内排队，不产生任何 HDFS 调用
- **第二级：HDFS 文件锁**——进程级，通过原子创建锁文件实现跨进程互斥

两级锁的关系：**内存锁在前，文件锁在后**。同进程内多线程只有一个会去碰 HDFS，其余全部在 JVM 内零开销等待。

---

## 3. 锁文件

### 3.1 路径

```
main 分支:     {tablePath}/.branch_lock
其他分支:      {tablePath}/branch/branch-{name}/.branch_lock
```

### 3.2 内容

```
{owner-uuid}|{timestamp-ms}
```

- `owner-uuid`：每个 REST Server 进程启动时生成的唯一标识
- `timestamp-ms`：锁创建时刻的毫秒时间戳，用于过期检测

---

## 4. 工作流程

### 4.1 抢锁

```
线程进入 withBranchLock
    │
    ▼
① waitingCount++（原子计数，记录该分支有多少线程在排队）
    │
    ▼
② executionLock.lock()（JVM ReentrantLock，同进程线程在这里排队）
    │
    ▼
③ 检查是否需要获取 HDFS 锁：
   - hdfsLockPath == null（本进程还没拿到 HDFS 锁）→ 去获取
   - 锁已持有但接近过期（已用时间 > TTL × 80%）→ 释放旧锁，重新获取
   - 锁已持有且未过期 → 直接复用，跳过 HDFS 调用
    │
    ▼
④ 获取 HDFS 锁（仅在需要时）：
   fileIO.tryToWriteAtomic(lockPath, content)
   - 文件不存在 → 创建成功 → 拿到锁
   - 文件已存在 → 读取内容，检查 timestamp：
     - age > TTL → 过期锁，强制删除后重试
     - age ≤ TTL → 活锁，sleep 退避后重试
       （退避策略：100ms → 200ms → 400ms → ... → 最大 8s）
     - 总超时 8 分钟 → 抛异常
    │
    ▼
⑤ 执行业务逻辑（commit / merge / rollback）
```

### 4.2 释放锁

```
业务执行完毕（或异常）
    │
    ▼
① waitingCount--（原子递减）
    │
    ├─ 结果 > 0：还有线程排队 → 不释放 HDFS 锁，留给下一个线程复用
    │
    └─ 结果 == 0：没有线程排队了 → 删除 HDFS 锁文件
    │
    ▼
② executionLock.unlock()（释放 JVM 锁，下一个线程进入）
```

### 4.3 锁继承

核心优化：**HDFS 锁是进程级的，不跟线程走。** 

同一个分支有 10 个线程排队时：
- 只有第 1 个线程创建 HDFS 锁文件（1 次 HDFS 写）
- 第 2~9 个线程直接复用，零 HDFS 开销
- 第 10 个线程（最后一个）删除锁文件（1 次 HDFS 删）

整个过程只有 2 次 HDFS 操作，而不是 20 次。

### 4.4 安全边界

HDFS 锁文件的 timestamp 在创建时写入，之后不更新。如果排队时间很长，其他进程可能认为锁过期并强制删除。

解决方案：**主动放弃快过期的锁。** 每个线程执行前检查锁的已用时间，如果超过 TTL 的 80%（默认 8 分钟），先释放旧锁再重新获取。这样：
- 锁永远不会被其他进程误删（自己在过期前就放弃了）
- 不需要后台续期线程，不额外增加 HDFS 调用
- 80% 阈值留出 2 分钟缓冲，足够下一个操作完成

---

## 5. 过期锁清理

当进程崩溃时，锁文件会残留在 HDFS 上。其他进程通过 TTL 机制清理：

1. 尝试创建锁文件失败
2. 读取锁文件内容，解析 timestamp
3. `当前时间 - timestamp > TTL` → 判定过期，强制删除
4. 解析失败 → 返回当前时间（即认为锁是新鲜的），避免误删有效锁

TTL 默认 10 分钟。进程崩溃的恢复代价是等待一个 TTL 周期。

---

## 6. 并发场景分析

| 场景 | 行为 |
|------|------|
| 同进程 10 线程 commit 同一分支 | JVM 锁排队，共享 1 把 HDFS 锁 |
| 进程 A 和进程 B 同时 merge 到 main | 一个拿到 HDFS 锁执行，另一个轮询等待 |
| 进程 A commit main + 进程 B merge 到 main | 串行，target 分支相同 |
| 进程 A commit main + 进程 B commit branchA | 并行，锁文件路径不同 |
| 进程 A 崩溃持有锁 | 进程 B 等待 TTL 过期后强制删除 |

---

## 7. 配置项

| 配置 | 默认值 | 说明 |
|------|--------|------|
| `lock.ttl` | 10 分钟 | 锁文件过期时间，进程崩溃后其他进程的最大等待时间 |
| `lock-acquire-timeout` | 8 分钟 | 获取锁的总超时时间 |
| `lock-check-max-sleep` | 8 秒 | 轮询等待锁时的最大 sleep 间隔 |

---

## 8. CAS 兜底

分布式锁之下，每个 snapshot 文件仍通过 `tryToWriteAtomic` 提供 CAS 保护，作为最后一道防线。双重保障：
- 正常情况：分布式锁保证互斥，CAS 不会失败
- 异常情况（锁实现 bug、HDFS 故障等）：CAS 阻止数据损坏
