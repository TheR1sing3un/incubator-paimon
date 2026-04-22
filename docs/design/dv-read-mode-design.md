# DV 表 L0 可见性提升 — read-mode 方案设计

## 阅读引导

- **推荐方案**：新增表属性 `deletion-vectors.read-mode`，默认 `performance`（等同现状），允许用户切到 `freshness` 以读到尚未 compaction 的 L0。
- **核心结论**：`freshness` 下 L0 与可能同 PK 的 L1+ 必须进同一个 merge 读取单元、按 sequence 覆盖，**不能**做"L1+ 走快路径 + L0 独立 merge 再 union"的 split 级分流。
- **已排除方案**：split 级分流（正确性错误）、修改写入路径（改动面过大，且与本次"读侧取舍"目标不符）。
- **代价**：含 L0 的 bucket 部分退化到 merge-on-read，并失去 bucket 级整桶裁剪；无 L0 的 bucket 与现状完全一致。

读者如果只关心选择哪个模式，直接看 §2 的对比表和决策建议即可。

## 1. 为什么要做这件事

DV 表走的是 Merge-On-Write：写入时 L0 直接落盘，后续 compaction 在把 L0 合入 L1+ 时顺带对被覆盖的旧行生成 deletion vector。读取时直接按 DV 跳过旧行，省掉 merge 开销，性能接近"裸读 parquet"。

代价是：**新写入的 L0 对读者不可见，必须等 compaction 跑完。**

原因在于 L0 之间允许同 PK（buffer 直接 flush，不做去重），而 DV 是下一个阶段才生成的。如果读的时候把 L0 直接放进来，又沿用"L1+ 裸读 + DV"的快路径，同一个 PK 就会出现多条。

当前实现通过两件事回避这个问题：
- 读取阶段把 L0 一律滤掉（只看 L1+）；
- 默认开启 `lookup-wait`，让写入同步等 compaction，尽量缩短 L0 滞留时间。

这对批读性能是最优解，但对"同步等 compaction"的写入延迟、或者"异步 compaction 下的数据可见性延迟"没有出口。用户需要一个明确的开关去做这个取舍。

## 2. 两种模式与选择建议

| 模式 | L0 可见性 | 读性能 | 适用场景 |
|------|-----------|--------|----------|
| `performance`（默认） | 等 compaction | 最快（纯 DV 跳读） | OLAP 查询、性能敏感、可以接受秒级~分钟级滞后 |
| `freshness` | 立即可见 | 含 L0 的 bucket 部分退化到 merge-on-read | 近实时看板，通常配合异步 compaction |

决策建议：

- 现有 OLAP 场景 → 不动，继续 `performance`。
- 需要近实时可见性 → 切 `freshness`，并把 `lookup-wait` 关掉让 compaction 异步跑；正常情况下 L0 会被快速消化，退化窗口很短。
- 写吞吐高、compaction 资源紧张 → 也建议 `freshness` + 异步 compaction，避免写入被 lookup-wait 拖住。

对流读：`freshness` **只**让"启动时的第一份全量"更新，不改变后续增量仍按 changelog 推进的行为（详见 §5）。

## 3. 为什么不能走"split 级分流"——正确性要害

一个看起来很自然的想法是：L1+ 继续走原来的裸读 + DV 快路径、L0 单独组一个 merge 读取单元，最后 union。这是错的，根因是 **DV 的生成和 L0 的落盘不在同一个 snapshot**：

- L0 由写 buffer 直接 flush，落盘时不做 lookup、不生成任何 DV；
- DV 只在后续 compaction 里，针对 L0 的 key 去 L1+ 里 lookup 时才产生，并且随 compaction 的提交形成**另一个** snapshot；
- `lookup-wait` 只是"尽量把 L0 和它的 DV 挤进同一次提交"，不是强一致保证（compaction 未必挑中这批 L0）；关掉之后更是显式两阶段。

因此一定存在一类中间 snapshot：**L0 已经有 PK=k 的新行、L1+ 里 PK=k 的旧行还在、对应 DV 尚未落盘**。在这种 snapshot 下做 split 级分流：

- L1+ 走快路径 → 输出旧行（DV 还没标它）
- L0 走独立 merge → 输出新行
- Union → 同一 PK 两条，错。

结论：**`freshness` 下，L0 与可能同 PK 的 L1+ 必须进同一个 merge 读取单元，由 sequence 决定胜负。**这不是性能取舍，是正确性要求。

## 4. 为什么 merge-on-read 是安全的

把 L0 与重叠 L1+ 合并读时，同一个 PK 的所有可能状态都能收敛到正确结果：

| 物理状态 | 读取行为 | 结果 |
|----------|----------|------|
| 只有 L1+ 的历史数据 | 直接输出 | 正确 |
| 只有 L0 的新写入 | L0 间按 sequence 去重 | 正确（最新胜出） |
| L0 有新行、L1+ 有旧行、DV 已就绪 | DV 先把 L1+ 旧行预过滤掉，merge 只剩新行 | 正确 |
| L0 有新行、L1+ 有旧行、DV 未就绪 | 两条都进 merge，按 sequence 取胜 | 正确（新行胜出） |

关键认识：**正确性靠 sequence 覆盖保证，不依赖 DV 是否已就绪**。`freshness` 模式里 DV 只承担"预过滤以减少 merge 输入"的性能角色，不再是正确性组件。这也是它能放心开 L0 可见的前提。

对比 `performance` 模式——它的快路径依赖"L1+ 之间同一 PK 只出现一次"这个写路径不变式；把这个假设延伸到 L0 上不成立，所以 `performance` 不能简单把 L0 放行。

## 5. 流读语义：bootstrap freshness

流读分两段：启动时先读一份全量（bootstrap），之后持续消费增量（follow-up）。

`freshness` 只改 **bootstrap**：

- `performance`：初始全量只看 L1+，后续靠 compaction 产出的 changelog 把 L0 的变化补上。
- `freshness`：初始全量把 starting snapshot 里的 L0 也 merge 进去，启动时就看到更接近该 snapshot 逻辑最终态的结果。

follow-up 仍然消费 changelog snapshot，不因为 `freshness` 就变成"直接跟随 APPEND snapshot"。

但这样会引入一个新问题：

- bootstrap 已经把 starting snapshot 的 L0 合进初始全量；
- 后续来自**同一次 commit** 的 compaction snapshot 会产出一份 changelog，描述"APPEND → compacted"的变化——这份变化其实已经体现在 bootstrap 里了。

因此 `freshness` 模式下流读需要**跳过这种"同一 commit 的补偿 compaction snapshot"**，否则会把已经发过的变化再发一次。`performance` 模式不能跳，因为它的 bootstrap 没看到那批 L0。

换一句话描述：`freshness` 在流读里是 **bootstrap freshness**——只让第一帧更新，不改增量通道。

## 6. 性能画像

无 L0 的 bucket 与 `performance` **完全等价**：L1+ 依然整层走快路径 + DV 跳读，value-stats 文件级裁剪和 bucket 级整桶裁剪都有效。

有 L0 的 bucket 按 L0 的 key 分布退化：

- L0 的 key 分布窄 → 大部分 L1+ 文件与 L0 不重叠，保留快路径，只有重叠的那几个文件跟 L0 进 merge；
- L0 的 key 分布广 → 大部分 L1+ 都要和 L0 一起 merge，整个 bucket 基本变成 merge-on-read。

另外有一个**固有代价**：含 L0 的 bucket 无法整桶裁剪。因为一旦 L0 放行，该 bucket 至少有一个 entry 通过 filter，bucket 级剪枝就拦不住它。强选择性谓词的查询会因此多扫一些 bucket。这个是 `freshness` 的固有退化，不是实现缺陷。

把 `freshness` 放进更大的参照系看：

| 能力维度 | DV 表 `performance` | DV 表 `freshness` | 非 DV 普通 PK 表 |
|----------|---------------------|--------------------|-------------------|
| L0 可见 | 不可见 | 可见 | 可见 |
| L1+ 文件级 value-stats 裁剪 | 有（全部可信） | 有（仅 L0 豁免） | 不可用（整条路径关闭） |
| bucket 级整桶裁剪 | 有 | 含 L0 的 bucket 失效，其余仍有效 | 不可用 |
| L1+ 快路径 | 整层一起走裸读 | 仅与 L0 不重叠的 L1+ 走裸读 | 与 `freshness` 等价 |

实用结论：

- 相对 `performance`：`freshness` 的退化仅发生在含 L0 的 bucket，且随 L0 比例放大；L0 能被 compaction 快速消化时，退化窗口很短。
- 相对非 DV 普通 PK 表：`freshness` 的**真正优势只有一条** —— L1+ 的文件级 value-stats 裁剪（及其带来的 bucket 级裁剪）。带选择性谓词的查询下 `freshness` 严格更优；无谓词全表扫下两者机制等价，差距消失。

一句话总结取舍：`freshness` 用"含 L0 bucket 的 merge + 整桶裁剪失效"换"L0 立即可见"，其余场景与 `performance` 等价；最坏情况也不会比非 DV 普通 PK 表更差。

## 7. 改动范围

读路径四层中实际触点只有两处：

1. **扫描层的 L0 过滤**：按模式决定是否保留当前的"只看 L1+"过滤。
2. **value-stats 裁剪对 L0 的豁免**：L0 之间 key 重叠、且可能与 L1+ 同 PK，一旦按文件级 stats 跳掉 L0 会漏掉它对 L1+ 的覆盖语义，因此 `freshness` 下 L0 文件必须绕过 value filter；L1+ 文件照旧。此改动在 `performance` 模式下是 no-op（L0 根本进不来）。

其余组件都不动：

- split 生成：现有的区间划分逻辑已经保证"与 L0 重叠的 L1+ 和 L0 进同一 merge 单元、不重叠的 L1+ 保留快路径"，正是 §3、§4 论证出来的必需行为。
- reader 分发：按 split 自身的"是否可裸读"标志自动落到正确 reader，无需感知模式。
- 合并读取器：已具备 DV 预过滤能力，DV 就绪时用、未就绪时跳过，语义自洽。
- 写入路径、DV 生成逻辑：完全不涉及。

流读侧额外加一条：`freshness` 下跳过"与 starting APPEND 属于同一 commit 的补偿 compaction snapshot"，避免 §5 描述的重复事件。
