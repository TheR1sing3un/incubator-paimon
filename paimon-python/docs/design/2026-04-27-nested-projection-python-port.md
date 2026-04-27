# 嵌套字段投影（PyPaimon 移植设计）

## 背景

Java 端的 ReadBuilder 同时接受 `int[] withProjection(...)`（顶层投影）和 `RowType withReadType(RowType)`（支持嵌套行裁剪）。`Projection` 工具类（`paimon-common/.../utils/Projection.java`）提供 `TopLevelProjection`（一维 `int[]`）与 `NestedProjection`（二维 `int[][]`，路径 `[1, 0]` 表示顶层第 1 字段的第 0 子字段）两套实现。底层格式 reader（Parquet）真正下推嵌套列读取，输出 RowType 把嵌套路径**扁平化**为顶层字段（字段名用 `_` 拼接，重名加 `_$N` 后缀）。

PyPaimon 当前只接受 `with_projection(List[str])`，仅支持顶层列名；带 `.` 的字段名（如 `'struct.nested'`）会被静默忽略掉。`pruneDataType`（递归裁剪 ROW/ARRAY/MAP 子字段）也完全缺失。这阻止了 PyPaimon 用户把嵌套列裁剪压到文件层。

## 目标

- 在 `ReadBuilder` 提供低层 API `with_nested_projection(List[List[int]])`：和 Java `int[][] withProjection(int[][])` 等价
- 在 `with_projection(List[str])` 中识别 dotted name（如 `"mv_col.LATEST_VERSION"`），转为嵌套路径
- 实现 `Projection` 工具类（`TopLevelProjection` / `NestedProjection`），其 `project(row_type)` 返回扁平化的 `List[DataField]`
- 在 Parquet/ORC reader 把嵌套投影下推到 PyArrow `dataset.scanner(columns={...})` dict 形式
- 保持嵌套字段的 fieldId（继承自原始嵌套字段），让 schema evolution 仍能按 fieldId 重映射

## 非目标

- **本阶段不支持嵌套字段下的谓词下推**——嵌套投影 + 嵌套谓词的组合非常复杂，留给后续 phase
- **不在 Avro / Lance 路径做真正的嵌套下推**：fastavro 没有嵌套列读取 API；本阶段在这两种格式上把整个顶层结构读出来再 Python 侧投影（功能正确，性能略劣，但与现有 Avro 顶层投影策略一致）
- **不重构 `_create_base_index_mapping`**——嵌套字段的 schema evolution 通过新生成的扁平 DataField 的 fieldId 来对接，不动现有逻辑
- **不修改 ARRAY<ROW> / MAP<_, ROW> 的嵌套投影**——只支持 ROW 类型的子字段投影。Java 端的 `pruneDataType` 也仅在 ROW 上做递归

## 设计

### 一、`Projection` 工具类（mirror Java `Projection.java`）

新建 `pypaimon/utils/projection.py`，导出：
- 抽象 `Projection`，方法 `project(row_type) -> RowType`、`is_nested()`、`to_top_level_indexes() -> List[int]`、`to_nested_indexes() -> List[List[int]]`
- `TopLevelProjection(indexes: List[int])`
- `NestedProjection(paths: List[List[int]])`：每条路径长度 ≥ 1；长度 1 等同顶层；长度 ≥ 2 走嵌套
- 工厂函数 `Projection.of(indexes_or_paths)` — 自动选择子类
- `NestedProjection.project(rowtype)` 实现扁平化：
  - 路径 `[i]`：直接取顶层字段
  - 路径 `[i, j]`：取顶层第 i 字段的子字段第 j（必须是 ROW 类型），新字段名 `<top>_<sub>`，**fieldId 继承子字段原始 id**
  - 长度更深的路径递推
  - 重名冲突在末尾追加 `_$N`（mirror Java L256-260）

### 二、ReadBuilder API 扩展

```python
class ReadBuilder:
    def with_projection(self, projection: List[str]) -> 'ReadBuilder':
        # 既有：识别 dotted name；如 'mv_col.LATEST_VERSION' 解析为 [mv_col_idx, LATEST_VERSION_idx]
        # 内部状态切换为 _nested_projection（List[List[int]]）
        ...

    def with_nested_projection(self, paths: List[List[int]]) -> 'ReadBuilder':
        # 新增低层 API；纯位置索引
        ...

    def read_type(self) -> List[DataField]:
        # 应用 Projection.project(...) 把嵌套路径扁平化为顶层字段列表
        ...
```

dotted name 解析规则：
- 任何字段名含 `.` 时尝试逐段解析：`a.b.c` → `[a_idx, b_idx_in_a, c_idx_in_b]`
- 顶层字段直接 → 长度 1 路径
- 字段名本身就含 `.`（如反引号转义）暂不支持，跨 SDK 行为对齐 Java

### 三、Format reader 的嵌套下推

`format_pyarrow_reader.py` 接收 `read_fields: List[DataField]` 不变，但增加可选的 `nested_paths: Optional[List[List[Union[str, int]]]]`：

- 当 nested_paths 提供时，scanner 用 dict 形式：
  ```python
  ds.dataset(...).scanner(columns={
      flattened_name: ds.field(*path_components_as_strings)
      for flattened_name, path_components in zip(read_field_names, nested_paths)
  }, filter=..., batch_size=...)
  ```
  PyArrow 会真正只读那些嵌套列。
- 否则走当前 `columns=List[str]` 路径，行为不变。
- Avro / Lance：暂不接 nested_paths；`SplitRead.file_reader_supplier` 检测到 nested 时 fallback 到读完整顶层结构 + Python 侧投影

### 四、SplitRead 的最小改动

- `SplitRead.__init__` 接收的 `read_type` 仍然是 `List[DataField]`（已扁平），无需变化
- 新增 `self.nested_paths`：当 ReadBuilder 走嵌套路径时传入；否则 None
- `file_reader_supplier` 在格式分支里把 `nested_paths` 透传到 `FormatPyArrowReader`
- `_create_base_index_mapping`（schema evolution 重映射）仍按 fieldId 工作；扁平字段继承了嵌套子字段的 fieldId，所以无须改动

### 五、与 Phase 1 inner/outer 的关系

嵌套投影发生在 ReadBuilder 层（用户 → outer），扁平后才进入 SplitRead。inner/outer 双层投影机制不变：merge 引擎仍可能补回缺失的嵌套已扁平字段（如果 mv 列被嵌套裁掉）。**但本阶段不强制处理"用户嵌套裁掉 mv 列子字段"的场景**——若用户用嵌套裁了 `mv_col.LATEST_VERSION` 但没保留 `mv_col` 整体，`adjust_read_type` 因为按字段名匹配查不到 `mv_col` 仍会重新 append，造成嵌套与扁平混合的输出。该组合在 Phase 1 之外属于灰色地带，本阶段不处理；**当用户走嵌套投影时，merge 表强制 fallback 到 full schema 读取**（在 SplitRead 检测：`isinstance(self, MergeFileSplitRead) and nested_paths is not None` → 报错或忽略嵌套）。

## 关键文件

新建：
- `pypaimon/utils/projection.py`（Projection 工具）

修改：
- `pypaimon/read/read_builder.py`（dotted name 解析 + with_nested_projection）
- `pypaimon/read/reader/format_pyarrow_reader.py`（nested_paths 入参）
- `pypaimon/read/split_read.py`（透传 nested_paths）
- `pypaimon/read/stream_read_builder.py`（同步 ReadBuilder API 扩展）

测试：
- `pypaimon/tests/test_projection_utility.py`（Projection / TopLevel / Nested 单测）
- `pypaimon/tests/test_nested_projection_e2e.py`（Parquet 嵌套下推端到端，Avro fallback 行为）

## 验证

```bash
flake8 --config=dev/cfg.ini pypaimon/
pytest pypaimon/tests/test_projection_utility.py -v
pytest pypaimon/tests/test_nested_projection_e2e.py -v
pytest pypaimon/tests/  # 全量回归（确认 1124 passed 不下降）
```

## 范围切分（实施时实际拆分）

实施时确认 Phase 2 完整工作量超出单轮交付，按以下子阶段执行：

- **Phase 2a（已完成）**：Projection 工具类（`TopLevelProjection` / `NestedProjection`）+ ReadBuilder 的 `with_nested_projection(int[][])` 低层 API + `with_projection` 识别 dotted name。提供基础设施 + API 闸门。
- **Phase 2b（已完成）**：`FormatPyArrowReader` 用 `dataset.scanner(columns={...})` dict 形式 + `ds.field(*path_strs)` 真下推到 Parquet/ORC 列读取；`SplitRead` 在 nested 模式下绕过 `_get_final_read_data_fields` / `create_index_mapping` / `_construct_partition_mapping` 的 field-id 机制（leaf 子字段 id 与顶层 id 会冲突）；分区表与 sub-field schema-evolution 边界都有测试覆盖。
- **Phase 2c（已完成）**：`FormatAvroReader` 加 `nested_name_paths` 入参，按字典路径 walk 提取叶子值（fastavro 不支持原生下推，Python 侧 fallback）。Lance 嵌套显式拒绝（NotImplementedError）。
- **Phase 2d（已完成）**：PK 表 + merge engine + nested 投影。`SplitRead` 在 PK + nested 时把 outer 的扁平字段折叠回顶层父 struct 作为 inner_read_type 传给 merge function；`OuterProjectionRecordReader` 增加 path-based 提取模式，按 `(inner_idx, sub_path)` 走入 struct 子字段恢复叶子值。`adjust_read_type` 走原有路径（对顶层 inner 操作）。`_needs_nested_file_pushdown` 与 `_is_nested_mode` 解耦，前者只在 append-only 走文件层 nested 下推，后者标识 API 表面的嵌套请求。

Phase 2a/2b/2c/2d 已端到端打通 append-only 与 PK + versioned-partial-update 两条路径，覆盖：dotted-name API、低层 nested-paths API、partition keys、sub-field schema evolution（缺失叶子返回 NULL）、与 PyArrow predicate 的安全交互、PK 表多次写入下的 mv 列子字段读取。
