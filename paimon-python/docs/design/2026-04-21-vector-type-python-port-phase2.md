# PyPaimon Vector Column Family (VCF) 移植计划 — Phase 2（写入侧）

## Context

**Phase 1 已完成**（分支 `feat_column_split`，commits `7194057d9 → af7e51eef`）：
- `VectorType` 类 + JSON/SQL 协议 + PyArrow `fixed_size_list` / Avro `array` 桥接
- `VectorDescriptor` 二进制编解码（LE，magic `0x5645435F50545200`，版本 1，29+N 字节，与 Java 字节对齐）
- `vector_ref.resolve_vector_descriptors(...)`：descriptor BINARY 列 → `pyarrow.FixedSizeListArray`
- `FormatPyArrowReader._apply_vector_resolution`：读路径识别三种物理形态（fixed_size_list / list / BINARY+descriptor），其中 **BINARY+descriptor 路径就是 Java VCF 的布局**
- Schema 校验：顶层 VECTOR 列必须 nullable、非 PK、非 partition
- **写入端**：Phase 1 默认把 `fixed_size_list` 直接落到 Parquet，**不做**分离存储

**Phase 2 目标**：补齐写入端的 Vector Column Family 分离存储能力。当 PK 表开启 `vector-column-family.enabled=true`：

1. 把 VECTOR 列从主 `pa.Table` 中剥离
2. 将向量原始字节以 append-only flat binary 写入 `.vector.bin` 文件（8 字节对齐、LE、无 header / footer）
3. 用 `VectorDescriptor.serialize()` 字节填充一个 BINARY 列替换原 VECTOR 列
4. `.vector.bin` 在达到 `vector-column-family.target-file-size`（默认 128 MiB）时滚动新文件
5. 把实际写出的 `.vector.bin` 文件名列入 `DataFileMeta.extra_files`，使 GC 和 Phase 1 的 reader 能定位到向量数据
6. **跨语言字节兼容**：pypaimon 写出的表被 Java VCF reader 读取正确，反之亦然

Phase 1 的 read 路径已经能读 Java VCF 产物，所以 Phase 2 完成后 pypaimon 即可双向互通。

**明确 Phase 2 不做**（文末 TODO）：partial-update merge-fn 的 null-skip 保留语义、append-only 表的 VCF、ORC 主格式、AccelerateIndex 链路、分布式 GC。

设计文档副本将写入 `paimon-python/docs/design/2026-04-21-vector-type-python-port-phase2.md`，沿用 Phase 1 设计文档 `2026-04-21-vector-type-python-port.md` 的风格。

## Architecture

### Java 侧关键组件（Phase 2 需严格对齐字节/协议）

| Java 类 | 职责 | 关键点 |
|---|---|---|
| `VectorColumnFamilyFlushHelper.processAndReplace(kv)` | flush 时拦截 KV，剥离向量、生成 descriptor | 所有向量列全 null 时原样返回 KV（依赖 partial-update null-skip） |
| `DefaultVectorFileWriter.writeVector(vec)` | 写入单个 vector 字节，返回 descriptor；`getPos() >= targetFileSize` 时 seal 并下次 writeVector 时开新文件 | `bytesPerVector = ((dim * elemSize + 7) / 8) * 8`（8 字节对齐） |
| `DataFilePathFactory.newVectorPath("bin")` | 产出 `{prefix}{uuid}-{counter}.vector.bin` | 通过 `VectorType.isVectorStoreFile(name)` (`name.contains(".vector.")`) 识别 |
| `KeyValueFileStoreWrite` | 根据 `CoreOptions` 构造 `VectorColumnFamilyFlushHelper.Factory` 注入 `MergeTreeWriter` | 读取 `vectorColumnFamilyEnabled`、`vectorColumnFamilyColumns`、`vectorColumnFamilyTargetFileSize` |
| `SchemaValidation.validateVectorColumns` | PK 表 + 1 个 vector 列 + 非 PK/partition + nullable + 无 external paths | 目前限制 `vectorColumns.size() == 1` |
| `VectorFileGarbageCollector` | 扫表目录列出 `.vector.bin`；分布式扫主文件抽取 descriptor 引用；删差集 | 引用从 live 数据行的 VECTOR 列 descriptor 提取 |

### pypaimon 写入路径（现状）

```
Table.new_batch_write_builder()
    -> BatchWriteBuilder.new_write() -> BatchTableWrite
         -> FileStoreWrite.write(partition, bucket, batch)
              -> _create_data_writer(): 路由分派
                   - blob 列 -> DataBlobWriter
                   - PK 表   -> KeyValueDataWriter
                   - append -> AppendOnlyDataWriter
              -> writer.write(batch)
    -> prepare_commit() -> 汇总 DataFileMeta
```

### Phase 2 写入路径（加入 VCF）

```
Table.new_batch_write_builder()
    -> BatchWriteBuilder.new_write()
         -> FileStoreWrite._create_data_writer():
              - VCF enabled on PK table -> VectorColumnFamilyDataWriter  (NEW)
              - blob 列 -> DataBlobWriter
              - PK 表   -> KeyValueDataWriter
              - append -> AppendOnlyDataWriter

VectorColumnFamilyDataWriter (wraps/extends KeyValueDataWriter):
    _process_data(batch):
        1. 识别 VECTOR 列名 (self.vector_columns)
        2. 从 batch 中剥离 VECTOR 列得 batch_vector (pa.Array[FixedSizeList])
        3. 用 VectorFileWriter 把每行 vector 的字节写入 .vector.bin，
           收集每行的 VectorDescriptor.serialize() 字节
        4. 用 BINARY 列 replace 原 VECTOR 列，返回 batch_without_vector_as_binary
        5. 继续走父类 _process_data (KV 系统字段 + 排序)
    prepare_commit():
        6. close 所有 VectorFileWriter，收集实际写出的 .vector.bin 路径
        7. 主 parquet 的 DataFileMeta.extra_files 填入 .vector.bin 相对文件名
```

关键复用（来自 Phase 1）：
- `VectorDescriptor.serialize()` / `deserialize()` — `pypaimon/data/vector_descriptor.py`
- `resolve_vector_descriptors(...)` — `pypaimon/data/vector_ref.py`（read 路径已具备）
- `FormatPyArrowReader._apply_vector_resolution` — 读 `.vector.bin` 已在 Phase 1 落地

Phase 2 新增的组件与 `DataBlobWriter` (`pypaimon/write/writer/data_blob_writer.py:36-373`) 同构：后者把 BLOB 列写到 `.blob`、主表替换为 descriptor，模式可直接借鉴。

## Tech Stack

- Python ≥ 3.6；pyarrow ≥ 6；numpy（与 Phase 1 一致）
- 无新外部依赖
- 测试：pytest（沿用 Phase 1 pattern）

## File Structure

### 新建

| 路径 | 职责 |
|---|---|
| `paimon-python/pypaimon/write/writer/vector_file_writer.py` | `VectorFileWriter` 类：打开 `.vector.bin`，write 原始 bytes，返回 `VectorDescriptor`，支持目标大小滚动、close、abort。等价 Java `DefaultVectorFileWriter` |
| `paimon-python/pypaimon/write/writer/vector_cf_data_writer.py` | `VectorColumnFamilyDataWriter`：组合 `VectorFileWriter` + 底层 `KeyValueDataWriter`，负责列剥离 + descriptor 生成 + extra_files 汇总。等价 Java `VectorColumnFamilyFlushHelper` + writer 编排 |
| `paimon-python/pypaimon/operation/vector_file_garbage_collector.py` | GC 端口：扫目录 `.vector.bin`、扫 VECTOR 列提取引用、删差集。对应 Java `VectorFileGarbageCollector` |
| `paimon-python/pypaimon/tests/write/vector_cf_writer_test.py` | 单测：writer 组件（stub FileIO + fake batch） |
| `paimon-python/pypaimon/tests/table/vector_cf_table_test.py` | E2E：PK 表 VCF 开启 → 写 → 主 parquet 含 BINARY 列 → `.vector.bin` 内容正确 → 用 Phase 1 reader 读回比对 |
| `paimon-python/pypaimon/tests/operation/vector_file_gc_test.py` | GC 测试：构造悬挂 `.vector.bin`、验证删除 |
| `paimon-python/docs/design/2026-04-21-vector-type-python-port-phase2.md` | 本计划在仓内的 materialize 副本 |

### 修改

| 路径 | 变更 |
|---|---|
| `paimon-python/pypaimon/common/options/core_options.py` | 新增 `VECTOR_COLUMN_FAMILY_ENABLED`、`VECTOR_COLUMN_FAMILY_COLUMNS`、`VECTOR_COLUMN_FAMILY_TARGET_FILE_SIZE`；新增 `CoreOptions` 方法访问器 |
| `paimon-python/pypaimon/schema/schema.py` | VCF 开启时的额外校验：只能 PK 表、只 1 个 vector 列、无 external path（保持 Phase 1 顶层 VECTOR 基础校验不变） |
| `paimon-python/pypaimon/utils/file_store_path_factory.py` | 新增 `vector_bin_path(partition, bucket) -> str`：产出 `{bucket_dir}/{prefix}{uuid}-0.vector.bin` |
| `paimon-python/pypaimon/write/file_store_write.py` | `_create_data_writer` 增加 VCF 路由分支（优先级：VCF > blob > KV > append） |
| `paimon-python/pypaimon/tests/data_types_test.py` | 补充 VCF schema 校验测试 |
| `paimon-python/CLAUDE.md` | 架构章节追加 VCF 写入说明 |
| `paimon-python/README.md` | 新增 "Vector Column Family" quickstart，指向 design 文档 |

---

# Tasks

> TDD 原则：每个 Task 先写红测 → 最小实现 → 跑绿 → commit。新源码文件必须含 Apache 2.0 license header。commit 前必须跑通新增/相关测试（按 `feedback_verify_before_done`）。

---

### Task 0: 把本 Phase 2 计划物化到项目文档

**Files:**
- Create: `paimon-python/docs/design/2026-04-21-vector-type-python-port-phase2.md`

- [ ] **Step 0.1**：把 `/Users/lcy/.claude/plans/java-embedding-embedding-java-java-pyth-sleepy-puzzle.md` 的内容原样复制到 `paimon-python/docs/design/2026-04-21-vector-type-python-port-phase2.md`。保留 Phase 1 的 design doc 不动。
- [ ] **Step 0.2**：`git add paimon-python/docs/design/2026-04-21-vector-type-python-port-phase2.md && git commit -m "[python] Add VCF Phase 2 design doc"`

---

### Task 1: CoreOptions 增加 VCF 相关配置

**Files:**
- Modify: `paimon-python/pypaimon/common/options/core_options.py`
- Test: `paimon-python/pypaimon/tests/` 补充最小选项解析单测（或附在 Task 2 里）

**Step 1.1 写测试** — 在合适位置（同目录已有选项测试则追加；否则临时放 `tests/write/vector_cf_options_test.py`）：

```python
from pypaimon.common.options import Options
from pypaimon.common.options.core_options import CoreOptions

def test_vcf_default_values():
    co = CoreOptions(Options({}))
    assert co.vector_column_family_enabled() is False
    assert co.vector_column_family_columns() == []                   # 空 list 表示自动检测
    assert co.vector_column_family_target_file_size() == 128 * 1024 * 1024

def test_vcf_override_values():
    co = CoreOptions(Options({
        "vector-column-family.enabled": "true",
        "vector-column-family.columns": "embed,vec2",
        "vector-column-family.target-file-size": "64mb",
    }))
    assert co.vector_column_family_enabled() is True
    assert co.vector_column_family_columns() == ["embed", "vec2"]
    assert co.vector_column_family_target_file_size() == 64 * 1024 * 1024
```

**Step 1.2 红**：`pytest` 预期 AttributeError。

**Step 1.3 实现**：对齐文件中 `BLOB_AS_DESCRIPTOR`、`TARGET_FILE_SIZE` 的注册风格，新增三项 `ConfigOption`，以及 `CoreOptions` 类上三个方法：

```python
VECTOR_COLUMN_FAMILY_ENABLED: ConfigOption[bool] = (
    ConfigOptions.key("vector-column-family.enabled")
    .boolean_type()
    .default_value(False)
    .with_description("Enable vector column family separation for PK tables.")
)
VECTOR_COLUMN_FAMILY_COLUMNS: ConfigOption[str] = (
    ConfigOptions.key("vector-column-family.columns")
    .string_type()
    .no_default_value()
    .with_description("Comma-separated vector column names. Empty = auto-detect all VectorType columns.")
)
VECTOR_COLUMN_FAMILY_TARGET_FILE_SIZE: ConfigOption[MemorySize] = (
    ConfigOptions.key("vector-column-family.target-file-size")
    .memory_type()
    .default_value(MemorySize.parse("128mb"))
    .with_description("Target size of one .vector.bin file before rollover.")
)

# CoreOptions method additions
def vector_column_family_enabled(self) -> bool:
    return self.options.get(self.VECTOR_COLUMN_FAMILY_ENABLED)

def vector_column_family_columns(self) -> List[str]:
    raw = self.options.get(self.VECTOR_COLUMN_FAMILY_COLUMNS)
    if raw is None or not raw.strip():
        return []
    return [c.strip() for c in raw.split(",") if c.strip()]

def vector_column_family_target_file_size(self) -> int:
    return self.options.get(self.VECTOR_COLUMN_FAMILY_TARGET_FILE_SIZE).get_bytes()
```

**Step 1.4 跑绿**：`pytest pypaimon/tests/... -v`

**Step 1.5 commit**：`[python] Add vector-column-family core options`

---

### Task 2: Schema 侧 VCF 校验

对齐 Java `SchemaValidation.validateVectorColumns` 语义：当 `vector-column-family.enabled=true`，要求 PK 表、恰好 1 个 vector 列（自动检测或 `columns` 指定）、该列必须 nullable、非 PK、非 partition。无 external paths。不改动 Phase 1 的顶层 VECTOR 常规校验。

**Files:**
- Modify: `paimon-python/pypaimon/schema/schema.py`
- Test: `paimon-python/pypaimon/tests/data_types_test.py`

**Step 2.1 写测试**（追加）：

```python
def test_vcf_requires_primary_key(self):
    from pypaimon.schema.schema import Schema
    pa_schema = pa.schema([
        pa.field("id", pa.int64(), nullable=False),
        pa.field("embed", pa.list_(pa.float32(), 4), nullable=True),
    ])
    with self.assertRaises(ValueError):
        Schema.from_pyarrow_schema(pa_schema,
                                   options={"vector-column-family.enabled": "true"})

def test_vcf_requires_exactly_one_vector(self):
    from pypaimon.schema.schema import Schema
    pa_schema = pa.schema([
        pa.field("id", pa.int64(), nullable=False),
        pa.field("embed1", pa.list_(pa.float32(), 4), nullable=True),
        pa.field("embed2", pa.list_(pa.float32(), 4), nullable=True),
    ])
    with self.assertRaises(ValueError):
        Schema.from_pyarrow_schema(pa_schema, primary_keys=["id"],
                                   options={"vector-column-family.enabled": "true"})

def test_vcf_happy_path(self):
    from pypaimon.schema.schema import Schema
    pa_schema = pa.schema([
        pa.field("id", pa.int64(), nullable=False),
        pa.field("embed", pa.list_(pa.float32(), 4), nullable=True),
    ])
    schema = Schema.from_pyarrow_schema(pa_schema, primary_keys=["id"],
                                        options={"vector-column-family.enabled": "true"})
    # 不抛错即通过
    self.assertEqual(len(schema.fields), 2)
```

**Step 2.2 实现**：在 `Schema.from_pyarrow_schema` 的 vector 基础校验之后追加：

```python
vcf_enabled = options and options.get("vector-column-family.enabled", "false").lower() == "true"
if vcf_enabled:
    if not pk_set:
        raise ValueError("Vector column family requires a primary key table.")
    configured = [c.strip() for c in (options.get("vector-column-family.columns", "") or "").split(",") if c.strip()]
    vector_cols = configured or [f.name for f in fields if isinstance(f.type, VectorType)]
    if len(vector_cols) != 1:
        raise ValueError(f"Vector column family currently supports exactly one vector column, got {vector_cols}.")
    col = vector_cols[0]
    vf = next((f for f in fields if f.name == col), None)
    if vf is None:
        raise ValueError(f"Vector column '{col}' not found in schema.")
    if not isinstance(vf.type, VectorType):
        raise ValueError(f"Column '{col}' must be VectorType, got {vf.type}.")
    # 顶层 VECTOR 常规校验（Phase 1 已覆盖：nullable、非 PK、非 partition）会在 vf 上生效
    if options.get("data-file.external-paths"):
        raise ValueError("Vector column family does not support data-file.external-paths.")
```

**Step 2.3 跑绿 + commit**：`[python] Validate vector-column-family schema constraints`

---

### Task 3: Path factory 新增 `vector_bin_path`

**Files:**
- Modify: `paimon-python/pypaimon/utils/file_store_path_factory.py`
- Test: 附在 Task 4 的 VectorFileWriter 测试里

**Step 3.1 实现**：

```python
# 在 FileStorePathFactory 类里
VECTOR_BIN_SUFFIX = ".vector.bin"

def vector_bin_path(self, partition: Tuple, bucket: int) -> str:
    """Produce {bucket_dir}/{data_file_prefix}{uuid}-0.vector.bin"""
    import uuid as _uuid
    bucket_dir = self.bucket_path(partition, bucket)
    prefix = self._core_options.data_file_prefix()  # reuse同一前缀
    return f"{bucket_dir.rstrip('/')}/{prefix}{_uuid.uuid4()}-0{self.VECTOR_BIN_SUFFIX}"
```

注意：Java 端 counter 语义（`-{counter}` 递增）不强依赖；pypaimon 里每次滚动生成新 uuid 可避免命名冲突，后缀固定 `-0`。这与 Phase 1 `DataWriter._write_data_to_file` 的 `uuid.uuid4()}-0.{format}` 命名一致，读路径与 GC 只靠 `.vector.bin` 后缀识别，兼容。

**Step 3.2 commit 合并到 Task 4。**

---

### Task 4: `VectorFileWriter`

**Files:**
- Create: `paimon-python/pypaimon/write/writer/vector_file_writer.py`
- Create: `paimon-python/pypaimon/tests/write/vector_file_writer_test.py`

**职责**：给定 FileIO + path_factory + DataField(VectorType) + target_file_size，提供：
- `write_vector(raw_bytes_row: np.ndarray[elem_dtype, (dim,)]) -> VectorDescriptor`
- `close()` / `abort()`：清理 open stream、返回 / 删除已写文件
- 内部自动滚动：当前流写入后 `stream.tell() >= target_file_size` 即关闭，下次 write 新开文件

**bytesPerVector 对齐**：与 Java 一致，`bytesPerVector = ((dim * elem_size + 7) // 8) * 8`。

**Step 4.1 测试骨架**：

```python
import os, tempfile, numpy as np
from pypaimon.common.options import Options
from pypaimon.filesystem.local_file_io import LocalFileIO
from pypaimon.schema.data_types import AtomicType, DataField, VectorType
from pypaimon.write.writer.vector_file_writer import VectorFileWriter

def _make_writer(tmp, target_size=1 << 20):
    file_io = LocalFileIO("file://" + tmp, Options({}))
    df = DataField(1, "embed", VectorType(True, 3, AtomicType("FLOAT")))
    return VectorFileWriter(file_io, df, path_producer=lambda: os.path.join(tmp, f"data-{np.random.randint(1<<30)}-0.vector.bin"),
                            target_file_size=target_size)

def test_write_single_vector(tmp_path):
    writer = _make_writer(str(tmp_path))
    d = writer.write_vector(np.array([1.0, 2.0, 3.0], dtype=np.float32))
    writer.close()
    assert d.dimension == 3
    assert d.bytes_per_vector == 16   # ceil(3*4 / 8) * 8
    assert d.row_index == 0
    data = open(d.file_path, "rb").read()
    assert len(data) == d.bytes_per_vector
    assert np.frombuffer(data[:12], dtype="<f4").tolist() == [1.0, 2.0, 3.0]

def test_rollover_opens_new_file(tmp_path):
    writer = _make_writer(str(tmp_path), target_size=16)  # 一条就滚
    d1 = writer.write_vector(np.array([1.0, 2.0, 3.0], dtype=np.float32))
    d2 = writer.write_vector(np.array([4.0, 5.0, 6.0], dtype=np.float32))
    writer.close()
    assert d1.file_path != d2.file_path
    assert d1.row_index == 0 and d2.row_index == 0

def test_abort_deletes_files(tmp_path):
    writer = _make_writer(str(tmp_path))
    d = writer.write_vector(np.array([1.0, 2.0, 3.0], dtype=np.float32))
    writer.abort()
    assert not os.path.exists(d.file_path)
```

**Step 4.2 实现骨架**：

```python
# pypaimon/write/writer/vector_file_writer.py
import numpy as np
from typing import Callable, List, Optional
from pypaimon.common.file_io import FileIO
from pypaimon.data.vector_descriptor import VectorDescriptor
from pypaimon.schema.data_types import AtomicType, DataField, VectorType

_ELEM_SIZE = {
    "BOOLEAN": 1, "TINYINT": 1, "SMALLINT": 2,
    "INT": 4, "INTEGER": 4, "BIGINT": 8,
    "FLOAT": 4, "DOUBLE": 8,
}
_ELEM_NUMPY = {
    "BOOLEAN": "<?", "TINYINT": "<i1", "SMALLINT": "<i2",
    "INT": "<i4", "INTEGER": "<i4", "BIGINT": "<i8",
    "FLOAT": "<f4", "DOUBLE": "<f8",
}

class VectorFileWriter:
    def __init__(self, file_io: FileIO, vector_field: DataField,
                 path_producer: Callable[[], str], target_file_size: int):
        vt: VectorType = vector_field.type
        assert isinstance(vt, AtomicType) is False   # vt is VectorType
        elem = vt.element.type.upper().split("(")[0].split(" ")[0]
        self._file_io = file_io
        self._path_producer = path_producer
        self._target_file_size = target_file_size
        self._dim = vt.length
        self._elem_dtype = np.dtype(_ELEM_NUMPY[elem])
        raw_size = self._dim * _ELEM_SIZE[elem]
        self._bytes_per_vector = ((raw_size + 7) // 8) * 8   # 8-byte aligned
        self._pad = self._bytes_per_vector - raw_size
        self._current_path: Optional[str] = None
        self._current_stream = None
        self._current_pos = 0
        self._current_row = 0
        self._written_paths: List[str] = []

    def _open_new(self):
        self._current_path = self._path_producer()
        self._current_stream = self._file_io.new_output_stream(self._current_path)
        self._current_pos = 0
        self._current_row = 0
        self._written_paths.append(self._current_path)

    def write_vector(self, row_vector) -> VectorDescriptor:
        if self._current_stream is None:
            self._open_new()
        arr = np.asarray(row_vector, dtype=self._elem_dtype)
        if arr.shape != (self._dim,):
            raise ValueError(f"vector shape {arr.shape} != ({self._dim},)")
        raw = arr.tobytes(order="C")
        if self._pad:
            raw = raw + (b"\x00" * self._pad)
        self._current_stream.write(raw)
        desc = VectorDescriptor(self._current_path, self._current_row, self._bytes_per_vector, self._dim)
        self._current_row += 1
        self._current_pos += self._bytes_per_vector
        if self._current_pos >= self._target_file_size:
            self._seal_current()
        return desc

    def _seal_current(self):
        if self._current_stream is not None:
            self._current_stream.close()
        self._current_stream = None
        self._current_path = None
        self._current_pos = 0
        self._current_row = 0

    def close(self) -> List[str]:
        self._seal_current()
        return list(self._written_paths)

    def abort(self):
        self._seal_current()
        for p in self._written_paths:
            try:
                self._file_io.delete_quietly(p)
            except Exception:
                pass
        self._written_paths.clear()
```

**Step 4.3 跑绿 + commit**：`[python] Add VectorFileWriter for vector-column-family`

---

### Task 5: `VectorColumnFamilyDataWriter`

**Files:**
- Create: `paimon-python/pypaimon/write/writer/vector_cf_data_writer.py`
- Create: `paimon-python/pypaimon/tests/write/vector_cf_writer_test.py`

**职责**：包装 `KeyValueDataWriter`，在父类 `_process_data` 之前剥离 VECTOR 列、写入 `.vector.bin`、把 VECTOR 列替换为 BINARY descriptor 列。`prepare_commit` 时：
- close 所有 VectorFileWriter
- 把实际写出的 `.vector.bin` 文件名（相对路径或文件名）追加到每个主 DataFileMeta 的 `extra_files`
- 若写入失败则 abort，删除主文件与向量文件

**设计选择**：
- **按批聚合 vs 按行一条**：pypaimon 的 `DataWriter` 是以 `pa.RecordBatch` 为粒度处理，而 Java VCF 在 KV 级别拦截。我们直接在 batch 级写入：对 batch 中每个非空 VECTOR 元素调用一次 `VectorFileWriter.write_vector`。这样是正确的但当 batch 很大时会多次小 IO，可后续优化。
- **全 null 行的处理**：这个 batch 的这些行在生成 BINARY 列时填 null，主文件保留 null 值。这保持与 Phase 1 read 路径兼容（reader 看到 null descriptor 返回 null vector）。
- **partial-update 语义保留**：本 Phase 不做跨 commit 的 null-skip 复用 Java merge-fn 的策略，列入 TODO。

**Step 5.1 测试骨架**：

```python
import os, tempfile, numpy as np, pyarrow as pa
from pypaimon import CatalogFactory, Schema
from pypaimon.data.vector_descriptor import VectorDescriptor

def test_vcf_writer_produces_descriptor_column_and_vector_bin(tmp_path):
    warehouse = str(tmp_path / "wh")
    catalog = CatalogFactory.create({"warehouse": warehouse})
    catalog.create_database("db", True)
    pa_schema = pa.schema([
        pa.field("id", pa.int64(), nullable=False),
        pa.field("embed", pa.list_(pa.float32(), 3), nullable=True),
    ])
    schema = Schema.from_pyarrow_schema(
        pa_schema, primary_keys=["id"],
        options={
            "bucket": "1",
            "vector-column-family.enabled": "true",
        },
    )
    catalog.create_table("db.vcf_t", schema, False)
    t = catalog.get_table("db.vcf_t")
    tbl = pa.Table.from_pydict({
        "id": pa.array([1, 2, 3], type=pa.int64()),
        "embed": pa.array([[0.1,0.2,0.3], [0.4,0.5,0.6], [0.7,0.8,0.9]],
                          type=pa.list_(pa.float32(), 3)),
    }, schema=pa_schema)
    wb = t.new_batch_write_builder()
    tw, tc = wb.new_write(), wb.new_commit()
    tw.write_arrow(tbl); tc.commit(tw.prepare_commit()); tw.close(); tc.close()

    # 1) 主 parquet 的 embed 列是 BINARY
    import pyarrow.parquet as pq, glob
    parquet_files = glob.glob(os.path.join(warehouse, "db.db/vcf_t/bucket-0/*.parquet"))
    assert len(parquet_files) >= 1
    raw = pq.read_table(parquet_files[0])
    assert pa.types.is_binary(raw.schema.field("embed").type) or \
           pa.types.is_large_binary(raw.schema.field("embed").type)

    # 2) .vector.bin 存在
    vector_files = glob.glob(os.path.join(warehouse, "db.db/vcf_t/bucket-0/*.vector.bin"))
    assert len(vector_files) >= 1

    # 3) Descriptor 指向真实文件 + 字节正确
    desc_bytes = raw.column("embed")[0].as_py()
    desc = VectorDescriptor.deserialize(desc_bytes)
    assert desc.dimension == 3
    expected_bytes = ((3*4 + 7) // 8) * 8   # 16
    assert desc.bytes_per_vector == expected_bytes
    with open(desc.file_path, "rb") as f:
        f.seek(desc.row_index * desc.bytes_per_vector)
        chunk = f.read(3 * 4)                  # dim * 4
    assert np.frombuffer(chunk, dtype="<f4").tolist() == [0.1, 0.2, 0.3]

    # 4) Phase 1 reader 读回 FixedSizeList
    rb = t.new_read_builder()
    read_table = rb.new_read().to_arrow(rb.new_scan().plan().splits())
    order = np.argsort(read_table.column("id").to_pylist())
    read_vec = np.array(read_table.column("embed").to_pylist(), dtype=np.float32)[order]
    np.testing.assert_array_almost_equal(
        read_vec, np.array([[0.1,0.2,0.3],[0.4,0.5,0.6],[0.7,0.8,0.9]], dtype=np.float32))

def test_vcf_rollover_multiple_vector_bin(tmp_path):
    # 使用 target-file-size = 64 强制每 4 行一个文件
    ...

def test_vcf_null_vector_row(tmp_path):
    # embed 列含 null，主 parquet 对应行的 descriptor 也是 null；reader 读回 null
    ...
```

**Step 5.2 实现骨架**（~120 行）：

```python
# pypaimon/write/writer/vector_cf_data_writer.py
import pyarrow as pa
from typing import List, Tuple
from pypaimon.common.options.core_options import CoreOptions
from pypaimon.data.vector_descriptor import VectorDescriptor
from pypaimon.manifest.schema.data_file_meta import DataFileMeta
from pypaimon.schema.data_types import VectorType
from pypaimon.write.writer.key_value_data_writer import KeyValueDataWriter
from pypaimon.write.writer.vector_file_writer import VectorFileWriter


class VectorColumnFamilyDataWriter(KeyValueDataWriter):
    """KV writer with vector-column-family separation: 把 VECTOR 列写到 .vector.bin，
    主表留 BINARY descriptor 列，close 时把 .vector.bin 文件名附到每个主 DataFileMeta 的
    extra_files。
    """
    def __init__(self, table, partition, bucket, max_seq_number, options: CoreOptions,
                 write_cols=None, merge_mode=None, vector_column: str = None,
                 target_file_size: int = 128 * 1024 * 1024):
        super().__init__(table, partition, bucket, max_seq_number, options, write_cols, merge_mode)
        self._vector_column = vector_column
        self._vector_field = self.table.field_dict[vector_column]
        assert isinstance(self._vector_field.type, VectorType)
        self._target_file_size = target_file_size
        self._vector_writer = VectorFileWriter(
            self.file_io, self._vector_field,
            path_producer=lambda: self.path_factory().vector_bin_path(self.partition, self.bucket),
            target_file_size=target_file_size,
        )

    def _process_data(self, data: pa.RecordBatch) -> pa.Table:
        # 1. 剥离 VECTOR 列
        vector_col = data.column(self._vector_column)
        # 2. 写 .vector.bin + 生成 descriptor 字节
        descriptors: List = []
        for i in range(data.num_rows):
            if vector_col.is_valid(i):
                row_vec = vector_col[i].as_py()  # list[float]
                desc = self._vector_writer.write_vector(row_vec)
                descriptors.append(desc.serialize())
            else:
                descriptors.append(None)
        # 3. 替换列
        new_col = pa.array(descriptors, type=pa.binary())
        new_schema = pa.schema([
            f if f.name != self._vector_column else pa.field(f.name, pa.binary(), nullable=f.nullable)
            for f in data.schema
        ])
        new_batch = pa.RecordBatch.from_arrays(
            [c if data.schema.field(idx).name != self._vector_column else new_col
             for idx, c in enumerate(data.columns)],
            schema=new_schema,
        )
        # 4. 走父类 KV 处理（加系统字段、排序）
        return super()._process_data(new_batch)

    def prepare_commit(self) -> List[DataFileMeta]:
        main_files = super().prepare_commit()
        vector_paths = self._vector_writer.close()
        if vector_paths and main_files:
            rel_paths = [self.file_io.to_relative_path(p) for p in vector_paths] \
                if hasattr(self.file_io, "to_relative_path") else \
                [os.path.basename(p) for p in vector_paths]
            enriched = []
            for m in main_files:
                enriched.append(m._replace(extra_files=list(m.extra_files) + rel_paths)
                                if hasattr(m, "_replace") else
                                DataFileMeta.create(**{**m.__dict__,
                                                       "extra_files": list(m.extra_files) + rel_paths}))
            return enriched
        return main_files

    def abort(self):
        try:
            self._vector_writer.abort()
        finally:
            super().abort()
```

**注意**：`DataFileMeta` 的不可变性需要确认，实际以 `paimon-python/pypaimon/manifest/schema/data_file_meta.py:31-59` 的实现为准。若是 `@dataclass` 且非 frozen，直接 `m.extra_files.extend(rel_paths)` 更简单。

**Step 5.3 跑绿 + commit**：`[python] Add VectorColumnFamilyDataWriter`

---

### Task 6: 在 `FileStoreWrite._create_data_writer` 路由 VCF

**Files:**
- Modify: `paimon-python/pypaimon/write/file_store_write.py`
- Test: 复用 Task 5 的 E2E 测试

**Step 6.1 修改 `_create_data_writer`（L60-106）**，优先级 `VCF > blob > PK > append`：

```python
def _create_data_writer(self, partition, bucket):
    options = self.options
    merge_mode = self._resolve_merge_mode(options)   # 原有逻辑抽出

    # NEW VCF 分支
    if options.vector_column_family_enabled() and self.table.is_primary_key_table:
        vector_cols = options.vector_column_family_columns() or \
            [f.name for f in self.table.fields if isinstance(f.type, VectorType)]
        if len(vector_cols) != 1:
            raise ValueError(f"VCF needs exactly one vector column, got {vector_cols}")
        return VectorColumnFamilyDataWriter(
            self.table, partition, bucket, self.max_seq_number, options,
            write_cols=self.write_cols, merge_mode=merge_mode,
            vector_column=vector_cols[0],
            target_file_size=options.vector_column_family_target_file_size(),
        )

    if self._has_blob_columns():
        return DataBlobWriter(...)
    if self.table.is_primary_key_table:
        return KeyValueDataWriter(...)
    return AppendOnlyDataWriter(...)
```

**Step 6.2 跑测试（Task 5 的 E2E 就是 Task 6 的验收）。commit**：`[python] Route VCF writer in FileStoreWrite`

---

### Task 7: VCF 端到端与互通测试

**Files:**
- Create: `paimon-python/pypaimon/tests/table/vector_cf_table_test.py`

覆盖场景：
1. **基础写读** — 已在 Task 5 覆盖，搬到这里更 E2E。
2. **Rolling** — 通过 `vector-column-family.target-file-size=64` 强制每条滚一个文件，校验 `len(glob('*.vector.bin')) == rows`。
3. **Null 向量** — 含 null，主文件对应行为 null；读回也为 null。
4. **Write 多批** — 多次 `tw.write_arrow`，最终 descriptor 指向正确 row_index。
5. **extra_files 记录** — 用 `SnapshotManager` 或 `ManifestListManager` 读 manifest 确认 `extra_files` 非空。
6. **Java 互通 fixture**（如 fixture 存在则跑，否则 skip）— 与 Phase 1 `JavaVectorCfInteropTest` 合并。

**commit**：`[python] Add end-to-end VCF write/read tests`

---

### Task 8: `VectorFileGarbageCollector`

**Files:**
- Create: `paimon-python/pypaimon/operation/vector_file_garbage_collector.py`
- Create: `paimon-python/pypaimon/tests/operation/vector_file_gc_test.py`

**职责**（对齐 Java，单机版）：
- 构造：`VectorFileGarbageCollector(table)`
- `collect_all_vector_files_from_fs() -> Set[str]`：递归扫表目录找 `.vector.bin`
- `collect_referenced(table) -> Set[str]`：用 pypaimon 的 ReadBuilder 投影到 VECTOR 列，迭代所有行提取 `descriptor.file_path` 的 basename
- `delete_unreferenced(all, referenced) -> int`：集合差集 → `file_io.delete_quietly`
- `gc() -> int`：一把梭

**Step 8.1 测试骨架**：

```python
def test_gc_deletes_unreferenced(tmp_path):
    # 1. 建 VCF PK 表，写 3 行
    # 2. 手工 touch 一个 orphan.vector.bin
    # 3. 跑 gc() 返回 1
    # 4. orphan 被删，live 文件完好
```

**Step 8.2 实现**（~80 行，模仿 Java）。

**commit**：`[python] Add VectorFileGarbageCollector`

---

### Task 9: 文档与回归

**Files:**
- Modify: `paimon-python/CLAUDE.md`（VCF 写入段落）
- Modify: `paimon-python/README.md`（VCF quickstart）
- 回归：`pytest pypaimon/tests/ -q`（确认 Phase 1 测试仍全绿、新测通过）
- lint：`flake8 --config=dev/cfg.ini pypaimon/`

**README 追加**示例：

```markdown
## Vector Column Family (VCF)

For embedding-heavy PK tables, enable VCF to keep vector bytes out of the main Parquet files.
Vector bytes live in append-only `.vector.bin` files referenced by in-line `VectorDescriptor`
bytes, saving compaction I/O for scalar-only updates.

```python
schema = Schema.from_pyarrow_schema(
    pa_schema, primary_keys=["id"],
    options={
        "bucket": "1",
        "vector-column-family.enabled": "true",
        "vector-column-family.target-file-size": "128mb",  # optional
    },
)
```

Reads are transparent: `FormatPyArrowReader` resolves descriptors back into `fixed_size_list`
columns (see phase 1 design doc).
```

**commit**：`[python] Document VCF write path and quickstart`

---

## Verification

- 每 Task 后：
  - 运行对应单测 / E2E
  - 确认 `git diff` 只涉及该 Task 的文件
  - 确认 flake8 通过
- 完成后全量：`pytest pypaimon/tests/ -q`
- **跨语言字节互通**（可选，需 Java 环境）：
  1. pypaimon 写入带 VCF 的表到 `/tmp/vcf_wh`
  2. 在 Java 里 `Catalog.create("filesystem", {"warehouse": "/tmp/vcf_wh"}).getTable("db.vcf_t")`，使用 VectorRef 读出向量，校验字节与原始 `pa.Table` 完全一致
  3. 反向：Java 写，pypaimon `FormatPyArrowReader` 读，比较

---

## Out of Scope / Phase 3 TODO

1. **partial-update merge-fn 的 null-skip 跨 commit 保留**：Java 能让 scalar-only update（vector=null）不覆盖先前 descriptor；pypaimon 当前 merge engine 只有 deduplicate，跨 commit null 会覆盖非 null，VCF 下表现为 vector 丢失。需要：
   - 实现 write-time PartialUpdate merge-fn，或
   - 在 VectorColumnFamilyDataWriter 写入前感知 null vector 并保留上一次 commit 的 descriptor（需读侧配合）
2. **append-only 表的 VCF**：当前只走 PK 表路径
3. **ORC 主格式 + VCF**：现 Phase 2 只验证 Parquet 主文件
4. **多向量列**：Java 也限制为 1；未来放开时 Python 需同步
5. **AccelerateIndex 读路径**：Phase 1 里 pypaimon 已有 `globalindex/lumina/`，但与 VCF 分离存储的跨路径尚未贯通
6. **分布式 GC**：Python 版 GC 先做单机扫描；大表需配合 Ray / 分布式执行
7. **Commit-time compaction with VCF**：vector 文件 append-only，永不参与 compaction；GC 即清理机制，但 `.vector.bin` 内旧行无法回收（dead-row inside live file）→ 后续若需压缩再设计
8. **Python writer 的 descriptor 做 placeholder（VectorRef 等价物）**：当前按批 materialize `descriptor.serialize()` bytes；Java 里走 `VectorRef` + `FallbackMappingRow` 零拷贝，pypaimon 以 pyarrow Array 替换列达到同等效果但多一次 Python list 转换，若发现热点可换成 pyarrow buffer 级拼装

---

## Self-Review

- [x] Phase 2 scope 明确（写入侧），不重复 Phase 1 read
- [x] 每 Task 都有 TDD 步骤 + 代码骨架（非 TBD 占位）
- [x] 字节兼容性通过 `VectorDescriptor` + `VectorFileWriter` 的对齐字节布局锁定（Phase 1 已有 Java golden vector 测试，Phase 2 新增 round-trip）
- [x] 命名一致：`vector_column_family_*` options、`VectorColumnFamilyDataWriter`、`VectorFileWriter`、`vector_bin_path`、`.vector.bin`
- [x] 用户记忆均已考虑（TDD、license header、plan materialize、`[python]` commit prefix）
- [x] Out of Scope 清晰；下阶段 TODO 列出具体续作点
