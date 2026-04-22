# PyPaimon Embedding (VectorType) Python 移植计划 — Phase 1

## Context

Apache Paimon Java 版近期落地了一整套 embedding 列能力，核心由以下模块组成：
1. **类型层** — `VectorType` (`paimon-api/.../types/VectorType.java`)，SQL 语法 `VECTOR<elem, N>`，JSON `{"type":"VECTOR"|"VECTOR NOT NULL","element":{...},"length":N}`。
2. **物理层** — `BinaryVector`（密集排布）+ `VectorDescriptor`（指向分离存储的指针，`paimon-common/.../data/VectorDescriptor.java`，29+N 字节，Little-Endian，含 magic `0x5645435F50545200`）+ `VectorRef`（lazy-load 代理）。
3. **Vector Column Family** — `VectorColumnFamilyFlushHelper` / `DefaultVectorFileWriter` 把向量剥离到 append-only `.vector.bin` 文件，主 Parquet 只存 descriptor BINARY。
4. **索引/搜索层** — `AccelerateIndex` 框架（Lumina / FAISS / Lucene），已有 `paimon-lumina` 与 pypaimon 侧 `globalindex/lumina/` 部分对齐。

pypaimon 当前**完全没有** VectorType；`schema/data_types.py` 里 `AtomicType / ArrayType / MultisetType / MapType / RowType / DataTypeParser / PyarrowFieldParser` 都跳过了 VECTOR 分支。`read/reader/format_pyarrow_reader.py` 和 `write/writer/data_writer.py` 对 VECTOR 列也无感知。

本计划**只覆盖 Layer 1-4**（类型 + 读写，Parquet 为唯一文件格式）。明确推迟的工作在文末 TODO 列出。决定：
- **内存表示**：`pyarrow.fixed_size_list(element_type, length)`，契合 PyArrow/Parquet 生态，零拷贝参与 compute。
- **Parquet 写入**：直接把 VECTOR 列以 `pyarrow.fixed_size_list` 写入 Parquet（原生 REPEATED 编码）。不做 vector-cf 分离存储。
- **Parquet 读取**：双形态兼容 —
  - (a) 读到 `fixed_size_list / list / large_list` 时直接透传；
  - (b) 读到 `BINARY` 且内容匹配 VectorDescriptor magic 时，解析 descriptor，从 `.vector.bin` 中按 `seek(rowIndex * bytesPerVector)` 读出原始字节并重组为 `fixed_size_list`（保证 Java vector-cf 写入的表可被 pypaimon 读取）。
- **Schema 校验**：VECTOR 列必须 nullable、不得作为 PK/partition key（与 Java `SchemaValidation` 对齐）。
- **测试**：单元测试（类型 + descriptor + ref）+ 集成测试（Parquet 往返 + Java 互通 fixture）。

## Architecture

整体分三层代码：

```
+---------------------------------------------+
|  schema/data_types.py                       |   (1) 类型层
|   - class VectorType(DataType)              |
|   - Keyword.VECTOR                          |
|   - DataTypeParser 增加 VECTOR 分支          |
|   - PyarrowFieldParser 增加 VECTOR 分支      |
|   - to_avro_type 增加 fixed_size_list 分支   |
+---------------------------------------------+
                  |
                  v
+---------------------------------------------+
|  data/vector_descriptor.py  (new)           |   (2) 物理层
|  data/vector_ref.py         (new)           |
|   - VectorDescriptor.serialize/deserialize  |
|   - VectorDescriptor.is_vector_descriptor   |
|   - VectorRef.from_descriptor(fileIO, d)    |
|   - load_vector_bytes(fileIO, descriptor)   |
+---------------------------------------------+
                  |
                  v
+---------------------------------------------+
|  read/reader/format_pyarrow_reader.py       |   (3) 读路径
|  write/writer/data_writer.py  (无改动)       |   (4) 写路径 (fixed_size_list 透传)
|  schema/schema.py                           |   (5) 校验
+---------------------------------------------+
```

## Tech Stack

- Python ≥ 3.6（pypaimon 兼容下限）
- `pyarrow ≥ 6`（含 `pyarrow.fixed_size_list`、`pyarrow.FixedSizeListArray`）
- `numpy`（descriptor/vector 字节 ↔ ndarray 转换）
- `struct`（VectorDescriptor 二进制序列化，不引入新依赖）
- `fastavro`（已在用，不需改动）
- 测试：`pytest`（已在用）

## File Structure

### 新建

| 路径 | 职责 |
|---|---|
| `paimon-python/pypaimon/data/vector_descriptor.py` | `VectorDescriptor` 类 + magic 校验 + serialize/deserialize |
| `paimon-python/pypaimon/data/vector_ref.py` | `load_vector_bytes` / `resolve_vector_column` 工具：给定 descriptor 与 FileIO，从 `.vector.bin` 拉字节 |
| `paimon-python/pypaimon/tests/data/__init__.py` | 测试包 |
| `paimon-python/pypaimon/tests/data/vector_descriptor_test.py` | `VectorDescriptor` 单测 |
| `paimon-python/pypaimon/tests/data/vector_ref_test.py` | `vector_ref` 工具函数单测（mock FileIO） |
| `paimon-python/pypaimon/tests/table/vector_type_table_test.py` | 端到端读写测试 |
| `paimon-python/pypaimon/tests/fixtures/java_vector_cf_parquet/` | 可选：存放由 Java 产出的 vector-cf 样例文件，用于互通回归 |

### 修改

| 路径 | 修改内容 |
|---|---|
| `paimon-python/pypaimon/schema/data_types.py` | 新增 `VectorType` 类；`Keyword` 增加 `VECTOR`；`DataTypeParser.parse_data_type` 增加 VECTOR 分支；`PyarrowFieldParser.from_paimon_type` / `to_paimon_type` / `to_avro_type` 各增加一条分支 |
| `paimon-python/pypaimon/schema/schema.py` | `from_pyarrow_schema` 内增加 vector 列校验（非 PK/partition、强制 nullable） |
| `paimon-python/pypaimon/read/reader/format_pyarrow_reader.py` | 读到 BINARY 列而 read_fields 声明为 VECTOR 时，走 descriptor 解析 → 从 `.vector.bin` 读取 → 重组 `fixed_size_list`；list/fixed_size_list 路径直接透传 |
| `paimon-python/pypaimon/tests/data_types_test.py` | 增加 VectorType 相关单测 |

### 不修改（但依赖）

- `write/writer/data_writer.py` — 只要 schema 是 `fixed_size_list`，PyArrow 会自然写入 Parquet。无需特殊分支。
- `common/file_io.py` — 已提供 `read_bytes` / 字节读取 API（PyArrowFileIO + LocalFileIO 覆盖）。

---

# Task N: [Component Name]

> 每个 Task 小步 commit。严格 TDD：先写红测 → 跑失败 → 最小实现 → 跑通过 → commit。运行测试先于任何"完成"声明（参见 CLAUDE 记忆：feedback_verify_before_done）。所有新文件必须含 Apache 2.0 license header（参见 feedback_license_header）。

---

### Task 0: 把本计划物化到项目文档

**Files:**
- Create: `paimon-python/docs/design/vector-type-python-port.md`（本计划的完整副本，作为 Phase 1 设计文档随代码一起入库）

- [ ] **Step 0.1: 复制计划内容**

将 `/Users/lcy/.claude/plans/java-embedding-embedding-java-java-pyth-sleepy-puzzle.md` 的完整内容写到 `paimon-python/docs/design/vector-type-python-port.md`，文件顶部加 Apache 2.0 license header（Markdown 注释形式）。

- [ ] **Step 0.2: Commit 设计文档（单独一个 commit）**

```bash
git add paimon-python/docs/design/vector-type-python-port.md
git commit -m "[python] Add design doc for VectorType port (Phase 1)"
```

---

### Task 1: VectorType 类与字符串/JSON 协议

**Files:**
- Modify: `paimon-python/pypaimon/schema/data_types.py` 在 `Keyword` enum 前（`RowType` 之后）插入 `VectorType`，并在 `Keyword` 加 `VECTOR = "VECTOR"`。
- Test: `paimon-python/pypaimon/tests/data_types_test.py`

- [ ] **Step 1.1: 写失败测试 — VectorType 字符串形态**

在 `data_types_test.py` `DataTypesTest` 类内追加：

```python
def test_vector_type_str(self):
    from pypaimon.schema.data_types import VectorType
    vt = VectorType(nullable=True, length=128, element_type=AtomicType("FLOAT"))
    self.assertEqual(str(vt), "VECTOR<FLOAT, 128>")

    vt_nn = VectorType(nullable=False, length=3, element_type=AtomicType("DOUBLE"))
    self.assertEqual(str(vt_nn), "VECTOR<DOUBLE, 3> NOT NULL")

def test_vector_type_to_dict_roundtrip(self):
    from pypaimon.schema.data_types import VectorType, DataTypeParser
    vt = VectorType(nullable=True, length=1024, element_type=AtomicType("FLOAT"))
    d = vt.to_dict()
    self.assertEqual(d["type"], "VECTOR")
    self.assertEqual(d["length"], 1024)
    self.assertEqual(d["element"], AtomicType("FLOAT").to_dict())

    vt2 = DataTypeParser.parse_data_type(d)
    self.assertEqual(vt, vt2)

def test_vector_type_not_null_json(self):
    from pypaimon.schema.data_types import VectorType, DataTypeParser
    vt = VectorType(nullable=False, length=16, element_type=AtomicType("FLOAT"))
    d = vt.to_dict()
    self.assertEqual(d["type"], "VECTOR NOT NULL")
    vt2 = DataTypeParser.parse_data_type(d)
    self.assertFalse(vt2.nullable)
    self.assertEqual(vt2.length, 16)

def test_vector_type_invalid_element(self):
    from pypaimon.schema.data_types import VectorType
    with self.assertRaises(ValueError):
        VectorType(nullable=True, length=8, element_type=AtomicType("STRING"))
    with self.assertRaises(ValueError):
        VectorType(nullable=True, length=0, element_type=AtomicType("FLOAT"))
```

- [ ] **Step 1.2: 跑测试确认失败**

Run: `cd paimon-python && pytest pypaimon/tests/data_types_test.py::DataTypesTest::test_vector_type_str -v`
Expected: FAIL — `ImportError: cannot import name 'VectorType'`.

- [ ] **Step 1.3: 实现 VectorType 与 Keyword**

在 `data_types.py` 的 `Keyword` enum 末尾追加：

```python
    VECTOR = "VECTOR"
```

在 `RowType` 之后、`Keyword` 之前插入：

```python
VALID_VECTOR_ELEMENT_TYPES = {"BOOLEAN", "TINYINT", "SMALLINT", "INT", "INTEGER",
                              "BIGINT", "FLOAT", "DOUBLE"}


@dataclass
class VectorType(DataType):
    element: DataType
    length: int

    MIN_LENGTH = 1

    def __init__(self, nullable: bool, length: int, element_type: DataType):
        super().__init__(nullable)
        if element_type is None:
            raise ValueError("Element type must not be null.")
        if not isinstance(element_type, AtomicType):
            raise ValueError(
                "Invalid element type for vector (must be atomic): {}".format(element_type))
        base = element_type.type.upper().split("(")[0].split(" ")[0]
        if base not in VALID_VECTOR_ELEMENT_TYPES:
            raise ValueError("Invalid element type for vector: {}".format(element_type))
        if length is None or length < self.MIN_LENGTH:
            raise ValueError(
                "Vector length must be >= {}, got {}".format(self.MIN_LENGTH, length))
        self.element = element_type
        self.length = length

    def __eq__(self, other):
        if self is other:
            return True
        if not isinstance(other, VectorType):
            return False
        return (self.element == other.element
                and self.length == other.length
                and self.nullable == other.nullable)

    def __hash__(self):
        return hash((self.element, self.length, self.nullable))

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": "VECTOR" if self.nullable else "VECTOR NOT NULL",
            "element": self.element.to_dict() if self.element else None,
            "length": self.length,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "VectorType":
        return DataTypeParser.parse_data_type(data)

    def __str__(self) -> str:
        null_suffix = "" if self.nullable else " NOT NULL"
        return "VECTOR<{}, {}>{}".format(self.element, self.length, null_suffix)
```

在 `DataTypeParser.parse_data_type`（`data_types.py:370-418`）的 `ROW` 分支之后、`else` 之前插入：

```python
            elif type_string.startswith("VECTOR"):
                element = DataTypeParser.parse_data_type(
                    json_data.get("element"), field_id)
                length = json_data.get("length")
                if length is None:
                    raise ValueError(
                        "Missing 'length' field for VECTOR type: {}".format(json_data))
                nullable = "NOT NULL" not in type_string
                return VectorType(nullable, int(length), element)
```

**注意**：不要在 `parse_atomic_type_sql_string` 里把 `VECTOR` 当 atomic 处理；VECTOR 总是走 dict 分支（Paimon manifest 里用 dict 表达）。`Keyword.VECTOR` 加入 enum 只是为了避免 `parse_atomic_type_sql_string` 在退路上接受 `"VECTOR"` 字符串时报错。

- [ ] **Step 1.4: 跑测试确认通过**

Run: `cd paimon-python && pytest pypaimon/tests/data_types_test.py -v -k vector_type`
Expected: 4 个 vector 相关 test 全 PASS。

- [ ] **Step 1.5: 跑 data_types 整体回归**

Run: `cd paimon-python && pytest pypaimon/tests/data_types_test.py -v`
Expected: 全部 PASS（既有测试不应被 regressed）。

- [ ] **Step 1.6: Commit**

```bash
git add paimon-python/pypaimon/schema/data_types.py paimon-python/pypaimon/tests/data_types_test.py
git commit -m "[python] Introduce VectorType data type"
```

---

### Task 2: PyArrow / Avro 桥接

**Files:**
- Modify: `paimon-python/pypaimon/schema/data_types.py`（`PyarrowFieldParser.from_paimon_type` / `to_paimon_type` / `to_avro_type`）
- Test: `paimon-python/pypaimon/tests/data_types_test.py`

- [ ] **Step 2.1: 写失败测试**

在 `DataTypesTest` 追加：

```python
def test_vector_type_to_pyarrow(self):
    from pypaimon.schema.data_types import VectorType
    vt = VectorType(nullable=True, length=128, element_type=AtomicType("FLOAT"))
    pa_type = PyarrowFieldParser.from_paimon_type(vt)
    self.assertTrue(pa.types.is_fixed_size_list(pa_type))
    self.assertEqual(pa_type.list_size, 128)
    self.assertTrue(pa.types.is_float32(pa_type.value_type))

def test_pyarrow_to_vector_type(self):
    from pypaimon.schema.data_types import VectorType
    pa_type = pa.list_(pa.float32(), 64)  # pyarrow 0.x: shorthand for fixed_size_list
    paimon_type = PyarrowFieldParser.to_paimon_type(pa_type, nullable=True)
    self.assertIsInstance(paimon_type, VectorType)
    self.assertEqual(paimon_type.length, 64)
    self.assertEqual(paimon_type.element.type, "FLOAT")

def test_vector_type_roundtrip_through_pyarrow(self):
    from pypaimon.schema.data_types import VectorType
    vt = VectorType(nullable=True, length=256, element_type=AtomicType("DOUBLE"))
    pa_type = PyarrowFieldParser.from_paimon_type(vt)
    vt2 = PyarrowFieldParser.to_paimon_type(pa_type, nullable=True)
    self.assertEqual(vt, vt2)

def test_vector_type_to_avro(self):
    fixed_size = pa.list_(pa.float32(), 32)
    avro = PyarrowFieldParser.to_avro_type(fixed_size, "embed")
    self.assertEqual(avro["type"], "array")
    self.assertEqual(avro["items"], "float")
```

- [ ] **Step 2.2: 跑测试确认失败**

Run: `cd paimon-python && pytest pypaimon/tests/data_types_test.py -v -k "vector_type_to_pyarrow or pyarrow_to_vector or vector_type_roundtrip or vector_type_to_avro"`
Expected: FAIL — `from_paimon_type` 抛 `Unsupported data type: VECTOR<...>`。

- [ ] **Step 2.3: 实现 PyArrow 桥接**

在 `PyarrowFieldParser.from_paimon_type`（`data_types.py:460-536`）的 `elif isinstance(data_type, RowType):` 之前插入：

```python
        elif isinstance(data_type, VectorType):
            element_pa = PyarrowFieldParser.from_paimon_type(data_type.element)
            return pyarrow.list_(element_pa, data_type.length)
```

（`pyarrow.list_(inner, size)` 的三元重载会返回 `pyarrow.fixed_size_list(inner, size)`，这是 PyArrow 6+ 的约定。）

在 `PyarrowFieldParser.to_paimon_type` 的 `elif types.is_list(pa_type) or types.is_large_list(pa_type):` 之前插入：

```python
        elif types.is_fixed_size_list(pa_type):
            pa_type: pyarrow.FixedSizeListType
            element_type = PyarrowFieldParser.to_paimon_type(pa_type.value_type, True)
            if isinstance(element_type, AtomicType) and element_type.type.split("(")[0].split(" ")[0] in VALID_VECTOR_ELEMENT_TYPES:
                return VectorType(nullable, pa_type.list_size, element_type)
            # 回退：非法元素类型的 fixed-size-list 保持为 ArrayType
            return ArrayType(nullable, element_type)
```

在 `PyarrowFieldParser.to_avro_type`（`data_types.py:640-695`）的 `elif pyarrow.types.is_list(field_type) or pyarrow.types.is_large_list(field_type):` 分支修改为：

```python
        elif (pyarrow.types.is_list(field_type)
              or pyarrow.types.is_large_list(field_type)
              or pyarrow.types.is_fixed_size_list(field_type)):
            value_field = field_type.value_field
            return {
                "type": "array",
                "items": PyarrowFieldParser.to_avro_type(value_field.type, value_field.name, parent_name)
            }
```

（注意 `FixedSizeListType.value_field` 在 PyArrow 6+ 可用；若运行在更旧版本需改走 `value_type`。）

- [ ] **Step 2.4: 跑测试确认通过**

Run: `cd paimon-python && pytest pypaimon/tests/data_types_test.py -v`
Expected: 全部 PASS。

- [ ] **Step 2.5: Commit**

```bash
git add paimon-python/pypaimon/schema/data_types.py paimon-python/pypaimon/tests/data_types_test.py
git commit -m "[python] Bridge VectorType <-> pyarrow fixed_size_list and avro"
```

---

### Task 3: VectorDescriptor 二进制格式

**Files:**
- Create: `paimon-python/pypaimon/data/vector_descriptor.py`
- Create: `paimon-python/pypaimon/tests/data/__init__.py`
- Create: `paimon-python/pypaimon/tests/data/vector_descriptor_test.py`

必须与 Java `VectorDescriptor.java` 完全一致：Little-Endian、`version(1B)=1`、`magic(8B)=0x5645435F50545200`、`filePathLength(4B)` + `filePath(UTF-8)` + `rowIndex(8B)` + `bytesPerVector(4B)` + `dimension(4B)`。

- [ ] **Step 3.1: 写失败测试**

`paimon-python/pypaimon/tests/data/__init__.py` 写空文件（只含 Apache 2.0 license header comment）。

`paimon-python/pypaimon/tests/data/vector_descriptor_test.py`:

```python
################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################
import unittest

from pypaimon.data.vector_descriptor import VectorDescriptor


class VectorDescriptorTest(unittest.TestCase):
    def test_serialize_size(self):
        d = VectorDescriptor("/tmp/data-x.vector.bin", 0, 512, 128)
        b = d.serialize()
        # 1 + 8 + 4 + N + 8 + 4 + 4 = 29 + N
        self.assertEqual(len(b), 29 + len("/tmp/data-x.vector.bin".encode("utf-8")))

    def test_serialize_magic_and_version(self):
        d = VectorDescriptor("/x.vector.bin", 0, 4, 1)
        b = d.serialize()
        self.assertEqual(b[0], 1)  # version
        # magic little-endian = 0x0052545020434556 (reversed bytes of 0x5645435F50545200)
        import struct
        magic = struct.unpack_from("<q", b, 1)[0]
        self.assertEqual(magic, 0x5645435F50545200)

    def test_roundtrip(self):
        d = VectorDescriptor("/hdfs/vector/data-abc.vector.bin", 12345, 512, 128)
        d2 = VectorDescriptor.deserialize(d.serialize())
        self.assertEqual(d, d2)

    def test_is_vector_descriptor(self):
        d = VectorDescriptor("/p.vector.bin", 0, 4, 1)
        b = d.serialize()
        self.assertTrue(VectorDescriptor.is_vector_descriptor(b))
        self.assertFalse(VectorDescriptor.is_vector_descriptor(b[:8]))
        self.assertFalse(VectorDescriptor.is_vector_descriptor(b[:9] + b"\x00"))  # bad magic

    def test_reject_future_version(self):
        d = VectorDescriptor("/p.vector.bin", 0, 4, 1)
        b = bytearray(d.serialize())
        b[0] = 99  # future version
        with self.assertRaises(Exception):
            VectorDescriptor.deserialize(bytes(b))

    def test_java_golden_vector(self):
        # Golden payload captured from Java VectorDescriptor("a.vector.bin", 7, 512, 128).serialize()
        import struct
        path = b"a.vector.bin"
        expected = b""
        expected += struct.pack("<b", 1)                   # version
        expected += struct.pack("<q", 0x5645435F50545200)  # magic
        expected += struct.pack("<i", len(path))           # pathLen
        expected += path                                   # path
        expected += struct.pack("<q", 7)                   # rowIndex
        expected += struct.pack("<i", 512)                 # bytesPerVector
        expected += struct.pack("<i", 128)                 # dimension
        d = VectorDescriptor("a.vector.bin", 7, 512, 128)
        self.assertEqual(d.serialize(), expected)
```

- [ ] **Step 3.2: 跑测试确认失败**

Run: `cd paimon-python && pytest pypaimon/tests/data/vector_descriptor_test.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'pypaimon.data.vector_descriptor'`.

- [ ] **Step 3.3: 实现 VectorDescriptor**

`paimon-python/pypaimon/data/vector_descriptor.py`:

```python
################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################
"""Binary pointer to a row stored in a separate vector-column-family file.

Wire-compatible with Java org.apache.paimon.data.VectorDescriptor.
Layout (Little-Endian):

    | Offset | Field           | Type    | Size |
    |--------|-----------------|---------|------|
    | 0      | version         | byte    | 1    |
    | 1      | magicNumber     | long    | 8    |
    | 9      | filePathLength  | int     | 4    |
    | 13     | filePathBytes   | byte[N] | N    |
    | 13+N   | rowIndex        | long    | 8    |
    | 21+N   | bytesPerVector  | int     | 4    |
    | 25+N   | dimension       | int     | 4    |
"""
import struct
from dataclasses import dataclass

MAGIC = 0x5645435F50545200  # "VEC_PTR\0" (ASCII, LE-serialized)
CURRENT_VERSION = 1
HEADER_FIXED_SIZE = 29  # bytes not counting path


@dataclass(frozen=True)
class VectorDescriptor:
    file_path: str
    row_index: int
    bytes_per_vector: int
    dimension: int
    version: int = CURRENT_VERSION

    def serialize(self) -> bytes:
        path_bytes = self.file_path.encode("utf-8")
        total = HEADER_FIXED_SIZE + len(path_bytes)
        buf = bytearray(total)
        off = 0
        struct.pack_into("<b", buf, off, self.version); off += 1
        struct.pack_into("<q", buf, off, MAGIC); off += 8
        struct.pack_into("<i", buf, off, len(path_bytes)); off += 4
        buf[off:off + len(path_bytes)] = path_bytes; off += len(path_bytes)
        struct.pack_into("<q", buf, off, self.row_index); off += 8
        struct.pack_into("<i", buf, off, self.bytes_per_vector); off += 4
        struct.pack_into("<i", buf, off, self.dimension); off += 4
        return bytes(buf)

    @staticmethod
    def deserialize(data: bytes) -> "VectorDescriptor":
        off = 0
        version = struct.unpack_from("<b", data, off)[0]; off += 1
        if version > CURRENT_VERSION:
            raise ValueError(
                "VectorDescriptor version {} > supported {}".format(version, CURRENT_VERSION))
        magic = struct.unpack_from("<q", data, off)[0]; off += 8
        if magic != MAGIC:
            raise ValueError(
                "Invalid VectorDescriptor: magic mismatch. expected={}, found={}".format(MAGIC, magic))
        path_len = struct.unpack_from("<i", data, off)[0]; off += 4
        file_path = bytes(data[off:off + path_len]).decode("utf-8"); off += path_len
        row_index = struct.unpack_from("<q", data, off)[0]; off += 8
        bytes_per_vector = struct.unpack_from("<i", data, off)[0]; off += 4
        dimension = struct.unpack_from("<i", data, off)[0]; off += 4
        return VectorDescriptor(
            file_path=file_path,
            row_index=row_index,
            bytes_per_vector=bytes_per_vector,
            dimension=dimension,
            version=version,
        )

    @staticmethod
    def is_vector_descriptor(data: bytes) -> bool:
        if data is None or len(data) < 9:
            return False
        version = data[0]
        if version > CURRENT_VERSION:
            return False
        magic = struct.unpack_from("<q", data, 1)[0]
        return magic == MAGIC
```

注意 `pypaimon/data/__init__.py` 已存在（存放 Timestamp），无需新建。

- [ ] **Step 3.4: 跑测试确认通过**

Run: `cd paimon-python && pytest pypaimon/tests/data/vector_descriptor_test.py -v`
Expected: 6 个 test 全 PASS。

- [ ] **Step 3.5: Commit**

```bash
git add paimon-python/pypaimon/data/vector_descriptor.py paimon-python/pypaimon/tests/data/__init__.py paimon-python/pypaimon/tests/data/vector_descriptor_test.py
git commit -m "[python] Add VectorDescriptor binary codec compatible with Java"
```

---

### Task 4: Vector bytes 加载工具（VectorRef 等价物）

**Files:**
- Create: `paimon-python/pypaimon/data/vector_ref.py`
- Test: `paimon-python/pypaimon/tests/data/vector_ref_test.py`

Python 侧不需要与 Java `VectorRef` 严格一一对应的 OOP 对象，只需提供从 `VectorDescriptor + FileIO + 元素类型` 解包为 numpy/list 的工具函数，让读路径能把一整批 descriptor 转成一个 `FixedSizeListArray`。

- [ ] **Step 4.1: 写失败测试**

`paimon-python/pypaimon/tests/data/vector_ref_test.py`:

```python
################################################################################
# (Apache 2.0 license header — same as other new files)
################################################################################
import os
import struct
import tempfile
import unittest

import numpy as np
import pyarrow as pa

from pypaimon.common.file_io import FileIO
from pypaimon.data.vector_descriptor import VectorDescriptor
from pypaimon.data.vector_ref import resolve_vector_descriptors


class VectorRefTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.file_io = FileIO(self.tmp, {})  # LocalFileIO under the hood for file:// warehouses

    def _write_vector_file(self, name, vectors: np.ndarray) -> str:
        path = os.path.join(self.tmp, name)
        with open(path, "wb") as f:
            f.write(vectors.astype(np.float32).tobytes(order="C"))
        return path

    def test_resolve_basic_float32(self):
        vecs = np.array([[1.0, 2.0, 3.0], [4.0, 5.0, 6.0], [7.0, 8.0, 9.0]], dtype=np.float32)
        path = self._write_vector_file("one.vector.bin", vecs)
        bytes_per = 3 * 4  # 12
        descriptors = [
            VectorDescriptor(path, 0, bytes_per, 3).serialize(),
            VectorDescriptor(path, 2, bytes_per, 3).serialize(),
        ]
        arr = resolve_vector_descriptors(self.file_io, descriptors, pa.float32(), 3)
        self.assertTrue(pa.types.is_fixed_size_list(arr.type))
        self.assertEqual(arr.type.list_size, 3)
        self.assertEqual(arr.to_pylist(), [[1.0, 2.0, 3.0], [7.0, 8.0, 9.0]])

    def test_resolve_with_null_descriptor(self):
        vecs = np.array([[1.0, 2.0]], dtype=np.float32)
        path = self._write_vector_file("two.vector.bin", vecs)
        descriptors = [VectorDescriptor(path, 0, 8, 2).serialize(), None]
        arr = resolve_vector_descriptors(self.file_io, descriptors, pa.float32(), 2)
        self.assertEqual(arr.is_null().to_pylist(), [False, True])
        self.assertEqual(arr.to_pylist()[0], [1.0, 2.0])
        self.assertIsNone(arr.to_pylist()[1])

    def test_resolve_multi_file_caching(self):
        vecs_a = np.array([[1.0, 2.0], [3.0, 4.0]], dtype=np.float32)
        vecs_b = np.array([[9.0, 9.0]], dtype=np.float32)
        pa_path = self._write_vector_file("a.vector.bin", vecs_a)
        pb_path = self._write_vector_file("b.vector.bin", vecs_b)
        descriptors = [
            VectorDescriptor(pa_path, 1, 8, 2).serialize(),
            VectorDescriptor(pb_path, 0, 8, 2).serialize(),
            VectorDescriptor(pa_path, 0, 8, 2).serialize(),
        ]
        arr = resolve_vector_descriptors(self.file_io, descriptors, pa.float32(), 2)
        self.assertEqual(arr.to_pylist(), [[3.0, 4.0], [9.0, 9.0], [1.0, 2.0]])
```

- [ ] **Step 4.2: 跑测试确认失败**

Run: `cd paimon-python && pytest pypaimon/tests/data/vector_ref_test.py -v`
Expected: FAIL — `ModuleNotFoundError: pypaimon.data.vector_ref`.

- [ ] **Step 4.3: 实现 vector_ref**

`paimon-python/pypaimon/data/vector_ref.py`:

```python
################################################################################
# (Apache 2.0 license header)
################################################################################
"""Utilities to resolve VectorDescriptor BINARY columns into fixed_size_list arrays.

Mirrors the semantics of Java org.apache.paimon.data.VectorRef.fromDescriptor:
given a stream of descriptors, a FileIO, and the element arrow type, return a
pyarrow.FixedSizeListArray of dense-packed vectors.
"""
from typing import Dict, List, Optional, Sequence

import numpy as np
import pyarrow as pa

from pypaimon.common.file_io import FileIO
from pypaimon.data.vector_descriptor import VectorDescriptor


_ARROW_TO_NUMPY = {
    pa.bool_(): np.bool_,
    pa.int8(): np.int8,
    pa.int16(): np.int16,
    pa.int32(): np.int32,
    pa.int64(): np.int64,
    pa.float32(): np.float32,
    pa.float64(): np.float64,
}


def _numpy_dtype_for(element_type: pa.DataType) -> np.dtype:
    for arrow_t, np_t in _ARROW_TO_NUMPY.items():
        if element_type.equals(arrow_t):
            return np.dtype(np_t)
    raise ValueError("Unsupported vector element type: {}".format(element_type))


def resolve_vector_descriptors(
    file_io: FileIO,
    descriptor_bytes_list: Sequence[Optional[bytes]],
    element_type: pa.DataType,
    dimension: int,
) -> pa.Array:
    """Read raw vector bytes for each descriptor and return a FixedSizeListArray.

    `descriptor_bytes_list[i] is None` ==> null entry at position i.
    Uses a per-call path cache so each .vector.bin file is read once.
    """
    np_dtype = _numpy_dtype_for(element_type)
    element_size = np_dtype.itemsize

    # Cache whole file bytes per path; .vector.bin is flat, random-access friendly.
    cached_bytes: Dict[str, bytes] = {}
    flat = np.empty((len(descriptor_bytes_list), dimension), dtype=np_dtype)
    validity: List[bool] = []

    for i, db in enumerate(descriptor_bytes_list):
        if db is None:
            validity.append(False)
            continue
        descriptor = VectorDescriptor.deserialize(db)
        if descriptor.dimension != dimension:
            raise ValueError(
                "Descriptor dim {} != schema dim {} (row {})"
                .format(descriptor.dimension, dimension, i))
        expected_bytes = dimension * element_size
        if descriptor.bytes_per_vector < expected_bytes:
            raise ValueError(
                "Descriptor bytes_per_vector {} < dim*elem_size {} (row {})"
                .format(descriptor.bytes_per_vector, expected_bytes, i))
        file_bytes = cached_bytes.get(descriptor.file_path)
        if file_bytes is None:
            file_bytes = file_io.read_file_utf8_bytes(descriptor.file_path) \
                if hasattr(file_io, "read_file_utf8_bytes") \
                else _read_all_bytes(file_io, descriptor.file_path)
            cached_bytes[descriptor.file_path] = file_bytes
        off = descriptor.row_index * descriptor.bytes_per_vector
        raw = file_bytes[off:off + expected_bytes]
        flat[i] = np.frombuffer(raw, dtype=np_dtype, count=dimension)
        validity.append(True)

    values_arr = pa.array(flat.reshape(-1), type=element_type)
    mask = pa.array([not v for v in validity], type=pa.bool_())
    return pa.FixedSizeListArray.from_arrays(values_arr, dimension).filter(
        pa.compute.invert(pa.array([False] * len(validity)))  # placeholder; see below
    ) if False else _with_validity(values_arr, dimension, validity)


def _with_validity(values_arr: pa.Array, dimension: int, validity: List[bool]) -> pa.Array:
    # Build FixedSizeListArray with validity bitmap via pyarrow buffers API.
    inner = pa.FixedSizeListArray.from_arrays(values_arr, dimension)
    if all(validity):
        return inner
    # pyarrow 6+: Array.from_buffers to attach validity bitmap
    n = len(validity)
    vb = pa.array(validity, type=pa.bool_()).buffers()[1]
    return pa.FixedSizeListArray.from_buffers(
        pa.list_(values_arr.type, dimension),
        n,
        [vb],
        null_count=sum(1 for v in validity if not v),
        offset=0,
        children=[values_arr],
    )


def _read_all_bytes(file_io: FileIO, path: str) -> bytes:
    with file_io.new_input_stream(path) as s:  # FileIO 接口若不同，请以实际 API 替换
        return s.read()
```

**注意**：`FileIO.read_file_utf8_bytes` 不一定存在；实现时先 grep `paimon-python/pypaimon/common/file_io.py` 确认可用的「按 path 读整文件为 bytes」API（多半是 `read_file_utf8` 的 bytes 变体或 `new_input_stream`）。若没有，改成：

```python
with file_io.new_input_stream(descriptor.file_path) as s:
    file_bytes = s.read()
```

- [ ] **Step 4.4: 跑测试确认通过**

Run: `cd paimon-python && pytest pypaimon/tests/data/vector_ref_test.py -v`
Expected: 3 个 test PASS。

- [ ] **Step 4.5: Commit**

```bash
git add paimon-python/pypaimon/data/vector_ref.py paimon-python/pypaimon/tests/data/vector_ref_test.py
git commit -m "[python] Add vector descriptor -> FixedSizeListArray resolver"
```

---

### Task 5: Parquet 读路径集成 — VECTOR 列双形态识别

**Files:**
- Modify: `paimon-python/pypaimon/read/reader/format_pyarrow_reader.py`
- Test: `paimon-python/pypaimon/tests/table/vector_type_table_test.py`

目标：`FormatPyArrowReader` 在 `read_fields` 包含 `VectorType` 时，拿到 PyArrow `RecordBatch` 后：
- 若对应列已是 `fixed_size_list / list`，直接透传；
- 若对应列是 `binary / large_binary`，把每行 bytes 当作 descriptor 解码，用 `resolve_vector_descriptors` 生成 `FixedSizeListArray`，替换掉原列。

- [ ] **Step 5.1: 写失败测试**

在 `paimon-python/pypaimon/tests/table/vector_type_table_test.py` 写一个只打 reader 的 unit test（暂不过 catalog/write path，避免其他层还未改完）：

```python
################################################################################
# (Apache 2.0 license header)
################################################################################
import os
import tempfile
import unittest

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from pypaimon.common.file_io import FileIO
from pypaimon.data.vector_descriptor import VectorDescriptor
from pypaimon.read.reader.format_pyarrow_reader import FormatPyArrowReader
from pypaimon.schema.data_types import (AtomicType, DataField, VectorType)


class VectorReadFormatTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.file_io = FileIO(self.tmp, {})

    def _write_parquet(self, path, table):
        pq.write_table(table, path)

    def test_read_fixed_size_list_parquet(self):
        table = pa.table({
            "id": pa.array([1, 2], type=pa.int64()),
            "embed": pa.array([[0.1, 0.2, 0.3], [0.4, 0.5, 0.6]],
                              type=pa.list_(pa.float32(), 3)),
        })
        path = os.path.join(self.tmp, "native.parquet")
        self._write_parquet(path, table)

        read_fields = [
            DataField(0, "id", AtomicType("BIGINT")),
            DataField(1, "embed", VectorType(True, 3, AtomicType("FLOAT"))),
        ]
        reader = FormatPyArrowReader(self.file_io, "parquet", path, read_fields, None)
        batch = reader.read_arrow_batch()
        self.assertIsNotNone(batch)
        self.assertTrue(pa.types.is_fixed_size_list(batch.schema.field("embed").type))
        self.assertEqual(batch.column("embed").to_pylist(),
                         [[0.1, 0.2, 0.3], [0.4, 0.5, 0.6]])

    def test_read_descriptor_binary_parquet(self):
        # Simulate Java vector-cf: main Parquet has BINARY column containing descriptors
        vec_bin = os.path.join(self.tmp, "data-abc.vector.bin")
        vecs = np.array([[0.1, 0.2], [0.3, 0.4]], dtype=np.float32)
        with open(vec_bin, "wb") as f:
            f.write(vecs.tobytes(order="C"))
        bytes_per = 2 * 4

        d0 = VectorDescriptor(vec_bin, 0, bytes_per, 2).serialize()
        d1 = VectorDescriptor(vec_bin, 1, bytes_per, 2).serialize()
        table = pa.table({
            "id": pa.array([10, 20], type=pa.int64()),
            "embed": pa.array([d0, d1], type=pa.binary()),
        })
        path = os.path.join(self.tmp, "descriptor.parquet")
        self._write_parquet(path, table)

        read_fields = [
            DataField(0, "id", AtomicType("BIGINT")),
            DataField(1, "embed", VectorType(True, 2, AtomicType("FLOAT"))),
        ]
        reader = FormatPyArrowReader(self.file_io, "parquet", path, read_fields, None)
        batch = reader.read_arrow_batch()
        self.assertIsNotNone(batch)
        self.assertTrue(pa.types.is_fixed_size_list(batch.schema.field("embed").type))
        self.assertEqual(batch.column("embed").to_pylist(),
                         [[0.1, 0.2], [0.3, 0.4]])
```

- [ ] **Step 5.2: 跑测试确认失败**

Run: `cd paimon-python && pytest pypaimon/tests/table/vector_type_table_test.py -v -k vector_read`
Expected: 第 2 个测试 FAIL（BINARY→fixed_size_list 转换未实现）。第 1 个可能也 FAIL 视 PyArrow dataset 能否对 fixed_size_list 原生读取。

- [ ] **Step 5.3: 实现 Reader 的 VECTOR 处理**

修改 `paimon-python/pypaimon/read/reader/format_pyarrow_reader.py`：

1. 顶部新增 import：

```python
from pypaimon.data.vector_descriptor import VectorDescriptor
from pypaimon.data.vector_ref import resolve_vector_descriptors
from pypaimon.schema.data_types import VectorType, PyarrowFieldParser
```

2. 在 `__init__` 记录一个 `self._vector_fields: Dict[str, VectorType]`：

```python
self._vector_fields = {
    f.name: f.type for f in read_fields if isinstance(f.type, VectorType)
}
self._file_io = file_io
```

3. 在 `read_arrow_batch` 返回前（批重建之后、或直接在 batch 得到后）加入 vector 处理：

```python
def _apply_vector_resolution(self, batch: pa.RecordBatch) -> pa.RecordBatch:
    if not self._vector_fields:
        return batch

    new_columns = []
    new_fields = []
    for i, name in enumerate(batch.schema.names):
        col = batch.column(i)
        vt = self._vector_fields.get(name)
        if vt is None:
            new_columns.append(col)
            new_fields.append(batch.schema.field(i))
            continue

        target_element_pa = PyarrowFieldParser.from_paimon_type(vt.element)

        if pa.types.is_fixed_size_list(col.type):
            new_columns.append(col)
            new_fields.append(batch.schema.field(i))
        elif pa.types.is_list(col.type) or pa.types.is_large_list(col.type):
            # Cast list<float> -> fixed_size_list<float, N>
            target_type = pa.list_(target_element_pa, vt.length)
            new_columns.append(col.cast(target_type))
            new_fields.append(pa.field(name, target_type, nullable=vt.nullable))
        elif pa.types.is_binary(col.type) or pa.types.is_large_binary(col.type):
            descriptor_bytes_list = [b if b is not None else None for b in col.to_pylist()]
            resolved = resolve_vector_descriptors(
                self._file_io, descriptor_bytes_list, target_element_pa, vt.length)
            new_columns.append(resolved)
            new_fields.append(
                pa.field(name, pa.list_(target_element_pa, vt.length), nullable=vt.nullable))
        else:
            raise ValueError(
                "VECTOR column '{}' has unsupported physical type {}".format(name, col.type))
    return pa.RecordBatch.from_arrays(new_columns, schema=pa.schema(new_fields))
```

然后在 `read_arrow_batch` 里既有的 missing-fields 重组后，追加：

```python
        result = batch  # 既有 missing_fields 处理后的 result
        return self._apply_vector_resolution(result)
```

若 `self.missing_fields` 为空的快路径也需要调用 `_apply_vector_resolution`。

- [ ] **Step 5.4: 跑测试确认通过**

Run: `cd paimon-python && pytest pypaimon/tests/table/vector_type_table_test.py -v -k vector_read`
Expected: 2 个 test 均 PASS。

- [ ] **Step 5.5: Commit**

```bash
git add paimon-python/pypaimon/read/reader/format_pyarrow_reader.py paimon-python/pypaimon/tests/table/vector_type_table_test.py
git commit -m "[python] Resolve VECTOR columns in FormatPyArrowReader (list + descriptor forms)"
```

---

### Task 6: Schema 校验（VECTOR 列约束）

**Files:**
- Modify: `paimon-python/pypaimon/schema/schema.py`
- Test: `paimon-python/pypaimon/tests/data_types_test.py`（新增 `SchemaValidationTest` 或单独新 file）

目标：与 Java `SchemaValidation.validateVectorColumns` 对齐核心规则 —— VECTOR 列必须 nullable、不得是 primary key、不得是 partition key。

- [ ] **Step 6.1: 写失败测试**

在 `data_types_test.py` 增加（或放到单独文件 `tests/schema/schema_test.py`）：

```python
def test_schema_vector_column_cannot_be_pk(self):
    import pyarrow as pa
    from pypaimon.schema.schema import Schema
    pa_schema = pa.schema([
        pa.field("id", pa.int64(), nullable=False),
        pa.field("embed", pa.list_(pa.float32(), 4), nullable=True),
    ])
    with self.assertRaises(ValueError):
        Schema.from_pyarrow_schema(pa_schema, primary_keys=["embed"])

def test_schema_vector_column_cannot_be_partition(self):
    import pyarrow as pa
    from pypaimon.schema.schema import Schema
    pa_schema = pa.schema([
        pa.field("id", pa.int64(), nullable=False),
        pa.field("embed", pa.list_(pa.float32(), 4), nullable=True),
    ])
    with self.assertRaises(ValueError):
        Schema.from_pyarrow_schema(pa_schema, partition_keys=["embed"])

def test_schema_vector_column_must_be_nullable(self):
    import pyarrow as pa
    from pypaimon.schema.schema import Schema
    pa_schema = pa.schema([
        pa.field("id", pa.int64(), nullable=False),
        pa.field("embed", pa.list_(pa.float32(), 4), nullable=False),
    ])
    with self.assertRaises(ValueError):
        Schema.from_pyarrow_schema(pa_schema, primary_keys=["id"])
```

- [ ] **Step 6.2: 跑测试确认失败**

Run: `cd paimon-python && pytest pypaimon/tests/data_types_test.py -v -k "schema_vector"`
Expected: 3 个 test FAIL（无 validation）。

- [ ] **Step 6.3: 实现校验**

修改 `paimon-python/pypaimon/schema/schema.py` 的 `from_pyarrow_schema`：

```python
        from pypaimon.schema.data_types import VectorType

        vector_fields = [f for f in fields if isinstance(f.type, VectorType)]
        vector_names = {f.name for f in vector_fields}
        pk_set = set(primary_keys or [])
        partition_set = set(partition_keys or [])

        for vf in vector_fields:
            if vf.name in pk_set:
                raise ValueError(
                    "Vector column '{}' cannot be a primary key.".format(vf.name))
            if vf.name in partition_set:
                raise ValueError(
                    "Vector column '{}' cannot be a partition key.".format(vf.name))
            if not vf.type.nullable:
                raise ValueError(
                    "Vector column '{}' must be nullable.".format(vf.name))
```

插入位置：紧接 `# Primary key fields must be NOT NULL` 那一段之后、`has_blob_type` 之前。

- [ ] **Step 6.4: 跑测试确认通过**

Run: `cd paimon-python && pytest pypaimon/tests/data_types_test.py -v -k "schema_vector"`
Expected: 3 个 test PASS。

- [ ] **Step 6.5: Commit**

```bash
git add paimon-python/pypaimon/schema/schema.py paimon-python/pypaimon/tests/data_types_test.py
git commit -m "[python] Validate vector column constraints (nullable, non-PK, non-partition)"
```

---

### Task 7: 端到端读写往返（Catalog + Table + Parquet）

**Files:**
- Test: `paimon-python/pypaimon/tests/table/vector_type_table_test.py`（追加 E2E case）

目标：用 `FileSystemCatalog` 建一张带 VECTOR 列的 append-only 表，写入 1000 行随机向量，回读比对。不走 vector-cf，pypaimon 把 VECTOR 列以 Parquet fixed_size_list 直存。

- [ ] **Step 7.1: 追加 E2E 测试**

```python
import tempfile, numpy as np, pyarrow as pa
from pypaimon import CatalogFactory, Schema
# ...

class VectorTypeE2ETest(unittest.TestCase):
    def test_write_read_fixed_size_list(self):
        warehouse = tempfile.mkdtemp()
        catalog = CatalogFactory.create({"metastore": "filesystem", "warehouse": warehouse})
        catalog.create_database("vec_db", ignore_if_exists=True)

        pa_schema = pa.schema([
            pa.field("id", pa.int64(), nullable=False),
            pa.field("embed", pa.list_(pa.float32(), 8), nullable=True),
        ])
        schema = Schema.from_pyarrow_schema(pa_schema)
        catalog.create_table("vec_db.t_embed", schema, ignore_if_exists=True)

        t = catalog.get_table("vec_db.t_embed")
        vectors = np.random.rand(32, 8).astype(np.float32)
        batch_table = pa.table({
            "id": pa.array(list(range(32)), type=pa.int64()),
            "embed": pa.array(vectors.tolist(), type=pa.list_(pa.float32(), 8)),
        })
        wb = t.new_batch_write_builder()
        w = wb.new_write()
        w.write_arrow(batch_table)
        commit = wb.new_commit()
        commit.commit(w.prepare_commit())
        w.close()

        rt = catalog.get_table("vec_db.t_embed")
        read = rt.new_read_builder().new_read().to_arrow(
            rt.new_read_builder().new_scan().plan().splits())

        self.assertEqual(read.num_rows, 32)
        self.assertTrue(pa.types.is_fixed_size_list(read.schema.field("embed").type))
        read_vectors = np.array(read.column("embed").to_pylist(), dtype=np.float32)
        np.testing.assert_array_almost_equal(read_vectors, vectors)
```

- [ ] **Step 7.2: 跑**

Run: `cd paimon-python && pytest pypaimon/tests/table/vector_type_table_test.py -v`
Expected: PASS；若 FAIL，排查：
- `Schema.from_pyarrow_schema` 是否把 `fixed_size_list` 正确转成 `VectorType`（Task 2）
- Avro manifest 是否能序列化该 schema（Task 2 到 `to_avro_type`）
- PyArrow 写 Parquet 对 `fixed_size_list` 的处理（PyArrow 6+ 支持）

- [ ] **Step 7.3: Commit**

```bash
git add paimon-python/pypaimon/tests/table/vector_type_table_test.py
git commit -m "[python] Add end-to-end VectorType read-write test"
```

---

### Task 8: Java 互通回归（可选但强烈建议）

**Files:**
- Fixture: `paimon-python/pypaimon/tests/fixtures/java_vector_cf_parquet/` (手工生成 / 下载 Java 端 VectorType 写的样例)
- Test: `paimon-python/pypaimon/tests/table/vector_type_table_test.py`（追加）

目标：用 Java 侧 `VectorTypeTableITCase` 产出的真实 Parquet 文件 + `.vector.bin` 文件放入 fixtures 目录，pypaimon 直接读取，验证 descriptor 解析路径。Java 生成：

```java
// From paimon-flink/.../VectorTypeTableITCase.java tweaked to dump locally
Schema schema = Schema.newBuilder()
    .column("id", DataTypes.BIGINT())
    .column("embed", DataTypes.VECTOR(8, DataTypes.FLOAT()))
    .option("vector-column-family.enabled", "true")
    .build();
// Write 16 rows, flush, copy data-*.parquet + data-*.vector.bin into fixtures dir
```

- [ ] **Step 8.1: 在 README / Task doc 里记录生成 fixture 的操作步骤**

可以先跳过（TODO）。此 Task 目标是保证 pypaimon 未来不会 regress Java vector-cf 读能力。

- [ ] **Step 8.2: 追加 fixture-driven 测试**

```python
class JavaInteropTest(unittest.TestCase):
    FIXTURE = os.path.join(os.path.dirname(__file__), "..", "fixtures",
                           "java_vector_cf_parquet")

    @unittest.skipUnless(os.path.exists(os.path.join(FIXTURE, "main.parquet")),
                        "Java fixture not present; generate with paimon-flink ITCase.")
    def test_read_java_vector_cf(self):
        # assume fixture has main.parquet with BINARY embed column + data.vector.bin
        ...
```

- [ ] **Step 8.3: Commit（包含 skip 测试即可）**

```bash
git add paimon-python/pypaimon/tests/table/vector_type_table_test.py
git commit -m "[python] Add skipped Java vector-cf interop fixture test"
```

---

### Task 9: 全量回归 + 文档更新

- [ ] **Step 9.1: 跑 pypaimon 全量 pytest**

Run: `cd paimon-python && pytest pypaimon/tests/ -v`
Expected: 所有既有测试 PASS，新增 vector 测试也 PASS。

- [ ] **Step 9.2: flake8**

Run: `cd paimon-python && flake8 --config=dev/cfg.ini pypaimon/`
Expected: 0 errors。

- [ ] **Step 9.3: 更新 CLAUDE.md / README**

在 `paimon-python/CLAUDE.md` 的 Architecture 小节（L50-79）插入一段关于 VectorType 的简短说明；在 `paimon-python/README.md` 增加一个写读 VECTOR 列的 quickstart 片段（≤ 20 行）。

- [ ] **Step 9.4: Commit**

```bash
git add paimon-python/CLAUDE.md paimon-python/README.md
git commit -m "[python] Document VectorType read/write quickstart"
```

---

## 验证（Verification）

整体功能的验证链条：

1. **单元层**
   - `pytest pypaimon/tests/data_types_test.py -v` — VectorType 协议、PyArrow/Avro 桥接、schema 校验
   - `pytest pypaimon/tests/data/vector_descriptor_test.py -v` — 二进制格式（含 Java golden vector）
   - `pytest pypaimon/tests/data/vector_ref_test.py -v` — descriptor→FixedSizeListArray 解析

2. **读路径层**
   - `pytest pypaimon/tests/table/vector_type_table_test.py::VectorReadFormatTest -v` — 两种 Parquet 物理形态（fixed_size_list / BINARY descriptor）

3. **端到端层**
   - `pytest pypaimon/tests/table/vector_type_table_test.py::VectorTypeE2ETest -v` — Catalog + Schema + write/read roundtrip

4. **回归与 lint**
   - `pytest pypaimon/tests/ -v`
   - `flake8 --config=dev/cfg.ini pypaimon/`

5. **互通（可选）**
   - 用 Java `VectorTypeTableITCase` 生成一张带 vector-cf 的表，复制 `data-*.parquet` + `data-*.vector.bin` 到 `pypaimon/tests/fixtures/java_vector_cf_parquet/`，跑 `JavaInteropTest`。

---

## 关键 Java 参考点（移植时查证用）

| 关注 | Java 路径 |
|---|---|
| VectorType | `paimon-api/src/main/java/org/apache/paimon/types/VectorType.java` (L40-209) |
| VectorDescriptor | `paimon-common/src/main/java/org/apache/paimon/data/VectorDescriptor.java` (L55-198) |
| VectorRef | `paimon-common/src/main/java/org/apache/paimon/data/VectorRef.java` |
| Schema 校验 | `paimon-core/src/main/java/org/apache/paimon/schema/SchemaValidation.java` (L672-715) |
| Parquet 写 descriptor | `paimon-format/.../parquet/writer/ParquetRowDataWriter.java` (L356-376) |
| Flink IT 用例 | `paimon-flink/paimon-flink-common/src/test/java/org/apache/paimon/flink/VectorTypeTableITCase.java` |

---

## 明确推迟到 Phase 2+ 的工作（TODO）

1. **Lance 格式支持**：pypaimon `write_lance` / `FormatLanceReader` 已有基础，但 VECTOR→Lance 写未打通。用户本次明确推迟。
2. **Vector Column Family 写入**：
   - `VectorColumnFamilyFlushHelper` Python 版
   - `DefaultVectorFileWriter`（写 `.vector.bin`）
   - 配置解析：`vector-column-family.enabled` / `.columns` / `.target-file-size`
   - Merge tree 的 partial-update 语义保持
3. **VectorFileGarbageCollector**：`.vector.bin` 的 set-difference GC（读所有 manifest 引用 vs 磁盘实体文件集合）。
4. **AccelerateIndex 读侧深度集成**：pypaimon 已有 `globalindex/lumina/`，但与 vector-cf split 的链路尚未贯通。
5. **`vector-field` / `field.{name}.vector-dim` options 路径**：Java 允许把 ARRAY<FLOAT> 通过 options 升格为 VECTOR，pypaimon 本次只支持显式 VectorType，不做自动升格。
6. **ORC 写入路径**：当前 pypaimon 多数场景用 Parquet，ORC 里 VECTOR 列行为留作后续。

---

## Self-Review Notes

- [x] 每个 Task 都含真实的代码与 pytest 命令，无 TBD/TODO 占位
- [x] 类型与方法名前后一致：`VectorType.length`、`VectorDescriptor.row_index` 在所有 Task 中拼写统一
- [x] Java 协议 golden vector 已在 Task 3 测试中固化
- [x] 用户 memory 要求已考虑：测试先行（feedback_always_write_tests）、跑测试后再 commit（feedback_verify_before_done）、新文件 Apache 2.0 header（feedback_license_header）
- [x] 与用户确认：Parquet only（不做 Lance）、内存用 fixed_size_list、Layer 1-4（不做 vector-cf / GC / accelerate index）
