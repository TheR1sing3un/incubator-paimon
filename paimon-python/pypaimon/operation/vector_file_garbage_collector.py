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
"""Set-difference garbage collector for .vector.bin files.

Mirrors Java ``org.apache.paimon.operation.VectorFileGarbageCollector``:
- Enumerate every ``.vector.bin`` present anywhere under the table directory.
- Collect the subset that is still referenced by a live DataFileMeta. In pypaimon
  this is more efficient than Java's distributed descriptor scan because
  ``DataFileMeta.extra_files`` records the vector bin names directly at commit time
  (see ``VectorColumnFamilyDataWriter.prepare_commit``).
- Delete the difference.
"""
import logging
import os
from typing import Dict, Set

import pyarrow.fs as pafs


logger = logging.getLogger(__name__)


class VectorFileGarbageCollector:
    VECTOR_BIN_SUFFIX = ".vector.bin"

    def __init__(self, table):
        self.table = table
        self.file_io = table.file_io
        self._root = table.path_factory().root()

    # -- Public API ----------------------------------------------------------

    def gc(self) -> int:
        """Walk the table, compute orphan set, delete it. Return delete count."""
        all_map = self.collect_all_vector_files_from_fs()
        referenced = self.collect_referenced()
        return self.delete_unreferenced(all_map, referenced)

    def collect_all_vector_files_from_fs(self) -> Dict[str, str]:
        """Return {basename: absolute_path} for every .vector.bin found under the table dir."""
        results: Dict[str, str] = {}
        self._walk(self._root, results)
        return results

    def collect_referenced(self) -> Set[str]:
        """Basenames of .vector.bin files referenced by at least one live DataFileMeta."""
        referenced: Set[str] = set()
        read_builder = self.table.new_read_builder()
        plan = read_builder.new_scan().plan()
        for split in plan.splits():
            for file_meta in getattr(split, "files", []) or []:
                extras = getattr(file_meta, "extra_files", None) or []
                for name in extras:
                    basename = os.path.basename(name)
                    if basename.endswith(self.VECTOR_BIN_SUFFIX):
                        referenced.add(basename)
        return referenced

    def delete_unreferenced(self,
                            all_map: Dict[str, str],
                            referenced: Set[str]) -> int:
        deleted = 0
        for basename, abs_path in all_map.items():
            if basename in referenced:
                continue
            try:
                self.file_io.delete_quietly(abs_path)
                deleted += 1
                logger.debug("Deleted unreferenced vector file: %s", abs_path)
            except Exception:
                logger.warning("Failed to delete vector file: %s", abs_path, exc_info=True)
        return deleted

    # -- Internals -----------------------------------------------------------

    def _walk(self, dir_path: str, out: Dict[str, str]) -> None:
        try:
            entries = self.file_io.list_status(dir_path)
        except Exception:
            logger.warning("Failed to list %s", dir_path, exc_info=True)
            return
        for entry in entries:
            path = entry.path
            if entry.type == pafs.FileType.Directory:
                # Skip the root's own entry if list_status returns it
                if path.rstrip("/") == dir_path.rstrip("/"):
                    continue
                self._walk(path, out)
            elif entry.type == pafs.FileType.File:
                basename = os.path.basename(path)
                if basename.endswith(self.VECTOR_BIN_SUFFIX):
                    out[basename] = path
