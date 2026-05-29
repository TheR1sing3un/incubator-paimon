# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Kuaishou-internal default Hadoop config.

Bundled with ks-pypaimon only; intentionally absent from the upstream
community build. When HADOOP_CONF_DIR is not set on a host,
HdfsNativeFileIO falls back to the xml here so the native client can
still talk to viewfs://hadoop-lt-cluster/ without further configuration.
"""

import pkgutil
import xml.etree.ElementTree as ET
from typing import Dict

_FILES = ("core-site.xml", "hdfs-site.xml", "mountTable.xml")


def load() -> Dict[str, str]:
    """Parse the bundled xml files into a flat {name: value} dict.

    Later files override earlier ones on key collision (which matches
    Hadoop's own load order). Malformed / missing entries are skipped
    silently so a broken bundled file never blocks client startup.
    """
    result: Dict[str, str] = {}
    for fname in _FILES:
        try:
            data = pkgutil.get_data(__name__, fname)
        except (FileNotFoundError, OSError):
            continue
        if not data:
            continue
        try:
            root = ET.fromstring(data)
        except ET.ParseError:
            continue
        for prop in root.findall("property"):
            name_el = prop.find("name")
            value_el = prop.find("value")
            if name_el is None or name_el.text is None:
                continue
            value = (
                value_el.text.strip()
                if value_el is not None and value_el.text
                else ""
            )
            result[name_el.text.strip()] = value
    return result
