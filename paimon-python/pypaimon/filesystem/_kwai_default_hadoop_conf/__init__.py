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
