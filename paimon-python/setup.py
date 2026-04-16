##########################################################################
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
##########################################################################
from setuptools import find_packages, setup

VERSION = "1.4.902"

PACKAGES = find_packages(include=["pypaimon*"])


install_requires = [
    'cachetools>=4.2,<6; python_version=="3.6"',
    'cachetools>=5,<6; python_version>"3.6"',
    'dataclasses>=0.8; python_version < "3.7"',
    'fastavro>=1.4,<2',
    'fsspec>=2021.10,<2026; python_version<"3.8"',
    'fsspec>=2023,<2026; python_version>="3.8"',
    'packaging>=21,<26',
    'pandas>=1.1,<2; python_version < "3.7"',
    'pandas>=1.3,<3; python_version >= "3.7" and python_version < "3.9"',
    'pandas>=1.5,<3; python_version >= "3.9"',
    'polars>=0.9,<1; python_version<"3.8"',
    'polars>=1,<2; python_version>="3.8"',
    'pyarrow>=6,<7; python_version < "3.8"',
    'pyarrow>=16; python_version >= "3.8"',
    'pyroaring<=0.3.3; python_version < "3.7"',
    'pyroaring<=0.4.5; python_version == "3.7"',
    'pyroaring>=1.0.0; python_version >= "3.8"',
    'readerwriterlock>=1,<2',
    'zstandard>=0.19,<1',
    'cramjam>=1.3.0,<3; python_version>="3.7"',
    'pyyaml>=5.4,<7',
]

long_description = "See Apache Paimon Python API \
[Doc](https://paimon.apache.org/docs/master/pypaimon/python-api/) for usage."

setup(
    name="ks-pypaimon",
    version=VERSION,
    packages=PACKAGES,
    include_package_data=True,
    install_requires=install_requires,
    entry_points={
        'console_scripts': [
            'paimon=pypaimon.cli:main',
        ],
    },
    extras_require={
        'ray': [
            'ray>=2.10,<3; python_version>="3.7"',
        ],
        'duckdb': [
            'duckdb>=0.8.0; python_version>="3.7"',
        ],
        'daft': [
            'daft>=0.7,<1; python_version>="3.10"',
        ],
        'query-server': [
            'fastapi>=0.100,<1; python_version>="3.8"',
            'uvicorn[standard]>=0.20,<1; python_version>="3.8"',
            'duckdb>=0.8.0; python_version>="3.7"',
        ],
        'torch': [
            'torch',
        ],
        # faiss-cpu: optional for vector ANN index. 1.7.x has no wheel for 3.12+; 3.12+ use 1.10+.
        'faiss': [
            'faiss-cpu==1.7.2; python_version >= "3.6" and python_version < "3.7"',
            'faiss-cpu==1.7.4; python_version >= "3.7" and python_version < "3.12"',
            'faiss-cpu>=1.10,<2; python_version >= "3.12"',
        ],
        'oss': [
            'ossfs>=2021.8; python_version<"3.8"',
            'ossfs>=2023; python_version>="3.8"'
        ],
        'lance': [
            'pylance>=0.20,<1; python_version>="3.9"',
            'pylance>=0.10,<1; python_version>="3.8" and python_version<"3.9"',
            'faiss-cpu==1.7.2; python_version >= "3.6" and python_version < "3.7"',
            'faiss-cpu==1.7.4; python_version >= "3.7" and python_version < "3.12"',
            'faiss-cpu>=1.10,<2; python_version >= "3.12"',
        ],
    },
    description="Apache Paimon Python API",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="Apache Software Foundation",
    author_email="dev@paimon.apache.org",
    url="https://paimon.apache.org",
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
    python_requires=">=3.6",
)
