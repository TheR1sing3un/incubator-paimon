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

import logging
from typing import List

from pypaimon.api.rest_exception import NotImplementedException
from pypaimon.catalog.catalog import Catalog
from pypaimon.common.identifier import Identifier

logger = logging.getLogger(__name__)
from pypaimon.snapshot.snapshot import Snapshot
from pypaimon.snapshot.snapshot_commit import (PartitionStatistics,
                                               SnapshotCommit)


class CatalogSnapshotCommit(SnapshotCommit):
    """A SnapshotCommit using Catalog to commit.

    When the REST server's underlying catalog does not support commitSnapshot
    (e.g. FileSystemCatalog returns 501), this class automatically falls back
    to RenamingSnapshotCommit for filesystem-based atomic commit.
    """

    def __init__(self, catalog: Catalog, identifier: Identifier, uuid: str,
                 fallback_commit: SnapshotCommit = None):
        """
        Initialize CatalogSnapshotCommit.

        Args:
            catalog: The catalog instance to use for committing
            identifier: The table identifier
            uuid: Optional table UUID for verification
            fallback_commit: Optional fallback SnapshotCommit (e.g. RenamingSnapshotCommit)
        """
        self.catalog = catalog
        self.identifier = identifier
        self.uuid = uuid
        self._fallback_commit = fallback_commit
        self._use_fallback = False

    def commit(self, snapshot: Snapshot, statistics: List[PartitionStatistics],
               committer=None, message=None) -> bool:
        if self._use_fallback and self._fallback_commit is not None:
            return self._fallback_commit.commit(snapshot, statistics, committer, message)

        if hasattr(self.catalog, 'commit_snapshot'):
            try:
                success = self.catalog.commit_snapshot(
                    self.identifier, self.uuid, snapshot, statistics, committer, message)
                if success:
                    logger.info("Catalog snapshot commit succeeded for %s, snapshot id %d",
                                self.identifier, snapshot.id)
                return success
            except NotImplementedException:
                # Server returned 501: underlying catalog doesn't support commitSnapshot.
                # Fall back to filesystem-based commit.
                if self._fallback_commit is not None:
                    logger.info("Catalog commitSnapshot not supported (501), "
                                "falling back to filesystem commit for %s", self.identifier)
                    self._use_fallback = True
                    return self._fallback_commit.commit(snapshot, statistics, committer, message)
                raise
        else:
            raise NotImplementedError(
                "The catalog does not support snapshot commits."
            )

    def close(self):
        """Close the catalog and release resources."""
        if self._fallback_commit is not None and hasattr(self._fallback_commit, 'close'):
            self._fallback_commit.close()
        if hasattr(self.catalog, 'close'):
            self.catalog.close()
