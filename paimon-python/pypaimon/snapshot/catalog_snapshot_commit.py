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

import logging
from typing import List

from pypaimon.api.rest_exception import NotImplementedException
from pypaimon.catalog.catalog import Catalog
from pypaimon.common.identifier import Identifier
from pypaimon.snapshot.snapshot import Snapshot
from pypaimon.snapshot.snapshot_commit import (PartitionStatistics,
                                               SnapshotCommit)

logger = logging.getLogger(__name__)


class CatalogSnapshotCommit(SnapshotCommit):
    """A SnapshotCommit using Catalog to commit.

    When the REST server's underlying catalog does not support commitSnapshot
    (e.g. FileSystemCatalog returns 501), this class automatically falls back
    to ``fallback_commit`` (typically a RenamingSnapshotCommit) for
    filesystem-based atomic commit.
    """

    def __init__(self, catalog: Catalog, identifier: Identifier, uuid: str,
                 fallback_commit: 'SnapshotCommit' = None):
        """
        Initialize CatalogSnapshotCommit.

        Args:
            catalog: The catalog instance to use for committing
            identifier: The table identifier (already encodes branch in object name)
            uuid: Optional table UUID for verification
            fallback_commit: Optional fallback SnapshotCommit used when the
                REST server replies 501 (NotImplementedException) on
                commitSnapshot. Subsequent commits go straight to the
                fallback once the first 501 is observed.
        """
        self.catalog = catalog
        self.identifier = identifier
        self.uuid = uuid
        self._fallback_commit = fallback_commit
        self._use_fallback = False

    def commit(self, snapshot: Snapshot, statistics: List[PartitionStatistics],
               committer=None, message=None) -> bool:
        """
        Commit the snapshot using the catalog.

        Args:
            snapshot: The snapshot to commit
            statistics: List of partition statistics
            committer: Optional committer name attached to this commit.
            message: Optional commit message attached to this commit.

        Returns:
            True if commit was successful

        Raises:
            Exception: If commit fails
        """
        if self._use_fallback and self._fallback_commit is not None:
            return self._fallback_commit.commit(snapshot, statistics, committer, message)

        if hasattr(self.catalog, 'commit_snapshot'):
            try:
                success = self.catalog.commit_snapshot(
                    self.identifier, self.uuid, snapshot, statistics,
                    committer=committer, message=message,
                )
                if success:
                    logger.info(
                        "Catalog snapshot commit succeeded for %s, snapshot id %d",
                        self.identifier, snapshot.id)
                return success
            except NotImplementedException:
                if self._fallback_commit is not None:
                    logger.info(
                        "Catalog commitSnapshot not supported (501); "
                        "falling back to filesystem commit for %s",
                        self.identifier)
                    self._use_fallback = True
                    return self._fallback_commit.commit(
                        snapshot, statistics, committer, message)
                raise
        else:
            # Fallback for catalogs that don't support snapshot commits
            raise NotImplementedError(
                "The catalog does not support snapshot commits. "
                "The commit_snapshot method needs to be implemented in the catalog interface."
            )

    def close(self):
        """Close the catalog and release resources."""
        if self._fallback_commit is not None and hasattr(self._fallback_commit, 'close'):
            self._fallback_commit.close()
        if hasattr(self.catalog, 'close'):
            self.catalog.close()
