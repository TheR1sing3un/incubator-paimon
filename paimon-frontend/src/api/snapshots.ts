/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import apiClient, { getCurrentPrefix } from './client';
import { encodeBranchTable } from './tables';
import type { SnapshotInfo } from './types';

export async function listSnapshots(
  database: string,
  table: string,
  pageToken?: string,
  branch?: string
): Promise<{ snapshots: SnapshotInfo[]; nextPageToken?: string }> {
  const params: Record<string, string> = {};
  if (pageToken) params.pageToken = pageToken;
  const t = encodeBranchTable(table, branch);
  const resp = await apiClient.get(
    `/${getCurrentPrefix()}/databases/${database}/tables/${t}/snapshots`,
    { params }
  );
  return {
    snapshots: resp.data.snapshots ?? [],
    nextPageToken: resp.data.nextPageToken,
  };
}
