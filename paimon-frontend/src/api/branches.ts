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
import type { BranchInfo } from './types';

export async function listBranches(
  database: string,
  table: string
): Promise<BranchInfo[]> {
  const prefix = getCurrentPrefix();
  const resp = await apiClient.get(
    `/${prefix}/databases/${database}/tables/${table}/branches`
  );
  const names: string[] = resp.data.branches ?? [];

  // Fetch details for each branch
  const details = await Promise.all(
    names.map((name) =>
      apiClient
        .get(`/${prefix}/databases/${database}/tables/${table}/branches/${name}`)
        .then((r) => r.data as BranchInfo)
    )
  );

  // API doesn't return 'main' in the list — always prepend it
  const mainResp = await apiClient.get(
    `/${prefix}/databases/${database}/tables/${table}/branches/main`
  );
  const mainBranch = mainResp.data as BranchInfo;

  return [mainBranch, ...details];
}
