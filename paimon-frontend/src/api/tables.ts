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
import type {
  TableInfo,
  CreateTableRequest,
  AlterTableRequest,
  RenameTableRequest,
} from './types';

export function encodeBranchTable(table: string, branch?: string): string {
  if (!branch || branch === 'main') return table;
  return `${table}$branch_${branch}`;
}

export async function listTables(database: string): Promise<string[]> {
  const resp = await apiClient.get(`/${getCurrentPrefix()}/databases/${database}/tables`);
  return resp.data.tables ?? [];
}

export async function getTable(database: string, table: string, branch?: string): Promise<TableInfo> {
  const t = encodeBranchTable(table, branch);
  const resp = await apiClient.get(`/${getCurrentPrefix()}/databases/${database}/tables/${t}`);
  return resp.data;
}

export async function createTable(request: CreateTableRequest): Promise<void> {
  const database = request.identifier.database;
  await apiClient.post(`/${getCurrentPrefix()}/databases/${database}/tables`, request);
}

export async function dropTable(database: string, table: string): Promise<void> {
  await apiClient.delete(`/${getCurrentPrefix()}/databases/${database}/tables/${table}`);
}

export async function alterTable(
  database: string,
  table: string,
  request: AlterTableRequest
): Promise<void> {
  await apiClient.post(`/${getCurrentPrefix()}/databases/${database}/tables/${table}`, request);
}

export async function renameTable(request: RenameTableRequest): Promise<void> {
  await apiClient.post(`/${getCurrentPrefix()}/tables/rename`, request);
}
