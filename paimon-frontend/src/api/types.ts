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

export interface PagedResponse<T> {
  data: T[];
  nextPageToken?: string;
}

export interface DatabaseInfo {
  name: string;
  properties?: Record<string, string>;
  comment?: string;
  owner?: string;
  createdAt?: number;
  createdBy?: string;
  updatedAt?: number;
  updatedBy?: string;
}

export interface TableInfo {
  name: string;
  schemaId: number;
  schema: SchemaInfo;
  path?: string;
  tableId?: number;
  isExternal?: boolean;
  owner?: string;
  createdAt?: number;
  createdBy?: string;
  updatedAt?: number;
  updatedBy?: string;
}

export interface SchemaInfo {
  fields: FieldInfo[];
  partitionKeys: string[];
  primaryKeys: string[];
  options: Record<string, string>;
  comment?: string;
}

export type DataTypeNode = string | {
  type: string;
  element?: DataTypeNode;
  key?: DataTypeNode;
  value?: DataTypeNode;
  fields?: DataFieldNode[];
  dimension?: number;
};

export interface DataFieldNode {
  id: number;
  name: string;
  type: DataTypeNode;
  description?: string;
}

export interface FieldInfo {
  id: number;
  name: string;
  type: DataTypeNode;
  description?: string;
}

export interface SnapshotInfo {
  id: number;
  schemaId: number;
  commitKind: string;
  commitIdentifier: number;
  totalRecordCount: number;
  deltaRecordCount: number;
  changelogRecordCount: number;
  totalFileCount?: number;
  deltaFileCount?: number;
  changelogFileCount?: number;
  watermark: number;
  timeMillis: number;
  properties?: Record<string, string>;
}

export interface BranchInfo {
  branch: string;
  latestSnapshotId?: number;
  latestSchemaId?: number;
}

export interface TagInfo {
  tagName: string;
  snapshot?: {
    id: number;
    schemaId: number;
    commitKind: string;
    totalRecordCount: number;
    deltaRecordCount: number;
    timeMillis: number;
  };
  tagCreateTime?: number;
}

export interface PartitionInfo {
  spec: Record<string, string>;
  recordCount: number;
  fileSizeInBytes: number;
  fileCount: number;
  lastFileCreationTime?: number;
}

export interface SchemaHistoryEntry {
  schemaId: number;
  schema: {
    fields: FieldInfo[];
    partitionKeys: string[];
    primaryKeys: string[];
    options: Record<string, string>;
    comment?: string;
    timeMillis?: number;
  };
  timeMillis?: number;
}

export interface ConsumerInfo {
  consumerId: string;
  nextSnapshotId: number;
}

export interface ErrorResponse {
  resourceType?: string;
  resourceName?: string;
  message: string;
  code: number;
}

// Write operation request types

export interface CreateDatabaseRequest {
  name: string;
  options?: Record<string, string>;
}

export interface AlterDatabaseRequest {
  removals?: string[];
  updates?: Record<string, string>;
}

export interface AlterDatabaseResponse {
  removed: string[];
  updated: string[];
  missing: string[];
}

export interface TableIdentifier {
  database: string;
  object: string;
}

export interface CreateTableRequest {
  identifier: TableIdentifier;
  schema: {
    fields: CreateFieldInfo[];
    partitionKeys?: string[];
    primaryKeys?: string[];
    options?: Record<string, string>;
    comment?: string;
  };
}

export interface CreateFieldInfo {
  id: number;
  name: string;
  type: string;
  description?: string;
}

export type SchemaChange =
  | { action: 'addColumn'; fieldNames: string[]; dataType: string; comment?: string }
  | { action: 'dropColumn'; fieldNames: string[] }
  | { action: 'renameColumn'; fieldNames: string[]; newName: string }
  | { action: 'updateColumnType'; fieldNames: string[]; newDataType: string };

export interface AlterTableRequest {
  changes: SchemaChange[];
}

export interface RenameTableRequest {
  source: TableIdentifier;
  destination: TableIdentifier;
}

export interface CreateBranchRequest {
  branch: string;
  fromTag?: string;
  fromSnapshotId?: number;
}

export interface CreateTagRequest {
  tagName: string;
  snapshotId?: number;
  timeRetained?: string;
}
