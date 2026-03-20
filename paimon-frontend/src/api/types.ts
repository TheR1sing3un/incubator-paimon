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

export interface FieldInfo {
  id: number;
  name: string;
  type: string;
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
