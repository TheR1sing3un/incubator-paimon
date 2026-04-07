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

// Mirror of pypaimon/query_server/dag/models.py — keep in sync by hand.

export type DagNodeType =
  | 'paimon_read'
  | 'sql'
  | 'paimon_write'
  | 'preview'
  | 'file_export';

export type RunnerMode = 'native' | 'ray';

export interface PaimonReadConfig {
  database: string;
  table: string;
  snapshot_id?: number | null;
  tag_name?: string | null;
  columns?: string[] | null;
  limit?: number | null;
}

export interface SqlConfig {
  sql: string;
}

export interface PaimonWriteConfig {
  database: string;
  table: string;
  mode: 'append' | 'overwrite';
  create_if_not_exists: boolean;
}

export interface PreviewConfig {
  // No config fields at present
}

export interface FileExportConfig {
  format: 'csv' | 'parquet';
  filename: string;
}

export type NodeConfig =
  | ({ type: 'paimon_read' } & PaimonReadConfig)
  | ({ type: 'sql' } & SqlConfig)
  | ({ type: 'paimon_write' } & PaimonWriteConfig)
  | ({ type: 'preview' } & PreviewConfig)
  | ({ type: 'file_export' } & FileExportConfig);

export interface DagNodeSpec {
  id: string;
  name: string;
  type: DagNodeType;
  config: Record<string, unknown>;
}

export interface DagEdgeSpec {
  from: string;
  to: string;
}

export interface DagRequest {
  runner: RunnerMode;
  ray_address?: string | null;
  catalog_options: Record<string, string>;
  max_preview_rows?: number;
  timeout_seconds?: number;
  preview_chunk_rows?: number;
  nodes: DagNodeSpec[];
  edges: DagEdgeSpec[];
}

// --- Wire events from the server ---

export type DagEvent =
  | { event: 'dag_started'; node_count: number }
  | {
      event: 'node_started';
      node_id: string;
      node_name: string;
      node_type: DagNodeType;
    }
  | {
      event: 'node_finished';
      node_id: string;
      elapsed_ms: number;
      row_count?: number;
      truncated?: boolean;
    }
  | {
      event: 'node_failed';
      node_id: string;
      error: string;
      error_type: string;
    }
  | {
      event: 'preview_schema';
      node_id: string;
      columns: Array<{ name: string; type: string }>;
    }
  | {
      event: 'preview_chunk';
      node_id: string;
      row_offset: number;
      rows: unknown[][];
    }
  | {
      event: 'file_export_header';
      node_id: string;
      filename: string;
      format: string;
      total_bytes: number;
    }
  | {
      event: 'file_export_chunk';
      node_id: string;
      offset: number;
      data_b64: string;
    }
  | { event: 'dag_finished'; elapsed_ms: number }
  | {
      event: 'dag_failed';
      failed_node_id?: string | null;
      error: string;
      error_type: string;
    };

// --- Client-side runtime state for a node ---

export type NodeRunState = 'idle' | 'running' | 'success' | 'error';

export interface NodeRuntime {
  state: NodeRunState;
  elapsed_ms?: number;
  row_count?: number;
  truncated?: boolean;
  error?: string;
}

export interface PreviewBuffer {
  columns: Array<{ name: string; type: string }>;
  rows: unknown[][];
}
