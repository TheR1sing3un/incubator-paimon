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

import { useCallback, useRef, useState } from 'react';
import { message } from 'antd';

import { executeDag } from '../api';
import type {
  DagEvent,
  DagRequest,
  RunnerMode,
} from '../types';
import type { DagStore } from './useDagStore';

export interface RunOptions {
  runner: RunnerMode;
  rayAddress?: string;
  catalogOptions: Record<string, string>;
  queryServiceUrl: string;
  maxPreviewRows?: number;
  timeoutSeconds?: number;
}

export interface DagExecutionState {
  running: boolean;
  dagError: string | null;
  run(options: RunOptions, payload: {
    nodes: DagRequest['nodes'];
    edges: DagRequest['edges'];
  }): Promise<void>;
  cancel(): void;
}

/**
 * Orchestrates a single DAG run: opens the SSE stream, decodes events,
 * forwards them to the store, and surfaces terminal failures via antd
 * `message`. File-export downloads are accumulated per export node and
 * triggered as a blob download when the node finishes.
 */
export function useDagExecution(store: DagStore): DagExecutionState {
  const [running, setRunning] = useState(false);
  const [dagError, setDagError] = useState<string | null>(null);
  const abortRef = useRef<AbortController | null>(null);

  // Per-export node accumulator: node_id -> { filename, format, chunks[] }
  const exportBuffers = useRef<
    Record<
      string,
      { filename: string; format: string; chunks: Uint8Array[] }
    >
  >({});

  const cancel = useCallback(() => {
    abortRef.current?.abort();
    abortRef.current = null;
  }, []);

  const run = useCallback(
    async (options: RunOptions, payload: { nodes: DagRequest['nodes']; edges: DagRequest['edges'] }) => {
      if (running) return;
      const ac = new AbortController();
      abortRef.current = ac;
      exportBuffers.current = {};
      store.resetRuntime();
      setDagError(null);
      setRunning(true);

      const req: DagRequest = {
        runner: options.runner,
        ray_address: options.rayAddress || null,
        catalog_options: options.catalogOptions,
        max_preview_rows: options.maxPreviewRows ?? 1000,
        timeout_seconds: options.timeoutSeconds ?? 300,
        nodes: payload.nodes,
        edges: payload.edges,
      };

      const handleEvent = (event: DagEvent) => {
        switch (event.event) {
          case 'dag_started':
            break;
          case 'node_started':
            store.setNodeRuntime(event.node_id, { state: 'running' });
            break;
          case 'node_finished':
            store.setNodeRuntime(event.node_id, {
              state: 'success',
              elapsed_ms: event.elapsed_ms,
              row_count: event.row_count,
              truncated: event.truncated,
            });
            // Trigger download for file_export nodes on completion.
            if (exportBuffers.current[event.node_id]) {
              const buf = exportBuffers.current[event.node_id];
              // Convert each Uint8Array to a standalone ArrayBuffer copy
              // so TypeScript's strict BlobPart check (which rejects
              // SharedArrayBuffer-backed views) is satisfied.
              const parts: BlobPart[] = buf.chunks.map((c) => {
                const ab = new ArrayBuffer(c.byteLength);
                new Uint8Array(ab).set(c);
                return ab;
              });
              const blob = new Blob(parts, {
                type: buf.format === 'csv'
                  ? 'text/csv'
                  : 'application/octet-stream',
              });
              const url = URL.createObjectURL(blob);
              const a = document.createElement('a');
              a.href = url;
              a.download = buf.filename;
              document.body.appendChild(a);
              a.click();
              document.body.removeChild(a);
              URL.revokeObjectURL(url);
              delete exportBuffers.current[event.node_id];
            }
            break;
          case 'node_failed':
            store.setNodeRuntime(event.node_id, {
              state: 'error',
              error: event.error,
            });
            break;
          case 'preview_schema':
            store.appendPreviewSchema(event.node_id, event.columns);
            break;
          case 'preview_chunk':
            store.appendPreviewRows(event.node_id, event.rows);
            break;
          case 'file_export_header':
            exportBuffers.current[event.node_id] = {
              filename: event.filename,
              format: event.format,
              chunks: [],
            };
            break;
          case 'file_export_chunk': {
            const buf = exportBuffers.current[event.node_id];
            if (buf) {
              const bin = atob(event.data_b64);
              const bytes = new Uint8Array(bin.length);
              for (let i = 0; i < bin.length; i += 1) bytes[i] = bin.charCodeAt(i);
              buf.chunks.push(bytes);
            }
            break;
          }
          case 'dag_failed':
            setDagError(event.error);
            message.error(`DAG failed: ${event.error}`);
            break;
          case 'dag_finished':
            message.success(`DAG finished in ${event.elapsed_ms}ms`);
            break;
          default:
            console.warn('Unknown DAG event', event);
        }
      };

      try {
        await executeDag(options.queryServiceUrl, req, handleEvent, ac.signal);
      } catch (e) {
        if (ac.signal.aborted) {
          message.info('DAG cancelled');
        } else {
          const msg = e instanceof Error ? e.message : String(e);
          setDagError(msg);
          message.error(`DAG request failed: ${msg}`);
        }
      } finally {
        setRunning(false);
        abortRef.current = null;
      }
    },
    [running, store],
  );

  return { running, dagError, run, cancel };
}
