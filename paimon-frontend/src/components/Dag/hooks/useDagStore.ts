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

import { useCallback, useMemo, useState } from 'react';
import {
  applyNodeChanges,
  applyEdgeChanges,
  type Node,
  type Edge,
  type NodeChange,
  type EdgeChange,
  type Connection,
  addEdge,
} from '@xyflow/react';

import type {
  DagNodeType,
  NodeRuntime,
  PreviewBuffer,
} from '../types';

export interface DagNodeData extends Record<string, unknown> {
  name: string;
  nodeType: DagNodeType;
  config: Record<string, unknown>;
  runtime: NodeRuntime;
}

export type DagFlowNode = Node<DagNodeData>;

let _idCounter = 0;
function nextId(type: DagNodeType): string {
  _idCounter += 1;
  return `${type}_${Date.now().toString(36)}_${_idCounter}`;
}

const DEFAULT_CONFIG: Record<DagNodeType, Record<string, unknown>> = {
  paimon_read: { database: '', table: '' },
  sql: { sql: 'SELECT *\nFROM upstream_node_name' },
  paimon_write: { database: '', table: '', mode: 'append', create_if_not_exists: false },
  preview: {},
  file_export: { format: 'csv', filename: 'export.csv' },
};

const DEFAULT_NAME: Record<DagNodeType, string> = {
  paimon_read: 'source',
  sql: 'query',
  paimon_write: 'sink',
  preview: 'preview',
  file_export: 'export',
};

function makeDefaultRuntime(): NodeRuntime {
  return { state: 'idle' };
}

export interface DagStore {
  nodes: DagFlowNode[];
  edges: Edge[];
  selectedId: string | null;
  previews: Record<string, PreviewBuffer>;

  onNodesChange(changes: NodeChange[]): void;
  onEdgesChange(changes: EdgeChange[]): void;
  onConnect(connection: Connection): void;
  addNode(type: DagNodeType, position: { x: number; y: number }): void;
  setSelected(id: string | null): void;
  updateNodeConfig(id: string, patch: Record<string, unknown>): void;
  renameNode(id: string, name: string): void;
  deleteNode(id: string): void;
  clearAll(): void;

  // Runtime state mutations (called from SSE event handler)
  resetRuntime(): void;
  setNodeRuntime(id: string, runtime: NodeRuntime): void;
  appendPreviewSchema(nodeId: string, columns: PreviewBuffer['columns']): void;
  appendPreviewRows(nodeId: string, rows: unknown[][]): void;
}

export function useDagStore(): DagStore {
  const [nodes, setNodes] = useState<DagFlowNode[]>([]);
  const [edges, setEdges] = useState<Edge[]>([]);
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [previews, setPreviews] = useState<Record<string, PreviewBuffer>>({});

  const onNodesChange = useCallback(
    (changes: NodeChange[]) =>
      setNodes((ns) => applyNodeChanges(changes, ns) as DagFlowNode[]),
    [],
  );
  const onEdgesChange = useCallback(
    (changes: EdgeChange[]) => setEdges((es) => applyEdgeChanges(changes, es)),
    [],
  );
  const onConnect = useCallback(
    (connection: Connection) => setEdges((es) => addEdge(connection, es)),
    [],
  );

  const addNode = useCallback(
    (type: DagNodeType, position: { x: number; y: number }) => {
      const id = nextId(type);
      setNodes((ns) => {
        // Auto-number the default name so it's unique.
        let idx = 1;
        const existingNames = new Set(ns.map((n) => n.data.name));
        let name = DEFAULT_NAME[type];
        while (existingNames.has(name)) {
          idx += 1;
          name = `${DEFAULT_NAME[type]}_${idx}`;
        }
        return [
          ...ns,
          {
            id,
            type,
            position,
            data: {
              name,
              nodeType: type,
              config: { ...DEFAULT_CONFIG[type] },
              runtime: makeDefaultRuntime(),
            },
          },
        ];
      });
      setSelectedId(id);
    },
    [],
  );

  const setSelected = setSelectedId;

  const updateNodeConfig = useCallback(
    (id: string, patch: Record<string, unknown>) => {
      setNodes((ns) =>
        ns.map((n) =>
          n.id === id
            ? { ...n, data: { ...n.data, config: { ...n.data.config, ...patch } } }
            : n,
        ),
      );
    },
    [],
  );

  const renameNode = useCallback((id: string, name: string) => {
    setNodes((ns) =>
      ns.map((n) => (n.id === id ? { ...n, data: { ...n.data, name } } : n)),
    );
  }, []);

  const deleteNode = useCallback((id: string) => {
    setNodes((ns) => ns.filter((n) => n.id !== id));
    setEdges((es) => es.filter((e) => e.source !== id && e.target !== id));
    setSelectedId((s) => (s === id ? null : s));
    setPreviews((p) => {
      const { [id]: _, ...rest } = p;
      return rest;
    });
  }, []);

  const clearAll = useCallback(() => {
    setNodes([]);
    setEdges([]);
    setSelectedId(null);
    setPreviews({});
  }, []);

  const resetRuntime = useCallback(() => {
    setNodes((ns) =>
      ns.map((n) => ({
        ...n,
        data: { ...n.data, runtime: makeDefaultRuntime() },
      })),
    );
    setPreviews({});
  }, []);

  const setNodeRuntime = useCallback(
    (id: string, runtime: NodeRuntime) => {
      setNodes((ns) =>
        ns.map((n) => (n.id === id ? { ...n, data: { ...n.data, runtime } } : n)),
      );
    },
    [],
  );

  const appendPreviewSchema = useCallback(
    (nodeId: string, columns: PreviewBuffer['columns']) => {
      setPreviews((p) => ({ ...p, [nodeId]: { columns, rows: [] } }));
    },
    [],
  );

  const appendPreviewRows = useCallback(
    (nodeId: string, rows: unknown[][]) => {
      setPreviews((p) => {
        const prev = p[nodeId] ?? { columns: [], rows: [] };
        return { ...p, [nodeId]: { ...prev, rows: [...prev.rows, ...rows] } };
      });
    },
    [],
  );

  return useMemo(
    () => ({
      nodes,
      edges,
      selectedId,
      previews,
      onNodesChange,
      onEdgesChange,
      onConnect,
      addNode,
      setSelected,
      updateNodeConfig,
      renameNode,
      deleteNode,
      clearAll,
      resetRuntime,
      setNodeRuntime,
      appendPreviewSchema,
      appendPreviewRows,
    }),
    [
      nodes,
      edges,
      selectedId,
      previews,
      onNodesChange,
      onEdgesChange,
      onConnect,
      addNode,
      setSelected,
      updateNodeConfig,
      renameNode,
      deleteNode,
      clearAll,
      resetRuntime,
      setNodeRuntime,
      appendPreviewSchema,
      appendPreviewRows,
    ],
  );
}

/**
 * Serialise the current canvas into the wire DagRequest format.
 * Keeps the frontend React Flow structures separate from the backend
 * contract.
 */
export function serializeDag(
  nodes: DagFlowNode[],
  edges: Edge[],
): {
  nodes: Array<{
    id: string;
    name: string;
    type: DagNodeType;
    config: Record<string, unknown>;
  }>;
  edges: Array<{ from: string; to: string }>;
} {
  return {
    nodes: nodes.map((n) => ({
      id: n.id,
      name: n.data.name,
      type: n.data.nodeType,
      config: n.data.config,
    })),
    edges: edges.map((e) => ({ from: e.source, to: e.target })),
  };
}
