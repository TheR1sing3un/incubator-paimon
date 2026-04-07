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

import type { DagNodeSpec, DagEdgeSpec, DagNodeType } from './types';

const NAME_RE = /^[a-zA-Z_][a-zA-Z0-9_]*$/;

const SINK_TYPES: ReadonlySet<DagNodeType> = new Set([
  'preview',
  'paimon_write',
  'file_export',
]);

const DEGREE: Record<DagNodeType, { min: number; max: number | null }> = {
  paimon_read: { min: 0, max: 0 },
  sql: { min: 1, max: null },
  paimon_write: { min: 1, max: 1 },
  preview: { min: 1, max: 1 },
  file_export: { min: 1, max: 1 },
};

export const MAX_NODES = 50;

/**
 * Pre-flight validation that mirrors the backend validator, surfacing
 * problems in the canvas before the user pays the cost of a round trip.
 * Returns an array of human-readable errors (empty = OK).
 */
export function validateDag(
  nodes: DagNodeSpec[],
  edges: DagEdgeSpec[],
): string[] {
  const errors: string[] = [];
  if (nodes.length === 0) {
    errors.push('DAG is empty — add some nodes first');
    return errors;
  }
  if (nodes.length > MAX_NODES) {
    errors.push(`DAG has ${nodes.length} nodes, maximum is ${MAX_NODES}`);
  }

  const ids = new Set<string>();
  const names = new Set<string>();
  for (const n of nodes) {
    if (ids.has(n.id)) errors.push(`Duplicate node id: ${n.id}`);
    ids.add(n.id);
    if (!NAME_RE.test(n.name)) {
      errors.push(
        `Node name ${JSON.stringify(n.name)} must be a valid identifier ` +
        `(letters/digits/underscore, cannot start with digit)`,
      );
    }
    if (names.has(n.name)) {
      errors.push(
        `Duplicate node name ${JSON.stringify(n.name)} — SQL nodes ` +
        `reference upstreams by name, so names must be unique`,
      );
    }
    names.add(n.name);
  }

  const inDegree = new Map<string, number>();
  const outEdges = new Map<string, string[]>();
  for (const e of edges) {
    if (!ids.has(e.from)) errors.push(`Edge source ${e.from} does not exist`);
    if (!ids.has(e.to)) errors.push(`Edge target ${e.to} does not exist`);
    if (e.from === e.to) errors.push(`Self-loop on node ${e.from}`);
    inDegree.set(e.to, (inDegree.get(e.to) ?? 0) + 1);
    const list = outEdges.get(e.from) ?? [];
    list.push(e.to);
    outEdges.set(e.from, list);
  }

  for (const n of nodes) {
    const deg = inDegree.get(n.id) ?? 0;
    const rule = DEGREE[n.type];
    if (deg < rule.min) {
      errors.push(
        `Node ${n.name} (${n.type}) needs ${rule.min} upstream(s), got ${deg}`,
      );
    }
    if (rule.max !== null && deg > rule.max) {
      errors.push(
        `Node ${n.name} (${n.type}) accepts at most ${rule.max} upstream(s), got ${deg}`,
      );
    }
  }

  if (!nodes.some((n) => SINK_TYPES.has(n.type))) {
    errors.push(
      'DAG has no sink node — add a preview / paimon_write / file_export',
    );
  }

  // Cycle detection (DFS white/grey/black).
  const color = new Map<string, 0 | 1 | 2>();
  for (const n of nodes) color.set(n.id, 0);
  const dfs = (start: string): boolean => {
    const stack: Array<{ id: string; idx: number }> = [{ id: start, idx: 0 }];
    color.set(start, 1);
    while (stack.length) {
      const top = stack[stack.length - 1];
      const kids = outEdges.get(top.id) ?? [];
      if (top.idx >= kids.length) {
        color.set(top.id, 2);
        stack.pop();
        continue;
      }
      const child = kids[top.idx++];
      const c = color.get(child) ?? 0;
      if (c === 1) return true;
      if (c === 0) {
        color.set(child, 1);
        stack.push({ id: child, idx: 0 });
      }
    }
    return false;
  };
  for (const n of nodes) {
    if ((color.get(n.id) ?? 0) === 0 && dfs(n.id)) {
      errors.push('DAG contains a cycle');
      break;
    }
  }

  return errors;
}
