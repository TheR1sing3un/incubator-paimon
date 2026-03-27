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

import { useEffect, useState } from 'react';
import { Spin, Tag, Tooltip, Empty } from 'antd';
import { useQuery } from '@tanstack/react-query';
import { listBranches } from '../../api/branches';
import { listSnapshots } from '../../api/snapshots';
import { listSchemas } from '../../api/schemas';
import { listTags } from '../../api/tags';
import { formatTimestamp } from '../../utils/format';
import type { BranchInfo, SnapshotInfo, TagInfo, FieldInfo, SchemaHistoryEntry, DataTypeNode } from '../../api/types';

function typeToString(t: DataTypeNode): string {
  if (typeof t === 'string') return t;
  const keyword = t.type;
  if (t.fields) {
    const inner = t.fields.map(f => `${f.name} ${typeToString(f.type)}`).join(', ');
    return `${keyword}<${inner}>`;
  }
  if (t.key !== undefined && t.value !== undefined) return `${keyword}<${typeToString(t.key)}, ${typeToString(t.value)}>`;
  if (t.element !== undefined && t.dimension !== undefined) return `${keyword}<${typeToString(t.element)}, ${t.dimension}>`;
  if (t.element !== undefined) return `${keyword}<${typeToString(t.element)}>`;
  return keyword;
}

interface Props {
  database: string;
  table: string;
}

interface BranchData {
  branch: BranchInfo;
  snapshots: SnapshotInfo[];
  tags: TagInfo[];
}

const BRANCH_COLORS = ['#1677ff', '#52c41a', '#fa8c16', '#eb2f96', '#722ed1', '#13c2c2'];
const SCHEMA_COLOR = '#722ed1';
const TAG_BG = '#fff7e6';
const TAG_BORDER = '#ffc53d';
const TAG_TEXT = '#d48806';
const COL_WIDTH = 340;
const BASE_ROW_HEIGHT = 52;
const TAG_LINE_HEIGHT = 20;
const NODE_R = 7;
const LEFT_PAD = 30;

interface SchemaDiff {
  added: FieldInfo[];
  removed: FieldInfo[];
  typeChanged: { name: string; oldType: string; newType: string }[];
}

function diffSchema(
  prev: SchemaHistoryEntry['schema'],
  next: SchemaHistoryEntry['schema']
): SchemaDiff {
  const prevByName = new Map(prev.fields.map((f) => [f.name, f]));
  const nextByName = new Map(next.fields.map((f) => [f.name, f]));
  const added = next.fields.filter((f) => !prevByName.has(f.name));
  const removed = prev.fields.filter((f) => !nextByName.has(f.name));
  const typeChanged: SchemaDiff['typeChanged'] = [];
  for (const f of next.fields) {
    const old = prevByName.get(f.name);
    const oldStr = old ? typeToString(old.type) : '';
    const newStr = typeToString(f.type);
    if (old && oldStr !== newStr) {
      typeChanged.push({ name: f.name, oldType: oldStr, newType: newStr });
    }
  }
  return { added, removed, typeChanged };
}

export default function BranchGraph({ database, table }: Props) {
  const [data, setData] = useState<BranchData[]>([]);
  const [schemaMap, setSchemaMap] = useState<Map<number, SchemaHistoryEntry>>(new Map());
  const [loading, setLoading] = useState(true);

  const { data: branches = [] } = useQuery({
    queryKey: ['branches', database, table],
    queryFn: () => listBranches(database, table),
  });

  useEffect(() => {
    if (branches.length === 0) return;
    setLoading(true);
    const fetchAll = async () => {
      const results: BranchData[] = [];
      const allSchemas = new Map<number, SchemaHistoryEntry>();
      for (const branch of branches) {
        const [snapResp, tagResp, schemaResp] = await Promise.allSettled([
          listSnapshots(database, table, undefined, branch.branch),
          listTags(database, table, branch.branch),
          listSchemas(database, table, branch.branch),
        ]);
        results.push({
          branch,
          snapshots: snapResp.status === 'fulfilled' ? snapResp.value.snapshots : [],
          tags: tagResp.status === 'fulfilled' ? tagResp.value : [],
        });
        if (schemaResp.status === 'fulfilled') {
          for (const entry of schemaResp.value) {
            allSchemas.set(entry.schemaId, entry);
          }
        }
      }
      setData(results);
      setSchemaMap(allSchemas);
      setLoading(false);
    };
    fetchAll();
  }, [branches, database, table]);

  if (loading || branches.length === 0) return <Spin />;

  // Sort: main first
  const sorted = [...data].sort((a, b) => {
    if (a.branch.branch === 'main') return -1;
    if (b.branch.branch === 'main') return 1;
    return a.branch.branch.localeCompare(b.branch.branch);
  });

  if (sorted.every((b) => b.snapshots.length === 0)) {
    return <Empty description="No snapshot data" />;
  }

  // --- Build tag lookup: key = "branchIdx:timeMillis" → tag names ---
  // First build the graph structure, then attach tags

  const mainSnaps = sorted[0]?.snapshots ?? [];
  const mainTimeSet = new Set(mainSnaps.map((s) => s.timeMillis));

  interface ForkInfo {
    forkSnapTime: number;
    uniqueSnapshots: SnapshotInfo[];
  }
  const branchForks: (ForkInfo | null)[] = sorted.map((bd, idx) => {
    if (idx === 0) return null;
    const shared = bd.snapshots.filter((s) => mainTimeSet.has(s.timeMillis));
    const unique = bd.snapshots.filter((s) => !mainTimeSet.has(s.timeMillis));
    const forkSnap = shared.length > 0
      ? shared.reduce((a, b) => (a.timeMillis > b.timeMillis ? a : b))
      : null;
    return {
      forkSnapTime: forkSnap?.timeMillis ?? 0,
      uniqueSnapshots: unique.sort((a, b) => a.timeMillis - b.timeMillis),
    };
  });

  // Collect all display times
  const allTimes = new Set<number>();
  mainSnaps.forEach((s) => allTimes.add(s.timeMillis));
  branchForks.forEach((fi) => fi?.uniqueSnapshots.forEach((s) => allTimes.add(s.timeMillis)));
  const sortedTimes = [...allTimes].sort((a, b) => a - b);

  // Build lookup maps per column
  const mainByTime = new Map(mainSnaps.map((s) => [s.timeMillis, s]));
  const colByTime: Map<number, SnapshotInfo>[] = sorted.map((_, idx) => {
    if (idx === 0) return mainByTime;
    const fi = branchForks[idx];
    if (!fi) return new Map();
    return new Map(fi.uniqueSnapshots.map((s) => [s.timeMillis, s]));
  });

  // --- Schema change detection ---
  // For each column, sort snapshots by time and mark where schemaId changes
  // Key: "colIdx:timeMillis" → { prevSchemaId, newSchemaId }
  const schemaChangeMap = new Map<string, { prev: number; next: number }>();
  sorted.forEach((_, colIdx) => {
    const snaps = [...colByTime[colIdx].values()].sort((a, b) => a.timeMillis - b.timeMillis);
    for (let i = 1; i < snaps.length; i++) {
      if (snaps[i].schemaId !== snaps[i - 1].schemaId) {
        schemaChangeMap.set(`${colIdx}:${snaps[i].timeMillis}`, {
          prev: snaps[i - 1].schemaId,
          next: snaps[i].schemaId,
        });
      }
    }
  });

  interface RowNode {
    colIdx: number;
    branchName: string;
    snapshot: SnapshotInfo;
  }
  interface Row {
    nodes: RowNode[];
    time: number;
  }

  const rows: Row[] = sortedTimes.map((time) => {
    const nodes: RowNode[] = [];
    sorted.forEach((bd, colIdx) => {
      const snap = colByTime[colIdx].get(time);
      if (snap) nodes.push({ colIdx, branchName: bd.branch.branch, snapshot: snap });
    });
    return { nodes, time };
  }).filter((r) => r.nodes.length > 0);

  // --- Tag lookup ---
  // Build map: "colIdx:timeMillis" → tagName[]
  const tagMap = new Map<string, string[]>();
  sorted.forEach((bd, colIdx) => {
    for (const tag of bd.tags) {
      if (!tag.snapshot) continue;
      const time = tag.snapshot.timeMillis;
      // Find which column this tag's snapshot belongs to
      // If the time is in main and colIdx is not main, check if it's a shared snapshot
      let targetCol = colIdx;
      if (colIdx > 0 && mainTimeSet.has(time)) {
        // This tag is on a shared snapshot — show it on the main column
        targetCol = 0;
      }
      // But only if that column actually has a node at this time
      if (!colByTime[targetCol].has(time)) {
        // Try original column
        targetCol = colIdx;
      }
      const key = `${targetCol}:${time}`;
      const existing = tagMap.get(key) ?? [];
      if (!existing.includes(tag.tagName)) {
        existing.push(tag.tagName);
        tagMap.set(key, existing);
      }
    }
  });

  // Calculate row Y positions (variable height based on tag count)
  const rowYPositions: number[] = [];
  let currentY = 40;
  rows.forEach((row) => {
    rowYPositions.push(currentY);
    // Find max tags on any node in this row
    let maxTags = 0;
    row.nodes.forEach((node) => {
      const key = `${node.colIdx}:${node.snapshot.timeMillis}`;
      const tags = tagMap.get(key);
      if (tags) maxTags = Math.max(maxTags, tags.length);
    });
    currentY += BASE_ROW_HEIGHT + maxTags * TAG_LINE_HEIGHT;
  });

  // Fork lines
  const forkLines: { mainRow: number; branchCol: number; branchFirstRow: number }[] = [];
  sorted.forEach((_, colIdx) => {
    if (colIdx === 0) return;
    const fi = branchForks[colIdx];
    if (!fi || fi.forkSnapTime === 0) return;
    const mainRow = rows.findIndex((r) => r.nodes.some((n) => n.colIdx === 0 && n.snapshot.timeMillis === fi.forkSnapTime));
    const branchFirstRow = rows.findIndex((r) => r.nodes.some((n) => n.colIdx === colIdx));
    if (mainRow >= 0 && branchFirstRow >= 0) {
      forkLines.push({ mainRow, branchCol: colIdx, branchFirstRow });
    }
  });

  // Column row tracking for vertical lines
  const colRows: number[][] = sorted.map(() => []);
  rows.forEach((_, rowIdx) => {
    rows[rowIdx].nodes.forEach((node) => {
      colRows[node.colIdx].push(rowIdx);
    });
  });

  const getX = (colIdx: number) => LEFT_PAD + colIdx * COL_WIDTH + 20;
  const getY = (rowIdx: number) => rowYPositions[rowIdx];
  const svgWidth = LEFT_PAD + sorted.length * COL_WIDTH + 20;
  const svgHeight = currentY + 20;

  return (
    <div style={{ overflowX: 'auto' }}>
      <div style={{ marginBottom: 12 }}>
        {sorted.map((bd, idx) => (
          <Tag key={bd.branch.branch} color={BRANCH_COLORS[idx % BRANCH_COLORS.length]}>
            {bd.branch.branch}
            {bd.branch.latestSnapshotId != null && ` (latest: #${bd.branch.latestSnapshotId})`}
          </Tag>
        ))}
      </div>

      <svg width={svgWidth} height={svgHeight} style={{ display: 'block' }}>
        {/* Column headers */}
        {sorted.map((bd, colIdx) => (
          <text
            key={`h-${colIdx}`}
            x={getX(colIdx)}
            y={18}
            fontSize={13}
            fontWeight="bold"
            fill={BRANCH_COLORS[colIdx % BRANCH_COLORS.length]}
          >
            {bd.branch.branch}
          </text>
        ))}

        {/* Vertical lines */}
        {colRows.map((rowIndices, colIdx) => {
          if (rowIndices.length < 2) return null;
          const x = getX(colIdx);
          return (
            <line
              key={`vl-${colIdx}`}
              x1={x} y1={getY(rowIndices[0])}
              x2={x} y2={getY(rowIndices[rowIndices.length - 1])}
              stroke={BRANCH_COLORS[colIdx % BRANCH_COLORS.length]}
              strokeWidth={2}
              opacity={0.3}
            />
          );
        })}

        {/* Fork lines */}
        {forkLines.map((fl, i) => {
          const fromX = getX(0);
          const toX = getX(fl.branchCol);
          const fromY = getY(fl.mainRow);
          const toY = getY(fl.branchFirstRow);
          const midY = (fromY + toY) / 2;
          return (
            <path
              key={`fk-${i}`}
              d={`M ${fromX} ${fromY} C ${fromX} ${midY}, ${toX} ${midY}, ${toX} ${toY}`}
              fill="none"
              stroke={BRANCH_COLORS[fl.branchCol % BRANCH_COLORS.length]}
              strokeWidth={2}
              strokeDasharray="6,3"
              opacity={0.5}
            />
          );
        })}

        {/* Snapshot nodes + tags */}
        {rows.map((row, rowIdx) =>
          row.nodes.map((node) => {
            const x = getX(node.colIdx);
            const y = getY(rowIdx);
            const color = BRANCH_COLORS[node.colIdx % BRANCH_COLORS.length];
            const tagKey = `${node.colIdx}:${node.snapshot.timeMillis}`;
            const nodeTags = tagMap.get(tagKey) ?? [];
            const schemaChange = schemaChangeMap.get(tagKey);
            const schemaDiff = schemaChange
              ? (() => {
                  const prevEntry = schemaMap.get(schemaChange.prev);
                  const nextEntry = schemaMap.get(schemaChange.next);
                  return prevEntry && nextEntry ? diffSchema(prevEntry.schema, nextEntry.schema) : null;
                })()
              : null;

            return (
              <g key={`${node.branchName}-${node.snapshot.id}`}>
                {/* Tooltip on node */}
                <Tooltip
                  title={
                    <div style={{ fontSize: 12 }}>
                      <div><b>{node.branchName}</b> snapshot #{node.snapshot.id}</div>
                      <div>Kind: {node.snapshot.commitKind}</div>
                      <div>Schema: {node.snapshot.schemaId}</div>
                      {schemaChange && (
                        <div style={{ color: SCHEMA_COLOR, fontWeight: 'bold' }}>
                          Schema changed: {schemaChange.prev} → {schemaChange.next}
                        </div>
                      )}
                      {schemaDiff && (
                        <div style={{ marginTop: 4, borderTop: '1px solid rgba(255,255,255,0.2)', paddingTop: 4 }}>
                          {schemaDiff.added.map((f) => (
                            <div key={`+${f.name}`} style={{ color: '#52c41a' }}>
                              + {f.name} ({typeToString(f.type)})
                            </div>
                          ))}
                          {schemaDiff.removed.map((f) => (
                            <div key={`-${f.name}`} style={{ color: '#ff4d4f' }}>
                              - {f.name} ({typeToString(f.type)})
                            </div>
                          ))}
                          {schemaDiff.typeChanged.map((c) => (
                            <div key={`~${c.name}`} style={{ color: '#fa8c16' }}>
                              ~ {c.name}: {c.oldType} → {c.newType}
                            </div>
                          ))}
                        </div>
                      )}
                      {node.snapshot.properties?.['paimon.commit.committer'] && (
                        <div>Committer: {node.snapshot.properties['paimon.commit.committer']}</div>
                      )}
                      {node.snapshot.properties?.['paimon.commit.message'] && (
                        <div>Message: {node.snapshot.properties['paimon.commit.message']}</div>
                      )}
                      <div>Records: {node.snapshot.totalRecordCount} (+{node.snapshot.deltaRecordCount})</div>
                      <div>{formatTimestamp(node.snapshot.timeMillis)}</div>
                      {nodeTags.length > 0 && (
                        <div>Tags: {nodeTags.join(', ')}</div>
                      )}
                    </div>
                  }
                >
                  {schemaChange ? (
                    <g style={{ cursor: 'pointer' }}>
                      {/* Diamond outline for schema change */}
                      <rect
                        x={x - NODE_R - 2}
                        y={y - NODE_R - 2}
                        width={(NODE_R + 2) * 2}
                        height={(NODE_R + 2) * 2}
                        fill="none"
                        stroke={SCHEMA_COLOR}
                        strokeWidth={2}
                        transform={`rotate(45, ${x}, ${y})`}
                      />
                      <circle
                        cx={x} cy={y} r={NODE_R}
                        fill={color} stroke="#fff" strokeWidth={2}
                      />
                    </g>
                  ) : (
                    <circle
                      cx={x} cy={y} r={NODE_R}
                      fill={color} stroke="#fff" strokeWidth={2}
                      style={{ cursor: 'pointer' }}
                    />
                  )}
                </Tooltip>

                {/* Snapshot label */}
                <text x={x + NODE_R + 8} y={y + 1} fontSize={12} fill="#333" dominantBaseline="middle">
                  #{node.snapshot.id} {node.snapshot.commitKind}
                  <tspan fill="#999" fontSize={10}>
                    {' '}({node.snapshot.totalRecordCount} records)
                  </tspan>
                  {schemaChange && (
                    <tspan fill={SCHEMA_COLOR} fontSize={10} fontWeight="bold">
                      {' '}schema→{schemaChange.next}
                    </tspan>
                  )}
                </text>

                {/* Tag badges */}
                {nodeTags.map((tagName, ti) => {
                  const tagX = x + NODE_R + 8;
                  const tagY = y + 14 + ti * TAG_LINE_HEIGHT;
                  const textLen = tagName.length * 7 + 16;
                  return (
                    <g key={`tag-${tagName}`}>
                      <rect
                        x={tagX}
                        y={tagY}
                        width={textLen}
                        height={17}
                        rx={3}
                        fill={TAG_BG}
                        stroke={TAG_BORDER}
                        strokeWidth={1}
                      />
                      {/* Tag icon (bookmark shape) */}
                      <text
                        x={tagX + 4}
                        y={tagY + 12}
                        fontSize={10}
                        fill={TAG_TEXT}
                      >
                        🏷
                      </text>
                      <text
                        x={tagX + 18}
                        y={tagY + 12}
                        fontSize={11}
                        fill={TAG_TEXT}
                        fontWeight={500}
                      >
                        {tagName}
                      </text>
                    </g>
                  );
                })}
              </g>
            );
          })
        )}
      </svg>
    </div>
  );
}
