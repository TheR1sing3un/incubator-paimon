import { useEffect, useState } from 'react';
import { Spin, Tag, Tooltip, Empty } from 'antd';
import { useQuery } from '@tanstack/react-query';
import { listBranches } from '../../api/branches';
import { listSnapshots } from '../../api/snapshots';
import { formatTimestamp } from '../../utils/format';
import type { BranchInfo, SnapshotInfo } from '../../api/types';

interface Props {
  database: string;
  table: string;
}

interface BranchSnapshots {
  branch: BranchInfo;
  snapshots: SnapshotInfo[];
}

const COLORS = ['#1677ff', '#52c41a', '#fa8c16', '#eb2f96', '#722ed1', '#13c2c2'];
const COL_WIDTH = 300;
const ROW_HEIGHT = 48;
const NODE_R = 7;
const LEFT_PAD = 30;

export default function BranchGraph({ database, table }: Props) {
  const [branchData, setBranchData] = useState<BranchSnapshots[]>([]);
  const [loading, setLoading] = useState(true);

  const { data: branches = [] } = useQuery({
    queryKey: ['branches', database, table],
    queryFn: () => listBranches(database, table),
  });

  useEffect(() => {
    if (branches.length === 0) return;
    setLoading(true);
    const fetchAll = async () => {
      const results: BranchSnapshots[] = [];
      for (const branch of branches) {
        try {
          const resp = await listSnapshots(database, table, undefined, branch.branch);
          results.push({ branch, snapshots: resp.snapshots });
        } catch {
          results.push({ branch, snapshots: [] });
        }
      }
      setBranchData(results);
      setLoading(false);
    };
    fetchAll();
  }, [branches, database, table]);

  if (loading || branches.length === 0) return <Spin />;

  // Sort branches: main first
  const sorted = [...branchData].sort((a, b) => {
    if (a.branch.branch === 'main') return -1;
    if (b.branch.branch === 'main') return 1;
    return a.branch.branch.localeCompare(b.branch.branch);
  });

  if (sorted.length === 0 || sorted.every((b) => b.snapshots.length === 0)) {
    return <Empty description="No snapshot data" />;
  }

  const mainSnaps = sorted[0]?.snapshots ?? [];
  const mainTimeSet = new Set(mainSnaps.map((s) => s.timeMillis));

  // For each non-main branch, find fork point and filter out shared snapshots
  interface ForkInfo {
    forkSnapTime: number; // timeMillis of the fork-point snapshot on main
    uniqueSnapshots: SnapshotInfo[]; // snapshots after fork
  }
  const branchForks: (ForkInfo | null)[] = sorted.map((bd, idx) => {
    if (idx === 0) return null; // main
    // Find shared snapshots (same timeMillis as main)
    const shared = bd.snapshots.filter((s) => mainTimeSet.has(s.timeMillis));
    const unique = bd.snapshots.filter((s) => !mainTimeSet.has(s.timeMillis));
    // Fork point is the latest shared snapshot
    const forkSnap = shared.length > 0
      ? shared.reduce((a, b) => (a.timeMillis > b.timeMillis ? a : b))
      : null;
    return {
      forkSnapTime: forkSnap?.timeMillis ?? 0,
      uniqueSnapshots: unique.sort((a, b) => a.timeMillis - b.timeMillis),
    };
  });

  // Build rows: each row is a time point
  // Main column shows all main snapshots
  // Branch columns only show unique snapshots, aligned by time
  interface RowNode {
    colIdx: number;
    branchName: string;
    snapshot: SnapshotInfo;
  }
  interface Row {
    nodes: RowNode[];
    time: number;
  }

  // Collect all unique times, sorted ascending
  const allTimes = new Set<number>();
  // Main snapshots
  mainSnaps.forEach((s) => allTimes.add(s.timeMillis));
  // Branch unique snapshots
  branchForks.forEach((fi) => {
    fi?.uniqueSnapshots.forEach((s) => allTimes.add(s.timeMillis));
  });
  const sortedTimes = [...allTimes].sort((a, b) => a - b);

  // Build lookup maps
  const mainByTime = new Map(mainSnaps.map((s) => [s.timeMillis, s]));
  const branchByTime: Map<number, SnapshotInfo>[] = sorted.map((_, idx) => {
    if (idx === 0) return mainByTime;
    const fi = branchForks[idx];
    if (!fi) return new Map();
    return new Map(fi.uniqueSnapshots.map((s) => [s.timeMillis, s]));
  });

  const rows: Row[] = sortedTimes.map((time) => {
    const nodes: RowNode[] = [];
    sorted.forEach((bd, colIdx) => {
      const snap = branchByTime[colIdx].get(time);
      if (snap) {
        nodes.push({ colIdx, branchName: bd.branch.branch, snapshot: snap });
      }
    });
    return { nodes, time };
  }).filter((r) => r.nodes.length > 0);

  // Find fork row indices for drawing fork lines
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

  // Track column ranges for vertical lines
  const colRows: number[][] = sorted.map(() => []);
  rows.forEach((row, rowIdx) => {
    row.nodes.forEach((node) => {
      colRows[node.colIdx].push(rowIdx);
    });
  });

  const getX = (colIdx: number) => LEFT_PAD + colIdx * COL_WIDTH + 20;
  const getY = (rowIdx: number) => 36 + rowIdx * ROW_HEIGHT;
  const svgWidth = LEFT_PAD + sorted.length * COL_WIDTH + 20;
  const svgHeight = rows.length * ROW_HEIGHT + 60;

  return (
    <div style={{ overflowX: 'auto' }}>
      <div style={{ marginBottom: 12 }}>
        {sorted.map((bd, idx) => (
          <Tag key={bd.branch.branch} color={COLORS[idx % COLORS.length]}>
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
            y={16}
            fontSize={13}
            fontWeight="bold"
            fill={COLORS[colIdx % COLORS.length]}
          >
            {bd.branch.branch}
          </text>
        ))}

        {/* Vertical lines per column */}
        {colRows.map((rowIndices, colIdx) => {
          if (rowIndices.length < 2) return null;
          const x = getX(colIdx);
          return (
            <line
              key={`vl-${colIdx}`}
              x1={x} y1={getY(rowIndices[0])}
              x2={x} y2={getY(rowIndices[rowIndices.length - 1])}
              stroke={COLORS[colIdx % COLORS.length]}
              strokeWidth={2}
              opacity={0.3}
            />
          );
        })}

        {/* Fork lines (curved) */}
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
              stroke={COLORS[fl.branchCol % COLORS.length]}
              strokeWidth={2}
              strokeDasharray="6,3"
              opacity={0.5}
            />
          );
        })}

        {/* Snapshot nodes */}
        {rows.map((row, rowIdx) =>
          row.nodes.map((node) => {
            const x = getX(node.colIdx);
            const y = getY(rowIdx);
            const color = COLORS[node.colIdx % COLORS.length];
            const label = `#${node.snapshot.id} ${node.snapshot.commitKind}`;
            const detail = `${node.snapshot.totalRecordCount} records`;
            return (
              <g key={`${node.branchName}-${node.snapshot.id}`}>
                <Tooltip
                  title={
                    <div style={{ fontSize: 12 }}>
                      <div><b>{node.branchName}</b> snapshot #{node.snapshot.id}</div>
                      <div>Kind: {node.snapshot.commitKind}</div>
                      <div>Schema: {node.snapshot.schemaId}</div>
                      <div>Records: {node.snapshot.totalRecordCount}</div>
                      <div>Delta: +{node.snapshot.deltaRecordCount}</div>
                      <div>{formatTimestamp(node.snapshot.timeMillis)}</div>
                    </div>
                  }
                >
                  <circle
                    cx={x} cy={y} r={NODE_R}
                    fill={color} stroke="#fff" strokeWidth={2}
                    style={{ cursor: 'pointer' }}
                  />
                </Tooltip>
                <text x={x + NODE_R + 8} y={y + 1} fontSize={12} fill="#333" dominantBaseline="middle">
                  {label}
                </text>
                <text x={x + NODE_R + 8} y={y + 15} fontSize={10} fill="#aaa">
                  {detail}
                </text>
              </g>
            );
          })
        )}
      </svg>
    </div>
  );
}
