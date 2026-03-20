import apiClient, { getCurrentPrefix } from './client';
import { encodeBranchTable } from './tables';
import type { SnapshotInfo } from './types';

export async function listSnapshots(
  database: string,
  table: string,
  pageToken?: string,
  branch?: string
): Promise<{ snapshots: SnapshotInfo[]; nextPageToken?: string }> {
  const params: Record<string, string> = {};
  if (pageToken) params.pageToken = pageToken;
  const t = encodeBranchTable(table, branch);
  const resp = await apiClient.get(
    `/${getCurrentPrefix()}/databases/${database}/tables/${t}/snapshots`,
    { params }
  );
  return {
    snapshots: resp.data.snapshots ?? [],
    nextPageToken: resp.data.nextPageToken,
  };
}
