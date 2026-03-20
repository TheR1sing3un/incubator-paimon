import apiClient, { getCurrentPrefix } from './client';
import type { SnapshotInfo } from './types';

export async function listSnapshots(
  database: string,
  table: string,
  pageToken?: string
): Promise<{ snapshots: SnapshotInfo[]; nextPageToken?: string }> {
  const params: Record<string, string> = {};
  if (pageToken) params.pageToken = pageToken;
  const resp = await apiClient.get(
    `/${getCurrentPrefix()}/databases/${database}/tables/${table}/snapshots`,
    { params }
  );
  return {
    snapshots: resp.data.snapshots ?? [],
    nextPageToken: resp.data.nextPageToken,
  };
}
