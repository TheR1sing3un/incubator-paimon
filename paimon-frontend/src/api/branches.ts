import apiClient, { getCurrentPrefix } from './client';
import type { BranchInfo } from './types';

export async function listBranches(
  database: string,
  table: string
): Promise<BranchInfo[]> {
  const resp = await apiClient.get(
    `/${getCurrentPrefix()}/databases/${database}/tables/${table}/branches`
  );
  return resp.data.branches ?? [];
}
