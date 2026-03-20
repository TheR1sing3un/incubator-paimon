import apiClient, { getCurrentPrefix } from './client';
import type { BranchInfo } from './types';

export async function listBranches(
  database: string,
  table: string
): Promise<BranchInfo[]> {
  const prefix = getCurrentPrefix();
  const resp = await apiClient.get(
    `/${prefix}/databases/${database}/tables/${table}/branches`
  );
  const names: string[] = resp.data.branches ?? [];

  // Fetch details for each branch
  const details = await Promise.all(
    names.map((name) =>
      apiClient
        .get(`/${prefix}/databases/${database}/tables/${table}/branches/${name}`)
        .then((r) => r.data as BranchInfo)
    )
  );

  // API doesn't return 'main' in the list — always prepend it
  const mainResp = await apiClient.get(
    `/${prefix}/databases/${database}/tables/${table}/branches/main`
  );
  const mainBranch = mainResp.data as BranchInfo;

  return [mainBranch, ...details];
}
