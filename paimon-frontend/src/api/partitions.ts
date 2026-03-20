import apiClient, { getCurrentPrefix } from './client';
import { encodeBranchTable } from './tables';
import type { PartitionInfo } from './types';

export async function listPartitions(
  database: string,
  table: string,
  branch?: string
): Promise<PartitionInfo[]> {
  const t = encodeBranchTable(table, branch);
  const resp = await apiClient.get(
    `/${getCurrentPrefix()}/databases/${database}/tables/${t}/partitions`
  );
  return resp.data.partitions ?? [];
}
