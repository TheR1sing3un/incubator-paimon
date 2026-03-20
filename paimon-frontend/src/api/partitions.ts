import apiClient, { getCurrentPrefix } from './client';
import type { PartitionInfo } from './types';

export async function listPartitions(
  database: string,
  table: string
): Promise<PartitionInfo[]> {
  const resp = await apiClient.get(
    `/${getCurrentPrefix()}/databases/${database}/tables/${table}/partitions`
  );
  return resp.data.partitions ?? [];
}
