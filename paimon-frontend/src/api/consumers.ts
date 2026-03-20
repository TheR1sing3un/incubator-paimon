import apiClient, { getCurrentPrefix } from './client';
import { encodeBranchTable } from './tables';
import type { ConsumerInfo } from './types';

export async function listConsumers(
  database: string,
  table: string,
  branch?: string
): Promise<ConsumerInfo[]> {
  const t = encodeBranchTable(table, branch);
  const resp = await apiClient.get(
    `/${getCurrentPrefix()}/databases/${database}/tables/${t}/consumers`
  );
  return resp.data.consumers ?? [];
}
