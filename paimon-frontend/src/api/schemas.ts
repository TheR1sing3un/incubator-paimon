import apiClient, { getCurrentPrefix } from './client';
import { encodeBranchTable } from './tables';
import type { SchemaHistoryEntry } from './types';

export async function listSchemas(
  database: string,
  table: string,
  branch?: string
): Promise<SchemaHistoryEntry[]> {
  const t = encodeBranchTable(table, branch);
  const resp = await apiClient.get(
    `/${getCurrentPrefix()}/databases/${database}/tables/${t}/schemas`
  );
  return resp.data.schemas ?? [];
}
