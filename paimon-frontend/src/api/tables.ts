import apiClient, { getCurrentPrefix } from './client';
import type { TableInfo } from './types';

export function encodeBranchTable(table: string, branch?: string): string {
  if (!branch || branch === 'main') return table;
  return `${table}$branch_${branch}`;
}

export async function listTables(database: string): Promise<string[]> {
  const resp = await apiClient.get(`/${getCurrentPrefix()}/databases/${database}/tables`);
  return resp.data.tables ?? [];
}

export async function getTable(database: string, table: string, branch?: string): Promise<TableInfo> {
  const t = encodeBranchTable(table, branch);
  const resp = await apiClient.get(`/${getCurrentPrefix()}/databases/${database}/tables/${t}`);
  return resp.data;
}
