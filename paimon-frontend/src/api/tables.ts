import apiClient, { getCurrentPrefix } from './client';
import type { TableInfo } from './types';

export async function listTables(database: string): Promise<string[]> {
  const resp = await apiClient.get(`/${getCurrentPrefix()}/databases/${database}/tables`);
  return resp.data.tables ?? [];
}

export async function getTable(database: string, table: string): Promise<TableInfo> {
  const resp = await apiClient.get(`/${getCurrentPrefix()}/databases/${database}/tables/${table}`);
  return resp.data;
}
