import apiClient, { getCurrentPrefix } from './client';
import type { DatabaseInfo } from './types';

export async function listDatabases(): Promise<string[]> {
  const resp = await apiClient.get(`/${getCurrentPrefix()}/databases`);
  return resp.data.databases ?? [];
}

export async function getDatabase(database: string): Promise<DatabaseInfo> {
  const resp = await apiClient.get(`/${getCurrentPrefix()}/databases/${database}`);
  return resp.data;
}
