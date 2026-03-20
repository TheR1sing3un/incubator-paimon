import apiClient, { getCurrentPrefix } from './client';
import type { SchemaHistoryEntry } from './types';

export async function listSchemas(
  database: string,
  table: string
): Promise<SchemaHistoryEntry[]> {
  const resp = await apiClient.get(
    `/${getCurrentPrefix()}/databases/${database}/tables/${table}/schemas`
  );
  return resp.data.schemas ?? [];
}
