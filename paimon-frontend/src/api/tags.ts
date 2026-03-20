import apiClient, { getCurrentPrefix } from './client';
import type { TagInfo } from './types';

export async function listTags(
  database: string,
  table: string
): Promise<TagInfo[]> {
  const resp = await apiClient.get(
    `/${getCurrentPrefix()}/databases/${database}/tables/${table}/tags`
  );
  return resp.data.tags ?? [];
}
