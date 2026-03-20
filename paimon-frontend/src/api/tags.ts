import apiClient, { getCurrentPrefix } from './client';
import { encodeBranchTable } from './tables';
import type { TagInfo } from './types';

export async function listTags(
  database: string,
  table: string,
  branch?: string
): Promise<TagInfo[]> {
  const prefix = getCurrentPrefix();
  const t = encodeBranchTable(table, branch);
  const resp = await apiClient.get(
    `/${prefix}/databases/${database}/tables/${t}/tags`
  );
  const names: string[] = resp.data.tags ?? [];
  const details = await Promise.all(
    names.map((name) =>
      apiClient
        .get(`/${prefix}/databases/${database}/tables/${t}/tags/${name}`)
        .then((r) => r.data as TagInfo)
    )
  );
  return details;
}
