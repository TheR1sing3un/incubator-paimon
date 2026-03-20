import apiClient, { getCurrentPrefix } from './client';
import type { ConsumerInfo } from './types';

export async function listConsumers(
  database: string,
  table: string
): Promise<ConsumerInfo[]> {
  const resp = await apiClient.get(
    `/${getCurrentPrefix()}/databases/${database}/tables/${table}/consumers`
  );
  return resp.data.consumers ?? [];
}
