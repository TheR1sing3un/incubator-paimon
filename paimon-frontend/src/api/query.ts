/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import axios from 'axios';
import type { CatalogConfig } from '../store/catalogStore';

const QUERY_URL_KEY = 'paimon-query-service-url';

export function loadQueryServiceUrl(): string {
  return localStorage.getItem(QUERY_URL_KEY) || '';
}

export function saveQueryServiceUrl(url: string) {
  localStorage.setItem(QUERY_URL_KEY, url);
}

export interface QueryRequest {
  sql: string;
  database: string;
  catalog_options: Record<string, string>;
  max_rows?: number;
  timeout_seconds?: number;
}

export interface QueryColumn {
  name: string;
  type: string;
}

export interface QueryResult {
  columns: QueryColumn[];
  rows: unknown[][];
  row_count: number;
  truncated: boolean;
  elapsed_ms: number;
}

export interface QueryError {
  error: string;
  error_type: string;
}

export function buildCatalogOptions(catalog: CatalogConfig | null): Record<string, string> {
  const queryUrl = catalog?.queryServiceCatalogUrl?.replace(/\/+$/, '') || '';
  const baseUrl = catalog?.baseUrl?.replace(/\/+$/, '') || '';
  return {
    metastore: 'rest',
    uri: queryUrl || baseUrl || 'http://127.0.0.1:8090',
    'token.provider': 'noop',
    warehouse: catalog?.prefix || 'paimon',
  };
}

function buildQueryUrl(queryServiceUrl: string): string {
  const base = queryServiceUrl.replace(/\/+$/, '');
  const targetUrl = `${base}/query/execute`;
  try {
    const parsed = new URL(targetUrl);
    const isLocal =
      parsed.hostname === 'localhost' ||
      parsed.hostname === '127.0.0.1' ||
      parsed.hostname === '0.0.0.0';
    if (isLocal) {
      // Local query service: use the URL directly.
      return targetUrl;
    }
  } catch {
    // Relative path — use as-is.
    return targetUrl;
  }
  // Remote query service: route through the dev-server proxy to avoid CORS.
  return `/proxy?target=${encodeURIComponent(targetUrl)}`;
}

export async function executeQuery(
  queryServiceUrl: string,
  req: QueryRequest,
  signal?: AbortSignal,
): Promise<QueryResult> {
  const url = buildQueryUrl(queryServiceUrl);
  const resp = await axios.post<QueryResult>(
    url,
    req,
    { timeout: 120000, signal, headers: { 'Content-Type': 'application/json' } },
  );
  return resp.data;
}
