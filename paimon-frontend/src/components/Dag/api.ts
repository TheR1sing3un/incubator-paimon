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

import { toProxyUrl } from '../../api/client';
import type { DagEvent, DagRequest } from './types';

function buildDagUrl(queryServiceUrl: string): string {
  const base = queryServiceUrl.replace(/\/+$/, '');
  const targetUrl = `${base}/dag/execute`;
  try {
    const parsed = new URL(targetUrl);
    const isLocal =
      parsed.hostname === 'localhost' ||
      parsed.hostname === '127.0.0.1' ||
      parsed.hostname === '0.0.0.0';
    if (isLocal) return targetUrl;
  } catch {
    // Relative path — use as-is. Vite dev server proxies /dag to query-server.
    return targetUrl;
  }
  // Non-local — route through the dev-server proxy to avoid CORS.
  return toProxyUrl(targetUrl);
}

/**
 * Stream DAG events from the query-server.
 *
 * Uses `fetch` + ReadableStream rather than EventSource because
 * EventSource does not support POST bodies. Each SSE event is a single
 * ``data: {json}\n\n`` line; we parse them as they arrive and invoke
 * `onEvent` in order.
 *
 * The returned promise resolves when the stream ends cleanly (last event
 * was `dag_finished` or `dag_failed`) and rejects if the HTTP request
 * itself failed. Abort the `signal` to cancel mid-stream — the server
 * tears down the in-flight DAG via request disconnect.
 */
export async function executeDag(
  queryServiceUrl: string,
  req: DagRequest,
  onEvent: (event: DagEvent) => void,
  signal?: AbortSignal,
): Promise<void> {
  const url = buildDagUrl(queryServiceUrl);
  const resp = await fetch(url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(req),
    signal,
  });
  if (!resp.ok || !resp.body) {
    const text = await resp.text().catch(() => '');
    throw new Error(`DAG request failed: ${resp.status} ${resp.statusText} ${text}`);
  }
  const reader = resp.body.getReader();
  const decoder = new TextDecoder('utf-8');
  let buffer = '';
  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    buffer += decoder.decode(value, { stream: true });

    // SSE events are separated by a blank line (\n\n). Each event may
    // span multiple lines; we only look at `data: ` lines and ignore the
    // rest for simplicity.
    let sepIdx;
    while ((sepIdx = buffer.indexOf('\n\n')) >= 0) {
      const chunk = buffer.slice(0, sepIdx);
      buffer = buffer.slice(sepIdx + 2);
      for (const line of chunk.split('\n')) {
        if (line.startsWith('data: ')) {
          const jsonText = line.slice(6);
          try {
            const parsed = JSON.parse(jsonText) as DagEvent;
            onEvent(parsed);
          } catch (e) {
            console.warn('Failed to parse SSE event', jsonText, e);
          }
        }
      }
    }
  }
}
