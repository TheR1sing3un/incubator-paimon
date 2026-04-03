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
import type { ErrorResponse } from './types';
import {
  type CatalogConfig,
  getPrefix,
} from '../store/catalogStore';

/**
 * Convert an absolute URL into a /proxy path so that nginx (or the Vite dev
 * middleware) can reverse-proxy the request and avoid CORS issues.
 * e.g. "https://host.com/v1/paimon/databases" → "/proxy/https/host.com/v1/paimon/databases"
 */
export function toProxyUrl(absoluteUrl: string): string {
  const parsed = new URL(absoluteUrl);
  return `/proxy/${parsed.protocol.replace(':', '')}/${parsed.host}${parsed.pathname}${parsed.search}`;
}

let currentCatalog: CatalogConfig | null = null;

export function setCurrentCatalog(catalog: CatalogConfig | null) {
  currentCatalog = catalog;
}

export function getCurrentPrefix(): string {
  return getPrefix(currentCatalog);
}

const apiClient = axios.create({
  timeout: 30000,
  headers: {
    'Content-Type': 'application/json',
  },
});

// Dynamically route requests:
// - Local catalog (baseUrl empty): request /v1/... directly (same origin)
// - Remote catalog: request /proxy/<scheme>/<host>/v1/... (server-side proxy)
apiClient.interceptors.request.use((config) => {
  const baseUrl = currentCatalog?.baseUrl?.replace(/\/+$/, '');
  const path = config.url || '';

  if (baseUrl) {
    // Remote catalog: route through server-side proxy
    const targetUrl = `${baseUrl}/v1${path}`;
    config.baseURL = '';
    config.url = toProxyUrl(targetUrl);
  } else {
    // Local catalog: direct same-origin request
    config.baseURL = '/v1';
  }
  return config;
});

apiClient.interceptors.response.use(
  (response) => response,
  (error) => {
    if (error.response?.data) {
      const errData = error.response.data as ErrorResponse;
      return Promise.reject(new Error(errData.message || 'Unknown error'));
    }
    return Promise.reject(error);
  }
);

export default apiClient;
