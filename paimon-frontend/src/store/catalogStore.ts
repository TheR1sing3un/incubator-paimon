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

import { createContext, useContext } from 'react';

export interface CatalogConfig {
  name: string;
  baseUrl: string; // e.g. "https://example.com" or "" for local
  prefix: string;  // e.g. "paimon"
  queryServiceCatalogUrl?: string; // URL that query service uses to access this catalog
}

const CATALOGS_KEY = 'paimon-catalogs';
const ACTIVE_KEY = 'paimon-active-catalog';

export function loadCatalogs(): CatalogConfig[] {
  try {
    const raw = localStorage.getItem(CATALOGS_KEY);
    return raw ? JSON.parse(raw) : [];
  } catch {
    return [];
  }
}

export function saveCatalogs(catalogs: CatalogConfig[]) {
  localStorage.setItem(CATALOGS_KEY, JSON.stringify(catalogs));
}

export function loadActiveName(): string | null {
  return localStorage.getItem(ACTIVE_KEY);
}

export function saveActiveName(name: string) {
  localStorage.setItem(ACTIVE_KEY, name);
}

export function getApiBase(catalog: CatalogConfig | null): string {
  if (!catalog || !catalog.baseUrl) return '/v1';
  const base = catalog.baseUrl.replace(/\/+$/, '');
  return `${base}/v1`;
}

export function getPrefix(catalog: CatalogConfig | null): string {
  return catalog?.prefix || 'default';
}

export interface CatalogContextValue {
  catalogs: CatalogConfig[];
  active: CatalogConfig | null;
  setActive: (name: string) => void;
  addCatalog: (config: CatalogConfig) => void;
  updateCatalog: (oldName: string, config: CatalogConfig) => void;
  removeCatalog: (name: string) => void;
}

export const CatalogContext = createContext<CatalogContextValue>({
  catalogs: [],
  active: null,
  setActive: () => {},
  addCatalog: () => {},
  updateCatalog: () => {},
  removeCatalog: () => {},
});

export function useCatalog() {
  return useContext(CatalogContext);
}
