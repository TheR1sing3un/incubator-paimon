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

import { Component, useState, useCallback, useEffect, type ReactNode } from 'react';
import { Routes, Route, Navigate, useNavigate } from 'react-router-dom';
import { ConfigProvider, theme, Alert } from 'antd';
import { useQueryClient } from '@tanstack/react-query';
import AppLayout from './components/Layout/AppLayout';
import DatabaseList from './components/Database/DatabaseList';
import DatabaseDetail from './components/Database/DatabaseDetail';
import TableDetail from './components/Table/TableDetail';
import {
  CatalogContext,
  type CatalogConfig,
  type CatalogContextValue,
  loadCatalogs,
  saveCatalogs,
  loadActiveName,
  saveActiveName,
} from './store/catalogStore';
import { setCurrentCatalog } from './api/client';

class ErrorBoundary extends Component<
  { children: ReactNode },
  { error: Error | null }
> {
  state: { error: Error | null } = { error: null };
  static getDerivedStateFromError(error: Error) {
    return { error };
  }
  render() {
    if (this.state.error) {
      return (
        <div style={{ padding: 40 }}>
          <Alert
            type="error"
            message="Page Error"
            description={this.state.error.message}
            showIcon
          />
          <pre style={{ marginTop: 16, fontSize: 12, color: '#999' }}>
            {this.state.error.stack}
          </pre>
        </div>
      );
    }
    return this.props.children;
  }
}

function CatalogProvider({ children }: { children: ReactNode }) {
  const [catalogs, setCatalogs] = useState<CatalogConfig[]>(loadCatalogs);
  const [activeName, setActiveName] = useState<string | null>(loadActiveName);
  const queryClient = useQueryClient();
  const navigate = useNavigate();

  const active = catalogs.find((c) => c.name === activeName) ?? null;

  // Auto-detect local REST server on first visit (no catalogs configured)
  useEffect(() => {
    if (catalogs.length > 0) return;
    let cancelled = false;
    (async () => {
      try {
        const resp = await fetch('/v1/config');
        if (!resp.ok) return;
        const data = await resp.json();
        if (cancelled) return;
        const prefix = data?.defaults?.prefix || '';
        const config: CatalogConfig = { name: 'Local', baseUrl: '', prefix };
        setCatalogs([config]);
        saveCatalogs([config]);
        setActiveName('Local');
        saveActiveName('Local');
      } catch { /* no local server available */ }
    })();
    return () => { cancelled = true; };
  }, []);  // eslint-disable-line react-hooks/exhaustive-deps

  // Sync current catalog to api client, auto-detect prefix if missing
  useEffect(() => {
    setCurrentCatalog(active);

    if (active && !active.prefix) {
      const fetchPrefix = async () => {
        try {
          const baseUrl = active.baseUrl.replace(/\/+$/, '');
          const url = baseUrl
            ? `/proxy?target=${encodeURIComponent(`${baseUrl}/v1/config`)}`
            : '/v1/config';
          const resp = await fetch(url);
          const data = await resp.json();
          if (data?.defaults?.prefix) {
            const detectedPrefix = data.defaults.prefix;
            setCatalogs((prev) => {
              const next = prev.map((c) =>
                c.name === active.name ? { ...c, prefix: detectedPrefix } : c
              );
              saveCatalogs(next);
              return next;
            });
          }
        } catch { /* ignore */ }
      };
      fetchPrefix();
    }
  }, [active]);

  const handleSetActive = useCallback(
    (name: string) => {
      setActiveName(name);
      saveActiveName(name);
      // Clear all query caches when switching catalog
      queryClient.clear();
      navigate('/');
    },
    [queryClient, navigate]
  );

  const addCatalog = useCallback((config: CatalogConfig) => {
    setCatalogs((prev) => {
      const next = [...prev, config];
      saveCatalogs(next);
      return next;
    });
    // Auto-activate if first catalog
    setActiveName((prev) => {
      if (!prev) {
        saveActiveName(config.name);
        return config.name;
      }
      return prev;
    });
  }, []);

  const updateCatalog = useCallback(
    (oldName: string, config: CatalogConfig) => {
      setCatalogs((prev) => {
        const next = prev.map((c) => (c.name === oldName ? config : c));
        saveCatalogs(next);
        return next;
      });
      if (activeName === oldName && config.name !== oldName) {
        setActiveName(config.name);
        saveActiveName(config.name);
      }
    },
    [activeName]
  );

  const removeCatalog = useCallback(
    (name: string) => {
      setCatalogs((prev) => {
        const next = prev.filter((c) => c.name !== name);
        saveCatalogs(next);
        // If removing active, switch to first remaining
        if (activeName === name) {
          const newActive = next.length > 0 ? next[0].name : null;
          setActiveName(newActive);
          if (newActive) saveActiveName(newActive);
          queryClient.clear();
        }
        return next;
      });
    },
    [activeName, queryClient]
  );

  const value: CatalogContextValue = {
    catalogs,
    active,
    setActive: handleSetActive,
    addCatalog,
    updateCatalog,
    removeCatalog,
  };

  return (
    <CatalogContext.Provider value={value}>{children}</CatalogContext.Provider>
  );
}

function App() {
  return (
    <ConfigProvider
      theme={{
        algorithm: theme.defaultAlgorithm,
        token: {
          colorPrimary: '#1677ff',
        },
      }}
    >
      <ErrorBoundary>
        <CatalogProvider>
          <Routes>
            <Route element={<AppLayout />}>
              <Route path="/" element={<DatabaseList />} />
              <Route path="/databases/:db" element={<DatabaseDetail />} />
              <Route path="/databases/:db/tables/:table" element={<TableDetail />} />
              <Route path="*" element={<Navigate to="/" replace />} />
            </Route>
          </Routes>
        </CatalogProvider>
      </ErrorBoundary>
    </ConfigProvider>
  );
}

export default App;
