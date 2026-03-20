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

  // Sync current catalog to api client
  useEffect(() => {
    setCurrentCatalog(active);
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
