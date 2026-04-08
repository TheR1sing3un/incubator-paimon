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

import { useState, useEffect, useMemo } from 'react';
import { Layout, Menu, Spin, Empty, Input } from 'antd';
import { DatabaseOutlined, TableOutlined, SearchOutlined } from '@ant-design/icons';
import { useNavigate, useLocation } from 'react-router-dom';
import { useQuery, useQueries } from '@tanstack/react-query';
import { listDatabases } from '../../api/databases';
import { listTables } from '../../api/tables';
import { useCatalog } from '../../store/catalogStore';
import type { ItemType } from 'antd/es/menu/interface';

const { Sider } = Layout;

export default function Sidebar() {
  const [collapsed, setCollapsed] = useState(false);
  const navigate = useNavigate();
  const location = useLocation();
  const { active } = useCatalog();
  const [openKeys, setOpenKeys] = useState<string[]>([]);
  const [query, setQuery] = useState('');

  const { data: databases = [], isLoading } = useQuery({
    queryKey: ['databases', active?.name],
    queryFn: listDatabases,
    enabled: !!active,
  });

  // Reset sidebar state when catalog changes
  useEffect(() => {
    setOpenKeys([]);
    setQuery('');
  }, [active?.name]);

  // Auto-expand current database from URL
  useEffect(() => {
    const match = location.pathname.match(/^\/databases\/([^/]+)/);
    if (match) {
      const db = decodeURIComponent(match[1]);
      setOpenKeys((prev) => (prev.includes(db) ? prev : [...prev, db]));
    }
  }, [location.pathname]);

  // Load tables via React Query for each expanded database
  const tableQueries = useQueries({
    queries: openKeys.map((db) => ({
      queryKey: ['tables', db],
      queryFn: () => listTables(db),
    })),
  });

  const dbTables = useMemo(() => {
    const result: Record<string, string[] | undefined> = {};
    openKeys.forEach((db, idx) => {
      result[db] = tableQueries[idx].data;
    });
    return result;
  }, [openKeys, tableQueries]);

  const trimmedQuery = query.trim().toLowerCase();
  const isSearching = trimmedQuery.length > 0;

  const { menuItems, searchMatchedDbs } = useMemo(() => {
    const buildChildren = (db: string, tableFilter?: (t: string) => boolean) => {
      const loaded = dbTables[db];
      if (!loaded) {
        return [{ key: `${db}/__loading`, label: 'Loading...', disabled: true }];
      }
      const tables = tableFilter ? loaded.filter(tableFilter) : loaded;
      return tables.map((t) => ({
        key: `${db}/${t}`,
        icon: <TableOutlined />,
        label: t,
      }));
    };

    if (!isSearching) {
      const items: ItemType[] = databases.map((db) => ({
        key: db,
        icon: <DatabaseOutlined />,
        label: db,
        children: buildChildren(db),
      }));
      return { menuItems: items, searchMatchedDbs: [] as string[] };
    }

    const matchedDbs: string[] = [];
    const items: ItemType[] = [];
    for (const db of databases) {
      const dbHit = db.toLowerCase().includes(trimmedQuery);
      const loaded = dbTables[db];
      const tableHits = loaded
        ? loaded.filter((t) => t.toLowerCase().includes(trimmedQuery))
        : [];
      const hasTableHit = tableHits.length > 0;

      if (!dbHit && !hasTableHit) continue;

      matchedDbs.push(db);
      items.push({
        key: db,
        icon: <DatabaseOutlined />,
        label: db,
        // If db name matches, show all loaded tables; otherwise show only matching tables.
        children: dbHit
          ? buildChildren(db)
          : buildChildren(db, (t) => t.toLowerCase().includes(trimmedQuery)),
      });
    }
    return { menuItems: items, searchMatchedDbs: matchedDbs };
  }, [databases, dbTables, isSearching, trimmedQuery]);

  const effectiveOpenKeys = collapsed
    ? []
    : isSearching
      ? searchMatchedDbs
      : openKeys;

  const handleClick = (info: { key: string }) => {
    const key = info.key;
    if (key.includes('/')) {
      const [db, table] = key.split('/');
      navigate(`/databases/${db}/tables/${table}`);
    } else {
      navigate(`/databases/${key}`);
    }
  };

  const renderContent = () => {
    if (!active) {
      return (
        <div style={{ padding: 16, textAlign: 'center' }}>
          <Empty
            image={Empty.PRESENTED_IMAGE_SIMPLE}
            description="No catalog configured. Click the + button in the header to add one."
          />
        </div>
      );
    }
    if (isLoading) {
      return (
        <div style={{ padding: 24, textAlign: 'center' }}>
          <Spin />
        </div>
      );
    }
    if (menuItems.length === 0) {
      return (
        <div style={{ padding: 24, textAlign: 'center' }}>
          <Empty image={Empty.PRESENTED_IMAGE_SIMPLE} description="No matches" />
        </div>
      );
    }
    return (
      <Menu
        mode="inline"
        items={menuItems}
        openKeys={effectiveOpenKeys}
        onOpenChange={isSearching ? undefined : setOpenKeys}
        onClick={handleClick}
        style={{ borderRight: 0 }}
      />
    );
  };

  return (
    <Sider
      width={240}
      collapsible
      collapsed={collapsed}
      onCollapse={setCollapsed}
      style={{ background: '#fff' }}
    >
      <div
        style={{
          height: 32,
          margin: 16,
          fontWeight: 'bold',
          fontSize: collapsed ? 14 : 18,
          textAlign: 'center',
          whiteSpace: 'nowrap',
          overflow: 'hidden',
        }}
      >
        {collapsed ? 'P' : 'Paimon'}
      </div>
      {!collapsed && active && (
        <div style={{ padding: '0 12px 8px' }}>
          <Input
            allowClear
            size="small"
            placeholder="Search db / table"
            prefix={<SearchOutlined />}
            value={query}
            onChange={(e) => setQuery(e.target.value)}
          />
        </div>
      )}
      {renderContent()}
    </Sider>
  );
}
