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

import { useState, useEffect } from 'react';
import { Layout, Menu, Spin, Empty } from 'antd';
import { DatabaseOutlined, TableOutlined } from '@ant-design/icons';
import { useNavigate, useLocation } from 'react-router-dom';
import { useQuery } from '@tanstack/react-query';
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
  const [loadedDbs, setLoadedDbs] = useState<Set<string>>(new Set());
  const [dbTables, setDbTables] = useState<Record<string, string[]>>({});

  const { data: databases = [], isLoading } = useQuery({
    queryKey: ['databases', active?.name],
    queryFn: listDatabases,
    enabled: !!active,
  });

  // Reset sidebar state when catalog changes
  useEffect(() => {
    setOpenKeys([]);
    setLoadedDbs(new Set());
    setDbTables({});
  }, [active?.name]);

  // Auto-expand current database from URL
  useEffect(() => {
    const match = location.pathname.match(/^\/databases\/([^/]+)/);
    if (match) {
      const db = decodeURIComponent(match[1]);
      setOpenKeys((prev) => (prev.includes(db) ? prev : [...prev, db]));
    }
  }, [location.pathname]);

  // Load tables when a database is expanded
  useEffect(() => {
    for (const key of openKeys) {
      if (!loadedDbs.has(key)) {
        setLoadedDbs((prev) => new Set(prev).add(key));
        listTables(key).then((tables) => {
          setDbTables((prev) => ({ ...prev, [key]: tables }));
        });
      }
    }
  }, [openKeys, loadedDbs]);

  const menuItems: ItemType[] = databases.map((db) => ({
    key: db,
    icon: <DatabaseOutlined />,
    label: db,
    children: dbTables[db]
      ? dbTables[db].map((t) => ({
          key: `${db}/${t}`,
          icon: <TableOutlined />,
          label: t,
        }))
      : [{ key: `${db}/__loading`, label: 'Loading...', disabled: true }],
  }));

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
    return (
      <Menu
        mode="inline"
        items={menuItems}
        openKeys={collapsed ? [] : openKeys}
        onOpenChange={setOpenKeys}
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
      {renderContent()}
    </Sider>
  );
}
