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

import { useState } from 'react';
import { Layout, Breadcrumb, Select, Button, Space } from 'antd';
import { HomeOutlined, SettingOutlined, PlusOutlined } from '@ant-design/icons';
import { useLocation, Link } from 'react-router-dom';
import { useCatalog } from '../../store/catalogStore';
import CatalogManager from '../CatalogManager';

const { Header } = Layout;

export default function AppHeader() {
  const location = useLocation();
  const { catalogs, active, setActive } = useCatalog();
  const [managerOpen, setManagerOpen] = useState(false);

  const pathParts = location.pathname.split('/').filter(Boolean);

  const breadcrumbItems = [
    {
      title: (
        <Link to="/">
          <HomeOutlined /> Home
        </Link>
      ),
    },
  ];

  if (pathParts[0] === 'databases' && pathParts[1]) {
    const db = decodeURIComponent(pathParts[1]);
    breadcrumbItems.push({
      title: <Link to={`/databases/${db}`}>{db}</Link>,
    });

    if (pathParts[2] === 'tables' && pathParts[3]) {
      const table = decodeURIComponent(pathParts[3]);
      breadcrumbItems.push({
        title: <span>{table}</span>,
      });
    }
  }

  const catalogOptions = catalogs.map((c) => ({
    value: c.name,
    label: c.name,
  }));

  return (
    <>
      <Header
        style={{
          background: '#fff',
          padding: '0 24px',
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'space-between',
          borderBottom: '1px solid #f0f0f0',
        }}
      >
        <Breadcrumb items={breadcrumbItems} />
        <Space>
          {catalogs.length > 0 && (
            <Select
              value={active?.name}
              onChange={setActive}
              options={catalogOptions}
              style={{ minWidth: 160 }}
              placeholder="Select Catalog"
            />
          )}
          {catalogs.length === 0 && (
            <Button
              type="primary"
              icon={<PlusOutlined />}
              onClick={() => setManagerOpen(true)}
            >
              Add Catalog
            </Button>
          )}
          <Button
            icon={<SettingOutlined />}
            onClick={() => setManagerOpen(true)}
          />
        </Space>
      </Header>
      <CatalogManager open={managerOpen} onClose={() => setManagerOpen(false)} />
    </>
  );
}
