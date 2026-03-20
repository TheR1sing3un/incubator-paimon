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
