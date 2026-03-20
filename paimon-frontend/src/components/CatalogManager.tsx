import { useState } from 'react';
import { Modal, Form, Input, Button, Table, Space, Popconfirm, message, Tag } from 'antd';
import { PlusOutlined, EditOutlined, DeleteOutlined, CheckCircleOutlined } from '@ant-design/icons';
import { useCatalog, type CatalogConfig } from '../store/catalogStore';
import axios from 'axios';

interface Props {
  open: boolean;
  onClose: () => void;
}

export default function CatalogManager({ open, onClose }: Props) {
  const { catalogs, active, addCatalog, updateCatalog, removeCatalog } = useCatalog();
  const [formOpen, setFormOpen] = useState(false);
  const [editing, setEditing] = useState<string | null>(null);
  const [testing, setTesting] = useState(false);
  const [form] = Form.useForm<CatalogConfig>();

  const handleAdd = () => {
    setEditing(null);
    form.resetFields();
    setFormOpen(true);
  };

  const handleEdit = (record: CatalogConfig) => {
    setEditing(record.name);
    form.setFieldsValue(record);
    setFormOpen(true);
  };

  const handleSave = async () => {
    try {
      const values = await form.validateFields();
      values.baseUrl = values.baseUrl.replace(/\/+$/, '');
      if (editing) {
        updateCatalog(editing, values);
      } else {
        if (catalogs.some((c) => c.name === values.name)) {
          message.error(`Catalog "${values.name}" already exists`);
          return;
        }
        addCatalog(values);
      }
      setFormOpen(false);
      message.success('Saved');
    } catch {
      // validation error
    }
  };

  const handleTest = async () => {
    try {
      const values = await form.validateFields();
      const baseUrl = values.baseUrl.replace(/\/+$/, '');
      setTesting(true);
      const targetUrl = `${baseUrl}/v1/config`;
      const url = `/proxy?target=${encodeURIComponent(targetUrl)}`;
      const resp = await axios.get(url, { timeout: 10000 });
      if (resp.data?.defaults) {
        const serverPrefix = resp.data.defaults.prefix;
        message.success(`Connected! Server prefix: "${serverPrefix}"`);
        if (serverPrefix) {
          form.setFieldValue('prefix', serverPrefix);
        }
      } else {
        message.success('Connected');
      }
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      message.error(`Connection failed: ${msg}`);
    } finally {
      setTesting(false);
    }
  };

  const columns = [
    {
      title: 'Name',
      dataIndex: 'name',
      render: (name: string) => (
        <span>
          {name}
          {active?.name === name && (
            <Tag color="green" style={{ marginLeft: 8 }}>
              Active
            </Tag>
          )}
        </span>
      ),
    },
    { title: 'URL', dataIndex: 'baseUrl', ellipsis: true },
    { title: 'Prefix', dataIndex: 'prefix', width: 120 },
    {
      title: 'Actions',
      width: 120,
      render: (_: unknown, record: CatalogConfig) => (
        <Space size="small">
          <Button
            type="text"
            size="small"
            icon={<EditOutlined />}
            onClick={() => handleEdit(record)}
          />
          <Popconfirm
            title="Delete this catalog?"
            onConfirm={() => removeCatalog(record.name)}
          >
            <Button type="text" size="small" danger icon={<DeleteOutlined />} />
          </Popconfirm>
        </Space>
      ),
    },
  ];

  return (
    <>
      <Modal
        title="Manage Catalogs"
        open={open}
        onCancel={onClose}
        footer={null}
        width={700}
      >
        <div style={{ marginBottom: 16 }}>
          <Button type="primary" icon={<PlusOutlined />} onClick={handleAdd}>
            Add Catalog
          </Button>
        </div>
        <Table
          dataSource={catalogs}
          columns={columns}
          rowKey="name"
          pagination={false}
          size="small"
        />
      </Modal>

      <Modal
        title={editing ? 'Edit Catalog' : 'Add Catalog'}
        open={formOpen}
        onCancel={() => setFormOpen(false)}
        footer={
          <Space>
            <Button onClick={() => setFormOpen(false)}>Cancel</Button>
            <Button
              icon={<CheckCircleOutlined />}
              onClick={handleTest}
              loading={testing}
            >
              Test Connection
            </Button>
            <Button type="primary" onClick={handleSave}>
              Save
            </Button>
          </Space>
        }
      >
        <Form form={form} layout="vertical">
          <Form.Item
            name="name"
            label="Name"
            rules={[{ required: true, message: 'Please enter a name' }]}
          >
            <Input placeholder="e.g. Production" disabled={!!editing} />
          </Form.Item>
          <Form.Item
            name="baseUrl"
            label="Server URL"
            rules={[{ required: true, message: 'Please enter the server URL' }]}
          >
            <Input placeholder="e.g. https://paimon-server.example.com" />
          </Form.Item>
          <Form.Item
            name="prefix"
            label="Prefix"
            rules={[{ required: true, message: 'Click "Test Connection" to auto-detect, or enter manually' }]}
            tooltip="The REST catalog prefix. Click 'Test Connection' to auto-detect."
          >
            <Input placeholder="e.g. paimon (auto-detected on test)" />
          </Form.Item>
        </Form>
      </Modal>
    </>
  );
}
