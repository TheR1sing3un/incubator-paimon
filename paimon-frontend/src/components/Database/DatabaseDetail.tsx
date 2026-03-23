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
import {
  Table, Typography, Descriptions, Card, Spin, Alert, Button, Space,
  Popconfirm, Modal, Form, Input, Select, message, Drawer, Checkbox,
} from 'antd';
import { TableOutlined, PlusOutlined, DeleteOutlined, EditOutlined, MinusCircleOutlined } from '@ant-design/icons';
import { useParams, useNavigate } from 'react-router-dom';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { getDatabase, dropDatabase, alterDatabase } from '../../api/databases';
import { listTables, createTable, dropTable, renameTable } from '../../api/tables';

const { Title } = Typography;

export default function DatabaseDetail() {
  const { db } = useParams<{ db: string }>();
  const navigate = useNavigate();
  const queryClient = useQueryClient();

  const [editPropsOpen, setEditPropsOpen] = useState(false);
  const [createTableOpen, setCreateTableOpen] = useState(false);
  const [renameOpen, setRenameOpen] = useState(false);
  const [renameTarget, setRenameTarget] = useState('');
  const [propsForm] = Form.useForm();
  const [tableForm] = Form.useForm();
  const [renameForm] = Form.useForm();

  const { data: dbInfo, isLoading: dbLoading, error: dbError } = useQuery({
    queryKey: ['database', db],
    queryFn: () => getDatabase(db!),
    enabled: !!db,
  });

  const { data: tables = [], isLoading: tablesLoading } = useQuery({
    queryKey: ['tables', db],
    queryFn: () => listTables(db!),
    enabled: !!db,
  });

  const dropDbMutation = useMutation({
    mutationFn: () => dropDatabase(db!),
    onSuccess: () => {
      message.success('Database deleted');
      queryClient.invalidateQueries({ queryKey: ['databases'] });
      navigate('/');
    },
    onError: (err: Error) => message.error(err.message || 'Failed to delete database'),
  });

  const alterDbMutation = useMutation({
    mutationFn: (params: { removals?: string[]; updates?: Record<string, string> }) =>
      alterDatabase(db!, params),
    onSuccess: () => {
      message.success('Properties updated');
      queryClient.invalidateQueries({ queryKey: ['database', db] });
      setEditPropsOpen(false);
    },
    onError: (err: Error) => message.error(err.message || 'Failed to update properties'),
  });

  const createTableMutation = useMutation({
    mutationFn: createTable,
    onSuccess: () => {
      message.success('Table created');
      queryClient.invalidateQueries({ queryKey: ['tables', db] });
      setCreateTableOpen(false);
      tableForm.resetFields();
    },
    onError: (err: Error) => message.error(err.message || 'Failed to create table'),
  });

  const dropTableMutation = useMutation({
    mutationFn: (tableName: string) => dropTable(db!, tableName),
    onSuccess: () => {
      message.success('Table deleted');
      queryClient.invalidateQueries({ queryKey: ['tables', db] });
      queryClient.invalidateQueries({ queryKey: ['databases'] });
    },
    onError: (err: Error) => message.error(err.message || 'Failed to delete table'),
  });

  const renameMutation = useMutation({
    mutationFn: renameTable,
    onSuccess: () => {
      message.success('Table renamed');
      queryClient.invalidateQueries({ queryKey: ['tables'] });
      queryClient.invalidateQueries({ queryKey: ['databases'] });
      setRenameOpen(false);
      renameForm.resetFields();
    },
    onError: (err: Error) => message.error(err.message || 'Failed to rename table'),
  });

  const handleEditProps = () => {
    const currentProps = dbInfo?.properties ?? {};
    propsForm.setFieldsValue({
      properties: Object.entries(currentProps).map(([key, value]) => ({ key, value })),
    });
    setEditPropsOpen(true);
  };

  const handleSaveProps = () => {
    propsForm.validateFields().then((values) => {
      const oldKeys = Object.keys(dbInfo?.properties ?? {});
      const newProps: Record<string, string> = {};
      const newKeys: string[] = [];
      if (values.properties) {
        for (const item of values.properties) {
          if (item.key) {
            newProps[item.key] = item.value ?? '';
            newKeys.push(item.key);
          }
        }
      }
      const removals = oldKeys.filter((k) => !newKeys.includes(k));
      alterDbMutation.mutate({
        removals: removals.length > 0 ? removals : undefined,
        updates: Object.keys(newProps).length > 0 ? newProps : undefined,
      });
    });
  };

  const handleCreateTable = () => {
    tableForm.validateFields().then((values) => {
      const fields = (values.fields || []).map(
        (f: { name: string; type: string; nullable?: boolean; description?: string }, idx: number) => ({
          id: idx,
          name: f.name,
          type: f.nullable === false ? f.type + ' NOT NULL' : f.type,
          description: f.description || undefined,
        })
      );
      const options: Record<string, string> = {};
      if (values.options) {
        for (const item of values.options) {
          if (item.key && item.value) options[item.key] = item.value;
        }
      }
      createTableMutation.mutate({
        identifier: { database: db!, object: values.tableName },
        schema: {
          fields,
          partitionKeys: values.partitionKeys ?? [],
          primaryKeys: values.primaryKeys ?? [],
          options,
          comment: values.comment || '',
        },
      });
    });
  };

  const handleRename = (tableName: string) => {
    setRenameTarget(tableName);
    renameForm.setFieldsValue({ destDb: db, destTable: tableName });
    setRenameOpen(true);
  };

  const handleRenameSubmit = () => {
    renameForm.validateFields().then((values) => {
      renameMutation.mutate({
        source: { database: db!, object: renameTarget },
        destination: { database: values.destDb, object: values.destTable },
      });
    });
  };

  // Watch fields for partition/primary key selectors
  const fieldNames: string[] = Form.useWatch('fields', tableForm)
    ?.map((f: { name?: string }) => f?.name)
    .filter(Boolean) ?? [];

  if (dbError) {
    return <Alert type="error" message="Failed to load database" description={String(dbError)} />;
  }

  return (
    <div>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 8 }}>
        <Title level={3} style={{ margin: 0 }}>Database: {db}</Title>
        <Space>
          <Button icon={<EditOutlined />} onClick={handleEditProps}>Edit Properties</Button>
          <Popconfirm
            title="Delete this database?"
            description="This action cannot be undone."
            onConfirm={() => dropDbMutation.mutate()}
            okText="Delete"
            okButtonProps={{ danger: true }}
          >
            <Button danger icon={<DeleteOutlined />}>Delete Database</Button>
          </Popconfirm>
        </Space>
      </div>

      {dbLoading ? (
        <Spin />
      ) : dbInfo ? (
        <Card size="small" style={{ marginBottom: 16 }}>
          <Descriptions column={2} size="small">
            {dbInfo.owner && <Descriptions.Item label="Owner">{dbInfo.owner}</Descriptions.Item>}
            {dbInfo.comment && <Descriptions.Item label="Comment">{dbInfo.comment}</Descriptions.Item>}
            {dbInfo.properties &&
              Object.entries(dbInfo.properties).map(([k, v]) => (
                <Descriptions.Item key={k} label={k}>
                  {v}
                </Descriptions.Item>
              ))}
          </Descriptions>
        </Card>
      ) : null}

      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 8 }}>
        <Title level={4} style={{ margin: 0 }}>Tables</Title>
        <Button type="primary" icon={<PlusOutlined />} onClick={() => { setCreateTableOpen(true); tableForm.resetFields(); }}>
          Create Table
        </Button>
      </div>
      <Table
        loading={tablesLoading}
        dataSource={tables.map((t) => ({ name: t }))}
        rowKey="name"
        columns={[
          {
            title: 'Table Name',
            dataIndex: 'name',
            render: (name: string) => (
              <a onClick={() => navigate(`/databases/${db}/tables/${name}`)}>
                <TableOutlined style={{ marginRight: 8 }} />
                {name}
              </a>
            ),
          },
          {
            title: 'Actions',
            width: 200,
            render: (_: unknown, record: { name: string }) => (
              <Space>
                <Button size="small" onClick={() => handleRename(record.name)}>Rename</Button>
                <Popconfirm
                  title={`Delete table "${record.name}"?`}
                  description="This action cannot be undone."
                  onConfirm={() => dropTableMutation.mutate(record.name)}
                  okText="Delete"
                  okButtonProps={{ danger: true }}
                >
                  <Button size="small" danger>Delete</Button>
                </Popconfirm>
              </Space>
            ),
          },
        ]}
        pagination={false}
      />

      {/* Edit Properties Modal */}
      <Modal
        title="Edit Database Properties"
        open={editPropsOpen}
        onOk={handleSaveProps}
        onCancel={() => setEditPropsOpen(false)}
        confirmLoading={alterDbMutation.isPending}
      >
        <Form form={propsForm} layout="vertical">
          <Form.List name="properties">
            {(fields, { add, remove }) => (
              <>
                {fields.map(({ key, name, ...restField }) => (
                  <Space key={key} style={{ display: 'flex', marginBottom: 8 }} align="baseline">
                    <Form.Item {...restField} name={[name, 'key']} noStyle>
                      <Input placeholder="Key" />
                    </Form.Item>
                    <Form.Item {...restField} name={[name, 'value']} noStyle>
                      <Input placeholder="Value" />
                    </Form.Item>
                    <MinusCircleOutlined onClick={() => remove(name)} />
                  </Space>
                ))}
                <Button type="dashed" onClick={() => add()} block icon={<PlusOutlined />}>
                  Add Property
                </Button>
              </>
            )}
          </Form.List>
        </Form>
      </Modal>

      {/* Create Table Drawer */}
      <Drawer
        title="Create Table"
        open={createTableOpen}
        onClose={() => { setCreateTableOpen(false); tableForm.resetFields(); }}
        width={640}
        extra={
          <Button type="primary" onClick={handleCreateTable} loading={createTableMutation.isPending}>
            Create
          </Button>
        }
      >
        <Form form={tableForm} layout="vertical">
          <Form.Item
            name="tableName"
            label="Table Name"
            rules={[{ required: true, message: 'Please enter a table name' }]}
          >
            <Input placeholder="my_table" />
          </Form.Item>

          <Form.Item label="Fields" required>
            <Form.List name="fields" rules={[{
              validator: async (_, fields) => {
                if (!fields || fields.length < 1) throw new Error('At least one field is required');
              },
            }]}>
              {(fields, { add, remove }, { errors }) => (
                <>
                  {fields.map(({ key, name, ...restField }) => (
                    <Space key={key} style={{ display: 'flex', marginBottom: 8 }} align="start">
                      <Form.Item
                        {...restField}
                        name={[name, 'name']}
                        rules={[{ required: true, message: 'Name required' }]}
                        noStyle
                      >
                        <Input placeholder="Field name" style={{ width: 140 }} />
                      </Form.Item>
                      <Form.Item
                        {...restField}
                        name={[name, 'type']}
                        rules={[{ required: true, message: 'Type required' }]}
                        noStyle
                      >
                        <Input placeholder="INT, STRING, ..." style={{ width: 140 }} />
                      </Form.Item>
                      <Form.Item {...restField} name={[name, 'nullable']} valuePropName="checked" initialValue={true} noStyle>
                        <Checkbox>Nullable</Checkbox>
                      </Form.Item>
                      <Form.Item {...restField} name={[name, 'description']} noStyle>
                        <Input placeholder="Description" style={{ width: 140 }} />
                      </Form.Item>
                      <MinusCircleOutlined onClick={() => remove(name)} />
                    </Space>
                  ))}
                  <Form.Item>
                    <Button type="dashed" onClick={() => add()} block icon={<PlusOutlined />}>
                      Add Field
                    </Button>
                    <Form.ErrorList errors={errors} />
                  </Form.Item>
                </>
              )}
            </Form.List>
          </Form.Item>

          <Form.Item name="partitionKeys" label="Partition Keys">
            <Select
              mode="multiple"
              placeholder="Select partition columns"
              options={fieldNames.map((n) => ({ value: n, label: n }))}
            />
          </Form.Item>

          <Form.Item name="primaryKeys" label="Primary Keys">
            <Select
              mode="multiple"
              placeholder="Select primary key columns"
              options={fieldNames.map((n) => ({ value: n, label: n }))}
            />
          </Form.Item>

          <Form.Item label="Options">
            <Form.List name="options">
              {(fields, { add, remove }) => (
                <>
                  {fields.map(({ key, name, ...restField }) => (
                    <Space key={key} style={{ display: 'flex', marginBottom: 8 }} align="baseline">
                      <Form.Item {...restField} name={[name, 'key']} noStyle>
                        <Input placeholder="Key" />
                      </Form.Item>
                      <Form.Item {...restField} name={[name, 'value']} noStyle>
                        <Input placeholder="Value" />
                      </Form.Item>
                      <MinusCircleOutlined onClick={() => remove(name)} />
                    </Space>
                  ))}
                  <Button type="dashed" onClick={() => add()} block icon={<PlusOutlined />}>
                    Add Option
                  </Button>
                </>
              )}
            </Form.List>
          </Form.Item>

          <Form.Item name="comment" label="Comment">
            <Input.TextArea placeholder="Table description" rows={2} />
          </Form.Item>
        </Form>
      </Drawer>

      {/* Rename Table Modal */}
      <Modal
        title={`Rename Table: ${renameTarget}`}
        open={renameOpen}
        onOk={handleRenameSubmit}
        onCancel={() => { setRenameOpen(false); renameForm.resetFields(); }}
        confirmLoading={renameMutation.isPending}
      >
        <Form form={renameForm} layout="vertical">
          <Form.Item
            name="destDb"
            label="Destination Database"
            rules={[{ required: true }]}
          >
            <Input />
          </Form.Item>
          <Form.Item
            name="destTable"
            label="New Table Name"
            rules={[{ required: true, message: 'Please enter the new table name' }]}
          >
            <Input />
          </Form.Item>
        </Form>
      </Modal>
    </div>
  );
}
