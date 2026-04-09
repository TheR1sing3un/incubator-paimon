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
import { Card, List, Typography, Empty, Button, Modal, Form, Input, Space, message } from 'antd';
import { DatabaseOutlined, PlusOutlined, MinusCircleOutlined, SearchOutlined, LeftOutlined, RightOutlined } from '@ant-design/icons';
import { useMutation } from '@tanstack/react-query';
import { useNavigate } from 'react-router-dom';
import { listDatabasesPaged, createDatabase } from '../../api/databases';
import { useCatalog } from '../../store/catalogStore';
import { usePagedData } from '../../hooks/usePagedData';

const { Title } = Typography;

export default function DatabaseList() {
  const navigate = useNavigate();
  const { active } = useCatalog();
  const [createOpen, setCreateOpen] = useState(false);
  const [form] = Form.useForm();
  const [dbQuery, setDbQuery] = useState('');
  const [debouncedQuery, setDebouncedQuery] = useState('');

  useEffect(() => {
    setDbQuery('');
    setDebouncedQuery('');
  }, [active?.name]);

  useEffect(() => {
    const timer = setTimeout(() => setDebouncedQuery(dbQuery), 300);
    return () => clearTimeout(timer);
  }, [dbQuery]);

  const databaseNamePattern = debouncedQuery.trim() ? `%${debouncedQuery.trim()}%` : undefined;

  const { data: databases, isLoading, hasNext, hasPrev, pageIndex, goNext, goPrev, refetch: refetchDatabases } =
    usePagedData<string>({
      queryKey: ['databases', active?.name ?? '', databaseNamePattern ?? ''],
      fetcher: (pageToken) =>
        listDatabasesPaged(pageToken, databaseNamePattern).then((r) => ({
          data: r.databases,
          nextPageToken: r.nextPageToken,
        })),
      enabled: !!active,
    });

  const createMutation = useMutation({
    mutationFn: createDatabase,
    onSuccess: () => {
      message.success('Database created successfully');
      refetchDatabases();
      setCreateOpen(false);
      form.resetFields();
    },
    onError: (err: Error) => {
      message.error(err.message || 'Failed to create database');
    },
  });

  const handleCreate = () => {
    form.validateFields().then((values) => {
      const options: Record<string, string> = {};
      if (values.options) {
        for (const item of values.options) {
          if (item.key && item.value) {
            options[item.key] = item.value;
          }
        }
      }
      createMutation.mutate({
        name: values.name,
        options: Object.keys(options).length > 0 ? options : undefined,
      });
    });
  };

  if (!active) {
    return (
      <div style={{ textAlign: 'center', paddingTop: 80 }}>
        <Empty description="Please add a Catalog connection using the header button to get started." />
      </div>
    );
  }

  return (
    <div>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 16 }}>
        <Title level={3} style={{ margin: 0 }}>Databases</Title>
        <Space>
          <Input
            allowClear
            placeholder="Search databases"
            prefix={<SearchOutlined />}
            value={dbQuery}
            onChange={(e) => setDbQuery(e.target.value)}
            style={{ width: 240 }}
          />
          <Button type="primary" icon={<PlusOutlined />} onClick={() => setCreateOpen(true)}>
            Create Database
          </Button>
        </Space>
      </div>
      <List
        grid={{ gutter: 16, xs: 1, sm: 2, md: 3, lg: 4, xl: 4 }}
        loading={isLoading}
        dataSource={databases}
        renderItem={(db) => (
          <List.Item>
            <Card
              hoverable
              onClick={() => navigate(`/databases/${db}`)}
              style={{ textAlign: 'center' }}
            >
              <DatabaseOutlined style={{ fontSize: 32, color: '#1677ff' }} />
              <div style={{ marginTop: 8, fontWeight: 500 }}>{db}</div>
            </Card>
          </List.Item>
        )}
      />
      <Space style={{ marginTop: 12 }}>
        <Button icon={<LeftOutlined />} disabled={!hasPrev} onClick={goPrev}>
          Prev
        </Button>
        <span>Page {pageIndex + 1}</span>
        <Button icon={<RightOutlined />} disabled={!hasNext} onClick={goNext}>
          Next
        </Button>
      </Space>

      <Modal
        title="Create Database"
        open={createOpen}
        onOk={handleCreate}
        onCancel={() => { setCreateOpen(false); form.resetFields(); }}
        confirmLoading={createMutation.isPending}
      >
        <Form form={form} layout="vertical">
          <Form.Item
            name="name"
            label="Database Name"
            rules={[{ required: true, message: 'Please enter a database name' }]}
          >
            <Input placeholder="my_database" />
          </Form.Item>
          <Form.List name="options">
            {(fields, { add, remove }) => (
              <>
                <div style={{ marginBottom: 8 }}>Options (optional)</div>
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
        </Form>
      </Modal>
    </div>
  );
}
