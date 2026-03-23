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
import { Table, Button, Space, Modal, Form, Input, InputNumber, Popconfirm, message } from 'antd';
import { PlusOutlined, FastForwardOutlined } from '@ant-design/icons';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { listBranches, createBranch, dropBranch } from '../../api/branches';
import type { BranchInfo } from '../../api/types';

interface Props {
  database: string;
  table: string;
}

export default function BranchList({ database, table }: Props) {
  const queryClient = useQueryClient();
  const [createOpen, setCreateOpen] = useState(false);
  const [form] = Form.useForm();

  const { data = [], isLoading } = useQuery({
    queryKey: ['branches', database, table],
    queryFn: () => listBranches(database, table),
  });

  const invalidate = () => queryClient.invalidateQueries({ queryKey: ['branches', database, table] });

  const createMutation = useMutation({
    mutationFn: (req: { branch: string; fromTag?: string; fromSnapshotId?: number }) =>
      createBranch(database, table, req),
    onSuccess: () => {
      message.success('Branch created');
      invalidate();
      setCreateOpen(false);
      form.resetFields();
    },
    onError: (err: Error) => message.error(err.message || 'Failed to create branch'),
  });

  const dropMutation = useMutation({
    mutationFn: (branch: string) => dropBranch(database, table, branch),
    onSuccess: () => {
      message.success('Branch deleted');
      invalidate();
    },
    onError: (err: Error) => message.error(err.message || 'Failed to delete branch'),
  });

  const handleCreate = () => {
    form.validateFields().then((values) => {
      createMutation.mutate({
        branch: values.branch,
        fromTag: values.fromTag || undefined,
        fromSnapshotId: values.fromSnapshotId ?? undefined,
      });
    });
  };

  return (
    <div>
      <div style={{ marginBottom: 8, textAlign: 'right' }}>
        <Button type="primary" icon={<PlusOutlined />} onClick={() => { setCreateOpen(true); form.resetFields(); }}>
          Create Branch
        </Button>
      </div>
      <Table
        dataSource={data}
        rowKey="branch"
        loading={isLoading}
        pagination={false}
        size="small"
        columns={[
          { title: 'Branch Name', dataIndex: 'branch' },
          { title: 'Latest Snapshot ID', dataIndex: 'latestSnapshotId', render: (v) => v ?? '-' },
          { title: 'Latest Schema ID', dataIndex: 'latestSchemaId', render: (v) => v ?? '-' },
          {
            title: 'Actions',
            width: 200,
            render: (_: unknown, record: BranchInfo) => {
              if (record.branch === 'main') return null;
              return (
                <Space>
                  <Button size="small" icon={<FastForwardOutlined />} onClick={() => message.info('Fast forward is not supported yet')}>Fast Forward</Button>
                  <Popconfirm
                    title={`Delete branch "${record.branch}"?`}
                    description="This action cannot be undone."
                    onConfirm={() => dropMutation.mutate(record.branch)}
                    okText="Delete"
                    okButtonProps={{ danger: true }}
                  >
                    <Button size="small" danger>Delete</Button>
                  </Popconfirm>
                </Space>
              );
            },
          },
        ]}
      />

      <Modal
        title="Create Branch"
        open={createOpen}
        onOk={handleCreate}
        onCancel={() => { setCreateOpen(false); form.resetFields(); }}
        confirmLoading={createMutation.isPending}
      >
        <Form form={form} layout="vertical">
          <Form.Item
            name="branch"
            label="Branch Name"
            rules={[{ required: true, message: 'Please enter a branch name' }]}
          >
            <Input placeholder="my_branch" />
          </Form.Item>
          <Form.Item name="fromTag" label="From Tag (optional)">
            <Input placeholder="tag_name" />
          </Form.Item>
          <Form.Item name="fromSnapshotId" label="From Snapshot ID (optional)">
            <InputNumber style={{ width: '100%' }} placeholder="e.g. 1" />
          </Form.Item>
        </Form>
      </Modal>
    </div>
  );
}
