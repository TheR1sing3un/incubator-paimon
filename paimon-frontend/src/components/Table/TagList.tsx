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
import { Table, Tag, Button, Modal, Form, Input, InputNumber, Popconfirm, message } from 'antd';
import { PlusOutlined } from '@ant-design/icons';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { listTags, createTag, dropTag } from '../../api/tags';
import { formatTimestamp, formatNumber } from '../../utils/format';
import type { TagInfo } from '../../api/types';

interface Props {
  database: string;
  table: string;
  branch?: string;
}

export default function TagList({ database, table, branch }: Props) {
  const queryClient = useQueryClient();
  const [createOpen, setCreateOpen] = useState(false);
  const [form] = Form.useForm();

  const branchKey = branch ?? 'main';

  const { data = [], isLoading } = useQuery({
    queryKey: ['tags', database, table, branchKey],
    queryFn: () => listTags(database, table, branch),
  });

  const invalidate = () => queryClient.invalidateQueries({ queryKey: ['tags', database, table, branchKey] });

  const createMutation = useMutation({
    mutationFn: (req: { tagName: string; snapshotId?: number; timeRetained?: string }) =>
      createTag(database, table, req, branch),
    onSuccess: () => {
      message.success('Tag created');
      invalidate();
      setCreateOpen(false);
      form.resetFields();
    },
    onError: (err: Error) => message.error(err.message || 'Failed to create tag'),
  });

  const dropMutation = useMutation({
    mutationFn: (tagName: string) => dropTag(database, table, tagName, branch),
    onSuccess: () => {
      message.success('Tag deleted');
      invalidate();
    },
    onError: (err: Error) => message.error(err.message || 'Failed to delete tag'),
  });

  const handleCreate = () => {
    form.validateFields().then((values) => {
      createMutation.mutate({
        tagName: values.tagName,
        snapshotId: values.snapshotId ?? undefined,
        timeRetained: values.timeRetained || undefined,
      });
    });
  };

  return (
    <div>
      <div style={{ marginBottom: 8, display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
        <span>Branch: <Tag>{branchKey}</Tag></span>
        <Button type="primary" icon={<PlusOutlined />} onClick={() => { setCreateOpen(true); form.resetFields(); }}>
          Create Tag
        </Button>
      </div>
      <Table
        dataSource={data}
        rowKey="tagName"
        loading={isLoading}
        pagination={false}
        size="small"
        columns={[
          { title: 'Tag Name', dataIndex: 'tagName' },
          {
            title: 'Snapshot ID',
            render: (_: unknown, r: TagInfo) => r.snapshot?.id ?? '-',
          },
          {
            title: 'Schema ID',
            render: (_: unknown, r: TagInfo) => r.snapshot?.schemaId ?? '-',
          },
          {
            title: 'Commit Kind',
            render: (_: unknown, r: TagInfo) =>
              r.snapshot?.commitKind ? <Tag>{r.snapshot.commitKind}</Tag> : '-',
          },
          {
            title: 'Total Records',
            render: (_: unknown, r: TagInfo) => formatNumber(r.snapshot?.totalRecordCount),
          },
          {
            title: 'Snapshot Time',
            render: (_: unknown, r: TagInfo) => formatTimestamp(r.snapshot?.timeMillis),
          },
          {
            title: 'Tag Created',
            dataIndex: 'tagCreateTime',
            render: formatTimestamp,
          },
          {
            title: 'Actions',
            width: 100,
            render: (_: unknown, record: TagInfo) => (
              <Popconfirm
                title={`Delete tag "${record.tagName}"?`}
                description="This action cannot be undone."
                onConfirm={() => dropMutation.mutate(record.tagName)}
                okText="Delete"
                okButtonProps={{ danger: true }}
              >
                <Button size="small" danger>Delete</Button>
              </Popconfirm>
            ),
          },
        ]}
      />

      <Modal
        title={`Create Tag on branch: ${branchKey}`}
        open={createOpen}
        onOk={handleCreate}
        onCancel={() => { setCreateOpen(false); form.resetFields(); }}
        confirmLoading={createMutation.isPending}
      >
        <Form form={form} layout="vertical">
          <Form.Item
            name="tagName"
            label="Tag Name"
            rules={[{ required: true, message: 'Please enter a tag name' }]}
          >
            <Input placeholder="v1.0" />
          </Form.Item>
          <Form.Item name="snapshotId" label="Snapshot ID (optional)">
            <InputNumber style={{ width: '100%' }} placeholder="e.g. 1" />
          </Form.Item>
          <Form.Item name="timeRetained" label="Time Retained (optional)">
            <Input placeholder="e.g. 7d, 1h" />
          </Form.Item>
        </Form>
      </Modal>
    </div>
  );
}
