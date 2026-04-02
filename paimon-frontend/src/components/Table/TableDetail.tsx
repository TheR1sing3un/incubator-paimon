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
import { Tabs, Typography, Spin, Alert, Select, Space, Button, Popconfirm, Modal, Form, Input, message } from 'antd';
import { BranchesOutlined, DeleteOutlined, EditOutlined } from '@ant-design/icons';
import { useParams, useNavigate } from 'react-router-dom';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { getTable, dropTable, renameTable } from '../../api/tables';
import { listBranches } from '../../api/branches';
import SchemaView from './SchemaView';
import OptionsView from './OptionsView';
import SnapshotList from './SnapshotList';
import BranchList from './BranchList';
import BranchGraph from './BranchGraph';
import TagList from './TagList';
import PartitionList from './PartitionList';
import SchemaHistory from './SchemaHistory';
import ConsumerList from './ConsumerList';
import SqlPlayground from '../Playground/SqlPlayground';

const { Title } = Typography;

export default function TableDetail() {
  const { db, table } = useParams<{ db: string; table: string }>();
  const navigate = useNavigate();
  const queryClient = useQueryClient();
  const [currentBranch, setCurrentBranch] = useState('main');
  const [renameOpen, setRenameOpen] = useState(false);
  const [renameForm] = Form.useForm();

  const { data: tableInfo, isLoading, error } = useQuery({
    queryKey: ['table', db, table, currentBranch],
    queryFn: () => getTable(db!, table!, currentBranch),
    enabled: !!db && !!table,
  });

  const { data: branches = [] } = useQuery({
    queryKey: ['branches', db, table],
    queryFn: () => listBranches(db!, table!),
    enabled: !!db && !!table,
  });

  const dropMutation = useMutation({
    mutationFn: () => dropTable(db!, table!),
    onSuccess: () => {
      message.success('Table deleted');
      queryClient.invalidateQueries({ queryKey: ['tables', db] });
      queryClient.invalidateQueries({ queryKey: ['databases'] });
      navigate(`/databases/${db}`);
    },
    onError: (err: Error) => message.error(err.message || 'Failed to delete table'),
  });

  const renameMutation = useMutation({
    mutationFn: renameTable,
    onSuccess: (_data, variables) => {
      message.success('Table renamed');
      queryClient.invalidateQueries({ queryKey: ['tables'] });
      setRenameOpen(false);
      navigate(`/databases/${variables.destination.database}/tables/${variables.destination.object}`);
    },
    onError: (err: Error) => message.error(err.message || 'Failed to rename table'),
  });

  if (isLoading) return <Spin size="large" />;
  if (error) return <Alert type="error" message="Failed to load table" description={String(error)} />;
  if (!tableInfo) return null;

  const branchOptions = branches.map((b) => ({
    value: b.branch,
    label: b.branch,
  }));

  const tabItems = [
    {
      key: 'schema',
      label: 'Schema',
      children: (
        <>
          <SchemaView database={db!} table={table!} schema={tableInfo.schema} />
          <div style={{ marginTop: 24 }}>
            <SqlPlayground database={db!} table={table!} />
          </div>
        </>
      ),
    },
    {
      key: 'options',
      label: 'Options',
      children: <OptionsView options={tableInfo.schema.options} />,
    },
    {
      key: 'snapshots',
      label: 'Snapshots',
      children: <SnapshotList key={currentBranch} database={db!} table={table!} branch={currentBranch} />,
    },
    {
      key: 'branches',
      label: 'Branches',
      children: <BranchList database={db!} table={table!} />,
    },
    {
      key: 'graph',
      label: 'Graph',
      children: <BranchGraph database={db!} table={table!} />,
    },
    {
      key: 'tags',
      label: 'Tags',
      children: <TagList key={currentBranch} database={db!} table={table!} branch={currentBranch} />,
    },
    {
      key: 'partitions',
      label: 'Partitions',
      children: <PartitionList key={currentBranch} database={db!} table={table!} branch={currentBranch} />,
    },
    {
      key: 'schema-history',
      label: 'Schema History',
      children: <SchemaHistory key={currentBranch} database={db!} table={table!} branch={currentBranch} />,
    },
    {
      key: 'consumers',
      label: 'Consumers',
      children: <ConsumerList key={currentBranch} database={db!} table={table!} branch={currentBranch} />,
    },
  ];

  return (
    <div>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 8 }}>
        <Space align="center">
          <Title level={3} style={{ margin: 0 }}>
            {db}.{table}
          </Title>
          {branchOptions.length > 0 && (
            <>
              <BranchesOutlined style={{ marginLeft: 16, color: '#999' }} />
              <Select
                value={currentBranch}
                onChange={(val: string) => setCurrentBranch(val)}
                options={branchOptions}
                style={{ minWidth: 140 }}
              />
            </>
          )}
        </Space>
        <Space>
          <Button
            icon={<EditOutlined />}
            onClick={() => {
              renameForm.setFieldsValue({ destDb: db, destTable: table });
              setRenameOpen(true);
            }}
          >
            Rename
          </Button>
          <Popconfirm
            title={`Delete table "${table}"?`}
            description="This action cannot be undone."
            onConfirm={() => dropMutation.mutate()}
            okText="Delete"
            okButtonProps={{ danger: true }}
          >
            <Button danger icon={<DeleteOutlined />}>Delete Table</Button>
          </Popconfirm>
        </Space>
      </div>
      <Tabs defaultActiveKey="schema" items={tabItems} destroyOnHidden />

      <Modal
        title={`Rename Table: ${table}`}
        open={renameOpen}
        onOk={() => {
          renameForm.validateFields().then((values) => {
            renameMutation.mutate({
              source: { database: db!, object: table! },
              destination: { database: values.destDb, object: values.destTable },
            });
          });
        }}
        onCancel={() => { setRenameOpen(false); renameForm.resetFields(); }}
        confirmLoading={renameMutation.isPending}
      >
        <Form form={renameForm} layout="vertical">
          <Form.Item name="destDb" label="Destination Database" rules={[{ required: true }]}>
            <Input />
          </Form.Item>
          <Form.Item name="destTable" label="New Table Name" rules={[{ required: true }]}>
            <Input />
          </Form.Item>
        </Form>
      </Modal>
    </div>
  );
}
