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
import { Tabs, Typography, Spin, Alert, Select, Space } from 'antd';
import { BranchesOutlined } from '@ant-design/icons';
import { useParams } from 'react-router-dom';
import { useQuery } from '@tanstack/react-query';
import { getTable } from '../../api/tables';
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

const { Title } = Typography;

export default function TableDetail() {
  const { db, table } = useParams<{ db: string; table: string }>();
  const [currentBranch, setCurrentBranch] = useState('main');

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
      children: <SchemaView schema={tableInfo.schema} />,
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
      <Space align="center" style={{ marginBottom: 8 }}>
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
      <Tabs defaultActiveKey="schema" items={tabItems} destroyOnHidden />
    </div>
  );
}
