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

import { Table } from 'antd';
import { useQuery } from '@tanstack/react-query';
import { listBranches } from '../../api/branches';

interface Props {
  database: string;
  table: string;
}

export default function BranchList({ database, table }: Props) {
  const { data = [], isLoading } = useQuery({
    queryKey: ['branches', database, table],
    queryFn: () => listBranches(database, table),
  });

  return (
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
      ]}
    />
  );
}
