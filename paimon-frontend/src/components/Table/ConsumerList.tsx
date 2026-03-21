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
import { listConsumers } from '../../api/consumers';

interface Props {
  database: string;
  table: string;
  branch?: string;
}

export default function ConsumerList({ database, table, branch }: Props) {
  const { data = [], isLoading } = useQuery({
    queryKey: ['consumers', database, table, branch ?? 'main'],
    queryFn: () => listConsumers(database, table, branch),
  });

  return (
    <Table
      dataSource={data}
      rowKey="consumerId"
      loading={isLoading}
      pagination={false}
      size="small"
      columns={[
        { title: 'Consumer ID', dataIndex: 'consumerId' },
        { title: 'Next Snapshot ID', dataIndex: 'nextSnapshotId' },
      ]}
    />
  );
}
