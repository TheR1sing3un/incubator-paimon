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

import { Table, Tag } from 'antd';
import { useQuery } from '@tanstack/react-query';
import { listTags } from '../../api/tags';
import { formatTimestamp, formatNumber } from '../../utils/format';
import type { TagInfo } from '../../api/types';

interface Props {
  database: string;
  table: string;
  branch?: string;
}

export default function TagList({ database, table, branch }: Props) {
  const { data = [], isLoading } = useQuery({
    queryKey: ['tags', database, table, branch ?? 'main'],
    queryFn: () => listTags(database, table, branch),
  });

  return (
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
      ]}
    />
  );
}
