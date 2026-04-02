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

import { useMemo } from 'react';
import { Table, Alert, Space, Typography, Tag } from 'antd';
import type { QueryResult } from '../../api/query';

const { Text } = Typography;

interface ResultsTableProps {
  result: QueryResult | null;
  error: string | null;
  loading: boolean;
}

export default function ResultsTable({ result, error, loading }: ResultsTableProps) {
  const columns = useMemo(() => {
    if (!result) return [];
    return result.columns.map((col, idx) => ({
      title: (
        <div>
          <div>{col.name}</div>
          <Text type="secondary" style={{ fontSize: 11, fontWeight: 'normal' }}>
            {col.type}
          </Text>
        </div>
      ),
      dataIndex: idx,
      key: col.name,
      ellipsis: true,
      render: (value: unknown) => {
        if (value === null) return <Text type="secondary" italic>NULL</Text>;
        return String(value);
      },
    }));
  }, [result]);

  const dataSource = useMemo(() => {
    if (!result) return [];
    return result.rows.map((row, idx) => {
      const record: Record<string, unknown> = { key: idx };
      row.forEach((val, colIdx) => {
        record[colIdx] = val;
      });
      return record;
    });
  }, [result]);

  if (error) {
    return <Alert type="error" message="Query Error" description={error} showIcon />;
  }

  if (!result) {
    return (
      <div style={{ padding: 40, textAlign: 'center', color: '#999' }}>
        Run a query to see results
      </div>
    );
  }

  return (
    <div style={{ display: 'flex', flexDirection: 'column', height: '100%' }}>
      <div style={{ flex: 1, overflow: 'auto' }}>
        <Table
          columns={columns}
          dataSource={dataSource}
          loading={loading}
          pagination={false}
          size="small"
          scroll={{ x: 'max-content', y: 'calc(100vh - 420px)' }}
          bordered
        />
      </div>
      <Space style={{ padding: '8px 0', borderTop: '1px solid #f0f0f0' }}>
        <Text type="secondary">
          {result.row_count} row{result.row_count !== 1 ? 's' : ''}
        </Text>
        {result.truncated && (
          <Tag color="warning">Results truncated</Tag>
        )}
        <Text type="secondary">{result.elapsed_ms}ms</Text>
      </Space>
    </div>
  );
}
