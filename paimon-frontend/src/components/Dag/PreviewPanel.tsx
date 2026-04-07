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
import { Tabs, Empty, Table, Typography, Tag } from 'antd';
import type { DagStore } from './hooks/useDagStore';

const { Text } = Typography;

interface PreviewPanelProps {
  store: DagStore;
}

export default function PreviewPanel({ store }: PreviewPanelProps) {
  const previewNodes = useMemo(
    () => store.nodes.filter((n) => n.data.nodeType === 'preview'),
    [store.nodes],
  );

  if (previewNodes.length === 0) {
    return (
      <div
        style={{
          padding: 24,
          height: '100%',
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'center',
        }}
      >
        <Empty description="No preview nodes in this DAG" />
      </div>
    );
  }

  const items = previewNodes.map((pn) => {
    const buffer = store.previews[pn.id];
    const runtime = pn.data.runtime;
    return {
      key: pn.id,
      label: (
        <span>
          {pn.data.name}
          {runtime.state === 'running' && ' · ⏳'}
          {runtime.state === 'success' && (
            <Text type="secondary"> · {runtime.row_count ?? 0} rows</Text>
          )}
          {runtime.state === 'error' && <Tag color="error" style={{ marginLeft: 6 }}>error</Tag>}
        </span>
      ),
      children: buffer && buffer.columns.length > 0 ? (
        <Table
          columns={buffer.columns.map((col, idx) => ({
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
              if (value === null || value === undefined)
                return <Text type="secondary" italic>NULL</Text>;
              return String(value);
            },
          }))}
          dataSource={buffer.rows.map((row, idx) => {
            const rec: Record<string, unknown> = { key: idx };
            row.forEach((v, c) => (rec[c] = v));
            return rec;
          })}
          pagination={false}
          size="small"
          scroll={{ x: 'max-content', y: 240 }}
          bordered
        />
      ) : (
        <Empty description="No rows yet" image={Empty.PRESENTED_IMAGE_SIMPLE} />
      ),
    };
  });

  return (
    <div style={{ height: '100%', padding: 12 }}>
      <Tabs items={items} size="small" />
    </div>
  );
}
