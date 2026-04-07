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

import {
  Button,
  Form,
  Input,
  Select,
  Space,
  Switch,
  Typography,
  Alert,
  Popconfirm,
} from 'antd';
import { DeleteOutlined } from '@ant-design/icons';
import SqlEditor from '../Playground/SqlEditor';
import type { DagStore } from './hooks/useDagStore';

const { Title, Text } = Typography;

interface InspectorProps {
  store: DagStore;
}

export default function Inspector({ store }: InspectorProps) {
  const { nodes, selectedId, updateNodeConfig, renameNode, deleteNode } = store;
  const node = nodes.find((n) => n.id === selectedId);

  if (!node) {
    return (
      <div style={{ padding: 16 }}>
        <Title level={5} style={{ marginTop: 0 }}>
          Inspector
        </Title>
        <Text type="secondary">Select a node to edit its configuration.</Text>
      </div>
    );
  }

  const { name, nodeType, config } = node.data;
  const cfg = config as Record<string, unknown>;

  const renderTypeFields = () => {
    switch (nodeType) {
      case 'paimon_read':
        return (
          <>
            <Form.Item label="Database">
              <Input
                value={(cfg.database as string) || ''}
                onChange={(e) => updateNodeConfig(node.id, { database: e.target.value })}
                placeholder="default"
              />
            </Form.Item>
            <Form.Item label="Table">
              <Input
                value={(cfg.table as string) || ''}
                onChange={(e) => updateNodeConfig(node.id, { table: e.target.value })}
                placeholder="my_table"
              />
            </Form.Item>
            <Form.Item
              label="Columns"
              help="Comma-separated, leave empty for all columns"
            >
              <Input
                value={((cfg.columns as string[] | null) || []).join(', ')}
                onChange={(e) => {
                  const cols = e.target.value
                    .split(',')
                    .map((s) => s.trim())
                    .filter(Boolean);
                  updateNodeConfig(node.id, {
                    columns: cols.length > 0 ? cols : null,
                  });
                }}
                placeholder="col1, col2"
              />
            </Form.Item>
            <Form.Item label="Snapshot ID" help="Optional — read a specific snapshot">
              <Input
                type="number"
                value={(cfg.snapshot_id as number | null) ?? ''}
                onChange={(e) => {
                  const v = e.target.value;
                  updateNodeConfig(node.id, {
                    snapshot_id: v === '' ? null : Number(v),
                  });
                }}
              />
            </Form.Item>
          </>
        );
      case 'sql':
        return (
          <Form.Item
            label="SQL"
            help="Reference upstream nodes by their name (FROM node_name)"
          >
            <SqlEditor
              value={(cfg.sql as string) || ''}
              onChange={(v) => updateNodeConfig(node.id, { sql: v })}
              onExecute={() => { /* disabled in DAG context */ }}
              height="240px"
            />
          </Form.Item>
        );
      case 'paimon_write':
        return (
          <>
            <Form.Item label="Database">
              <Input
                value={(cfg.database as string) || ''}
                onChange={(e) => updateNodeConfig(node.id, { database: e.target.value })}
              />
            </Form.Item>
            <Form.Item label="Table">
              <Input
                value={(cfg.table as string) || ''}
                onChange={(e) => updateNodeConfig(node.id, { table: e.target.value })}
              />
            </Form.Item>
            <Form.Item label="Mode">
              <Select
                value={(cfg.mode as string) || 'append'}
                onChange={(v) => updateNodeConfig(node.id, { mode: v })}
                options={[
                  { value: 'append', label: 'append' },
                  { value: 'overwrite', label: 'overwrite' },
                ]}
              />
            </Form.Item>
            <Form.Item
              label="Create table if not exists"
              help="Automatically create the target table from the upstream schema"
            >
              <Switch
                checked={!!cfg.create_if_not_exists}
                onChange={(v) =>
                  updateNodeConfig(node.id, { create_if_not_exists: v })
                }
              />
            </Form.Item>
            <Alert
              type="warning"
              showIcon
              message="This node writes data to Paimon."
              style={{ marginTop: 8 }}
            />
          </>
        );
      case 'preview':
        return (
          <Text type="secondary">
            Results will appear in the bottom panel when the DAG runs.
          </Text>
        );
      case 'file_export':
        return (
          <>
            <Form.Item label="Format">
              <Select
                value={(cfg.format as string) || 'csv'}
                onChange={(v) =>
                  updateNodeConfig(node.id, {
                    format: v,
                    filename:
                      (cfg.filename as string)?.replace(/\.(csv|parquet)$/i, '') +
                      '.' + v,
                  })
                }
                options={[
                  { value: 'csv', label: 'CSV' },
                  { value: 'parquet', label: 'Parquet' },
                ]}
              />
            </Form.Item>
            <Form.Item label="Filename">
              <Input
                value={(cfg.filename as string) || ''}
                onChange={(e) => updateNodeConfig(node.id, { filename: e.target.value })}
              />
            </Form.Item>
          </>
        );
      default:
        return null;
    }
  };

  return (
    <div style={{ padding: 16, overflow: 'auto', height: '100%' }}>
      <Space style={{ justifyContent: 'space-between', width: '100%', marginBottom: 12 }}>
        <Title level={5} style={{ margin: 0 }}>
          Inspector
        </Title>
        <Popconfirm
          title="Delete this node?"
          onConfirm={() => deleteNode(node.id)}
          okText="Delete"
          okType="danger"
        >
          <Button danger size="small" icon={<DeleteOutlined />}>
            Delete
          </Button>
        </Popconfirm>
      </Space>
      <Form layout="vertical" size="small">
        <Form.Item
          label="Name"
          help="Used by SQL nodes to reference this output (e.g. FROM <name>)"
        >
          <Input
            value={name}
            onChange={(e) => renameNode(node.id, e.target.value)}
          />
        </Form.Item>
        <Form.Item label="Type">
          <Input value={nodeType} disabled />
        </Form.Item>
        {renderTypeFields()}
      </Form>
    </div>
  );
}
