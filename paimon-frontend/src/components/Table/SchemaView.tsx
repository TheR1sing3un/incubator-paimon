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
import { Table, Tag, Typography, Button, Modal, Form, Input, Select, Space, message } from 'antd';
import type { FormInstance } from 'antd';
import { PlusOutlined, MinusCircleOutlined } from '@ant-design/icons';
import { useMutation, useQueryClient } from '@tanstack/react-query';
import { alterTable } from '../../api/tables';
import type { SchemaInfo, SchemaChange } from '../../api/types';

const { Text } = Typography;

function SchemaChangeRow({ name, restField, form, onRemove }: {
  name: number;
  restField: { fieldKey?: number };
  form: FormInstance;
  onRemove: () => void;
}) {
  const changeType = Form.useWatch(['changes', name, 'action'], form);
  return (
    <Space style={{ display: 'flex', marginBottom: 8 }} align="start" wrap>
      <Form.Item
        {...restField}
        name={[name, 'action']}
        rules={[{ required: true, message: 'Select type' }]}
        noStyle
      >
        <Select placeholder="Change type" style={{ width: 170 }}>
          <Select.Option value="addColumn">Add Column</Select.Option>
          <Select.Option value="dropColumn">Drop Column</Select.Option>
          <Select.Option value="renameColumn">Rename Column</Select.Option>
          <Select.Option value="updateColumnType">Update Column Type</Select.Option>
        </Select>
      </Form.Item>
      {(changeType === 'addColumn' || changeType === 'dropColumn' ||
        changeType === 'renameColumn' || changeType === 'updateColumnType') && (
        <Form.Item {...restField} name={[name, 'fieldName']} noStyle>
          <Input placeholder="Field name" style={{ width: 130 }} />
        </Form.Item>
      )}
      {changeType === 'renameColumn' && (
        <Form.Item {...restField} name={[name, 'newFieldName']} noStyle>
          <Input placeholder="New name" style={{ width: 130 }} />
        </Form.Item>
      )}
      {(changeType === 'addColumn' || changeType === 'updateColumnType') && (
        <Form.Item {...restField} name={[name, 'fieldType']} noStyle>
          <Input placeholder="Field type" style={{ width: 130 }} />
        </Form.Item>
      )}
      <MinusCircleOutlined onClick={onRemove} />
    </Space>
  );
}

interface Props {
  database: string;
  table: string;
  schema: SchemaInfo;
}

export default function SchemaView({ database, table, schema }: Props) {
  const queryClient = useQueryClient();
  const [alterOpen, setAlterOpen] = useState(false);
  const [form] = Form.useForm();

  const alterMutation = useMutation({
    mutationFn: (changes: SchemaChange[]) => alterTable(database, table, { changes }),
    onSuccess: () => {
      message.success('Schema updated');
      queryClient.invalidateQueries({ queryKey: ['table', database, table] });
      setAlterOpen(false);
      form.resetFields();
    },
    onError: (err: Error) => message.error(err.message || 'Failed to alter schema'),
  });

  const handleAlter = () => {
    form.validateFields().then((values) => {
      const changes: SchemaChange[] = (values.changes || []).map(
        (c: { action: string; fieldName?: string; newFieldName?: string; fieldType?: string }) => {
          switch (c.action) {
            case 'addColumn':
              return { action: 'addColumn' as const, fieldNames: [c.fieldName!], dataType: c.fieldType! };
            case 'dropColumn':
              return { action: 'dropColumn' as const, fieldNames: [c.fieldName!] };
            case 'renameColumn':
              return { action: 'renameColumn' as const, fieldNames: [c.fieldName!], newName: c.newFieldName! };
            case 'updateColumnType':
              return { action: 'updateColumnType' as const, fieldNames: [c.fieldName!], newDataType: c.fieldType! };
            default:
              throw new Error(`Unknown action: ${c.action}`);
          }
        }
      );
      if (changes.length === 0) {
        message.warning('No changes specified');
        return;
      }
      alterMutation.mutate(changes);
    });
  };

  const columns = [
    { title: 'ID', dataIndex: 'id', width: 60 },
    {
      title: 'Name',
      dataIndex: 'name',
      render: (name: string) => {
        const isPk = schema.primaryKeys.includes(name);
        const isPart = schema.partitionKeys.includes(name);
        return (
          <span>
            <Text strong={isPk}>{name}</Text>
            {isPk && (
              <Tag color="blue" style={{ marginLeft: 4 }}>
                PK
              </Tag>
            )}
            {isPart && (
              <Tag color="green" style={{ marginLeft: 4 }}>
                Partition
              </Tag>
            )}
          </span>
        );
      },
    },
    { title: 'Type', dataIndex: 'type' },
    { title: 'Description', dataIndex: 'description', render: (v: string) => v || '-' },
  ];

  return (
    <div>
      <div style={{ marginBottom: 8, textAlign: 'right' }}>
        <Button icon={<PlusOutlined />} onClick={() => { setAlterOpen(true); form.resetFields(); }}>
          Alter Schema
        </Button>
      </div>
      <Table
        dataSource={schema.fields}
        columns={columns}
        rowKey="id"
        pagination={false}
        size="small"
      />

      <Modal
        title="Alter Table Schema"
        open={alterOpen}
        onOk={handleAlter}
        onCancel={() => { setAlterOpen(false); form.resetFields(); }}
        confirmLoading={alterMutation.isPending}
        width={680}
      >
        <Form form={form} layout="vertical">
          <Form.List name="changes">
            {(fields, { add, remove }) => (
              <>
                {fields.map(({ key, name, ...restField }) => (
                  <SchemaChangeRow
                    key={key}
                    name={name}
                    restField={restField}
                    form={form}
                    onRemove={() => remove(name)}
                  />
                ))}
                <Button type="dashed" onClick={() => add()} block icon={<PlusOutlined />}>
                  Add Change
                </Button>
              </>
            )}
          </Form.List>
        </Form>
      </Modal>
    </div>
  );
}
