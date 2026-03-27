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
import { Tag, Popover, Table, Typography } from 'antd';
import { RightOutlined } from '@ant-design/icons';
import type { DataTypeNode, DataFieldNode } from '../../api/types';

const { Text } = Typography;

type TagColor = string;

function getTypeKeyword(node: DataTypeNode): string {
  if (typeof node === 'string') return node;
  return node.type.replace(/ NOT NULL$/, '');
}

function isNotNull(node: DataTypeNode): boolean {
  if (typeof node === 'string') return node.endsWith(' NOT NULL');
  return node.type.endsWith(' NOT NULL');
}

function getTagColor(keyword: string): TagColor {
  const upper = keyword.toUpperCase();
  if (['INT', 'INTEGER', 'BIGINT', 'SMALLINT', 'TINYINT', 'FLOAT', 'DOUBLE', 'DECIMAL'].includes(upper)) return 'blue';
  if (['STRING', 'VARCHAR', 'CHAR'].includes(upper)) return 'green';
  if (['DATE', 'TIME', 'TIMESTAMP', 'TIMESTAMP_LTZ', 'TIMESTAMP WITHOUT TIME ZONE', 'TIMESTAMP WITH LOCAL TIME ZONE', 'TIME WITHOUT TIME ZONE'].includes(upper)) return 'orange';
  if (['BINARY', 'VARBINARY', 'BLOB'].includes(upper)) return 'purple';
  if (upper === 'BOOLEAN') return 'cyan';
  if (upper === 'ARRAY') return 'geekblue';
  if (upper === 'MAP') return 'volcano';
  if (upper === 'ROW') return 'magenta';
  if (upper === 'MULTISET') return 'gold';
  if (upper === 'VECTOR') return 'lime';
  return 'default';
}

function getTypeSummary(node: DataTypeNode, maxDepth: number = 2): string {
  if (typeof node === 'string') return node;
  const keyword = getTypeKeyword(node);
  const notNull = isNotNull(node) ? ' NOT NULL' : '';

  if (maxDepth <= 0) return keyword + '<...>' + notNull;

  if (keyword === 'ARRAY' || keyword === 'MULTISET') {
    const inner = node.element ? getTypeSummary(node.element, maxDepth - 1) : '?';
    return `${keyword}<${inner}>${notNull}`;
  }
  if (keyword === 'MAP') {
    const k = node.key ? getTypeSummary(node.key, maxDepth - 1) : '?';
    const v = node.value ? getTypeSummary(node.value, maxDepth - 1) : '?';
    return `${keyword}<${k}, ${v}>${notNull}`;
  }
  if (keyword === 'ROW') {
    const count = node.fields?.length ?? 0;
    return `${keyword}<${count} fields>${notNull}`;
  }
  if (keyword === 'VECTOR') {
    const inner = node.element ? getTypeSummary(node.element, maxDepth - 1) : '?';
    return `${keyword}<${inner}, ${node.dimension ?? '?'}>${notNull}`;
  }
  return keyword + notNull;
}

function isComplexType(node: DataTypeNode): boolean {
  return typeof node !== 'string';
}

function NotNullBadge() {
  return <Tag color="red" style={{ marginLeft: 4, fontSize: 10, lineHeight: '16px', padding: '0 4px' }}>NOT NULL</Tag>;
}

function TypeTree({ node, depth = 0 }: { node: DataTypeNode; depth?: number }) {
  if (typeof node === 'string') {
    const keyword = node.replace(/ NOT NULL$/, '');
    return (
      <span>
        <Tag color={getTagColor(keyword)} style={{ fontFamily: 'monospace' }}>{keyword}</Tag>
        {isNotNull(node) && <NotNullBadge />}
      </span>
    );
  }

  const keyword = getTypeKeyword(node);
  const notNull = isNotNull(node);

  if (keyword === 'ARRAY' || keyword === 'MULTISET') {
    return (
      <div style={{ paddingLeft: depth > 0 ? 16 : 0 }}>
        <Tag color={getTagColor(keyword)}>{keyword}</Tag>
        {notNull && <NotNullBadge />}
        <div style={{ paddingLeft: 16, marginTop: 4 }}>
          <Text type="secondary" style={{ fontSize: 12 }}>Element: </Text>
          <TypeTree node={node.element ?? '?'} depth={depth + 1} />
        </div>
      </div>
    );
  }

  if (keyword === 'MAP') {
    return (
      <div style={{ paddingLeft: depth > 0 ? 16 : 0 }}>
        <Tag color={getTagColor(keyword)}>{keyword}</Tag>
        {notNull && <NotNullBadge />}
        <div style={{ paddingLeft: 16, marginTop: 4 }}>
          <Text type="secondary" style={{ fontSize: 12 }}>Key: </Text>
          <TypeTree node={node.key ?? '?'} depth={depth + 1} />
        </div>
        <div style={{ paddingLeft: 16, marginTop: 4 }}>
          <Text type="secondary" style={{ fontSize: 12 }}>Value: </Text>
          <TypeTree node={node.value ?? '?'} depth={depth + 1} />
        </div>
      </div>
    );
  }

  if (keyword === 'ROW') {
    const fields = node.fields ?? [];
    const columns = [
      { title: 'Name', dataIndex: 'name', width: 120, render: (name: string) => <Text code>{name}</Text> },
      {
        title: 'Type',
        dataIndex: 'type',
        render: (type: DataTypeNode) => isComplexType(type)
          ? <TypeTree node={type} depth={depth + 1} />
          : <TypeTree node={type} depth={0} />,
      },
    ];
    return (
      <div style={{ paddingLeft: depth > 0 ? 16 : 0 }}>
        <Tag color={getTagColor(keyword)}>{keyword}</Tag>
        {notNull && <NotNullBadge />}
        <Table<DataFieldNode>
          dataSource={fields}
          columns={columns}
          rowKey="id"
          pagination={false}
          size="small"
          bordered
          style={{ marginTop: 4, maxWidth: 450 }}
        />
      </div>
    );
  }

  if (keyword === 'VECTOR') {
    return (
      <div style={{ paddingLeft: depth > 0 ? 16 : 0 }}>
        <Tag color={getTagColor(keyword)}>{keyword}</Tag>
        {notNull && <NotNullBadge />}
        <div style={{ paddingLeft: 16, marginTop: 4 }}>
          <Text type="secondary" style={{ fontSize: 12 }}>Element: </Text>
          <TypeTree node={node.element ?? '?'} depth={depth + 1} />
          <Text type="secondary" style={{ fontSize: 12, marginLeft: 8 }}>Dimension: {node.dimension ?? '?'}</Text>
        </div>
      </div>
    );
  }

  return (
    <span>
      <Tag color={getTagColor(keyword)} style={{ fontFamily: 'monospace' }}>{keyword}</Tag>
      {notNull && <NotNullBadge />}
    </span>
  );
}

export default function TypeDisplay({ type }: { type: DataTypeNode }) {
  const summary = useMemo(() => getTypeSummary(type), [type]);
  const keyword = getTypeKeyword(type);

  if (!isComplexType(type)) {
    const simpleKeyword = (type as string).replace(/ NOT NULL$/, '');
    return (
      <span>
        <Tag color={getTagColor(simpleKeyword)} style={{ fontFamily: 'monospace' }}>{simpleKeyword}</Tag>
        {isNotNull(type) && <NotNullBadge />}
      </span>
    );
  }

  return (
    <Popover
      content={<div style={{ maxWidth: 500, maxHeight: 400, overflow: 'auto' }}><TypeTree node={type} /></div>}
      title="Type Details"
      trigger="click"
      placement="bottomLeft"
    >
      <Tag
        color={getTagColor(keyword)}
        style={{ cursor: 'pointer', fontFamily: 'monospace' }}
      >
        {summary} <RightOutlined style={{ fontSize: 10 }} />
      </Tag>
    </Popover>
  );
}
