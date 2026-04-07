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
  DatabaseOutlined,
  CodeOutlined,
  SaveOutlined,
  EyeOutlined,
  DownloadOutlined,
} from '@ant-design/icons';
import { Typography } from 'antd';
import type { ReactNode, DragEvent } from 'react';
import type { DagNodeType } from './types';

const { Title, Text } = Typography;

interface PaletteItem {
  type: DagNodeType;
  label: string;
  icon: ReactNode;
  accent: string;
  description: string;
}

const ITEMS: PaletteItem[] = [
  {
    type: 'paimon_read',
    label: 'Paimon Read',
    icon: <DatabaseOutlined />,
    accent: '#722ed1',
    description: 'Read a Paimon table',
  },
  {
    type: 'sql',
    label: 'SQL',
    icon: <CodeOutlined />,
    accent: '#1677ff',
    description: 'Transform with SQL',
  },
  {
    type: 'paimon_write',
    label: 'Paimon Write',
    icon: <SaveOutlined />,
    accent: '#fa541c',
    description: 'Write to a Paimon table',
  },
  {
    type: 'preview',
    label: 'Preview',
    icon: <EyeOutlined />,
    accent: '#13c2c2',
    description: 'Display rows in the results panel',
  },
  {
    type: 'file_export',
    label: 'File Export',
    icon: <DownloadOutlined />,
    accent: '#eb2f96',
    description: 'Download CSV / Parquet',
  },
];

export default function NodePalette() {
  const handleDragStart = (event: DragEvent<HTMLDivElement>, type: DagNodeType) => {
    event.dataTransfer.setData('application/paimon-dag-node', type);
    event.dataTransfer.effectAllowed = 'move';
  };

  return (
    <div style={{ padding: 16, height: '100%', overflow: 'auto' }}>
      <Title level={5} style={{ marginTop: 0 }}>
        Nodes
      </Title>
      <Text type="secondary" style={{ fontSize: 12 }}>
        Drag onto the canvas
      </Text>
      <div style={{ marginTop: 16, display: 'flex', flexDirection: 'column', gap: 8 }}>
        {ITEMS.map((item) => (
          <div
            key={item.type}
            draggable
            onDragStart={(e) => handleDragStart(e, item.type)}
            style={{
              padding: '10px 12px',
              borderRadius: 6,
              border: `1px solid ${item.accent}`,
              background: `${item.accent}10`,
              cursor: 'grab',
              display: 'flex',
              alignItems: 'center',
              gap: 10,
              userSelect: 'none',
            }}
          >
            <span style={{ color: item.accent, fontSize: 18 }}>{item.icon}</span>
            <div style={{ flex: 1, minWidth: 0 }}>
              <div style={{ fontWeight: 600, fontSize: 13 }}>{item.label}</div>
              <Text type="secondary" style={{ fontSize: 11 }}>
                {item.description}
              </Text>
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}
