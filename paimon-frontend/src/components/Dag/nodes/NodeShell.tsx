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

import type { ReactNode } from 'react';
import { Handle, Position } from '@xyflow/react';
import { Tooltip, Typography } from 'antd';
import type { NodeRuntime } from '../types';

const { Text } = Typography;

interface NodeShellProps {
  icon: ReactNode;
  title: string;
  subtitle?: string;
  runtime: NodeRuntime;
  hasInput: boolean;
  hasOutput: boolean;
  accent: string;
  children?: ReactNode;
}

function borderColor(state: NodeRuntime['state'], accent: string): string {
  switch (state) {
    case 'running':
      return '#1677ff';
    case 'success':
      return '#52c41a';
    case 'error':
      return '#ff4d4f';
    default:
      return accent;
  }
}

export default function NodeShell({
  icon,
  title,
  subtitle,
  runtime,
  hasInput,
  hasOutput,
  accent,
  children,
}: NodeShellProps) {
  const color = borderColor(runtime.state, accent);
  return (
    <div
      style={{
        minWidth: 180,
        maxWidth: 260,
        background: '#fff',
        borderRadius: 8,
        border: `2px solid ${color}`,
        boxShadow: runtime.state === 'running'
          ? '0 0 0 4px rgba(22, 119, 255, 0.15)'
          : '0 2px 8px rgba(0,0,0,0.08)',
        transition: 'border-color 0.2s, box-shadow 0.2s',
      }}
    >
      {hasInput && (
        <Handle
          type="target"
          position={Position.Left}
          style={{ background: accent, width: 10, height: 10 }}
        />
      )}
      <div
        style={{
          padding: '8px 12px',
          borderBottom: '1px solid #f0f0f0',
          display: 'flex',
          alignItems: 'center',
          gap: 8,
          background: `${accent}10`,
          borderRadius: '6px 6px 0 0',
        }}
      >
        <span style={{ color: accent, fontSize: 16 }}>{icon}</span>
        <div style={{ flex: 1, minWidth: 0 }}>
          <div style={{ fontWeight: 600, fontSize: 13 }}>{title}</div>
          {subtitle && (
            <Text
              type="secondary"
              style={{ fontSize: 11, display: 'block' }}
              ellipsis
            >
              {subtitle}
            </Text>
          )}
        </div>
      </div>
      <div style={{ padding: '8px 12px', fontSize: 12 }}>{children}</div>
      {(runtime.elapsed_ms !== undefined || runtime.error) && (
        <div
          style={{
            padding: '6px 12px',
            borderTop: '1px solid #f0f0f0',
            fontSize: 11,
            color: runtime.state === 'error' ? '#ff4d4f' : '#999',
          }}
        >
          {runtime.state === 'error' ? (
            <Tooltip title={runtime.error}>
              <span>✗ {runtime.error?.slice(0, 40)}…</span>
            </Tooltip>
          ) : (
            <>
              ✓ {runtime.elapsed_ms}ms
              {runtime.row_count !== undefined && ` · ${runtime.row_count} rows`}
              {runtime.truncated ? ' · truncated' : ''}
            </>
          )}
        </div>
      )}
      {hasOutput && (
        <Handle
          type="source"
          position={Position.Right}
          style={{ background: accent, width: 10, height: 10 }}
        />
      )}
    </div>
  );
}
