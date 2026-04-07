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
  Space,
  Select,
  Input,
  Typography,
  Tooltip,
  Popconfirm,
} from 'antd';
import {
  PlayCircleOutlined,
  StopOutlined,
  ClearOutlined,
} from '@ant-design/icons';
import type { RunnerMode } from './types';

const { Text } = Typography;

interface ToolbarProps {
  running: boolean;
  runner: RunnerMode;
  rayAddress: string;
  onRunnerChange(v: RunnerMode): void;
  onRayAddressChange(v: string): void;
  onRun(): void;
  onCancel(): void;
  onClear(): void;
}

export default function Toolbar({
  running,
  runner,
  rayAddress,
  onRunnerChange,
  onRayAddressChange,
  onRun,
  onCancel,
  onClear,
}: ToolbarProps) {
  return (
    <Space
      style={{
        padding: '8px 16px',
        borderBottom: '1px solid #f0f0f0',
        background: '#fafafa',
        width: '100%',
        justifyContent: 'space-between',
      }}
    >
      <Space>
        {running ? (
          <Button
            danger
            icon={<StopOutlined />}
            onClick={onCancel}
          >
            Cancel
          </Button>
        ) : (
          <Button
            type="primary"
            icon={<PlayCircleOutlined />}
            onClick={onRun}
          >
            Run
          </Button>
        )}
        <Popconfirm
          title="Clear the entire canvas?"
          onConfirm={onClear}
          okText="Clear"
          okType="danger"
          disabled={running}
        >
          <Button icon={<ClearOutlined />} disabled={running}>
            Clear
          </Button>
        </Popconfirm>
      </Space>
      <Space>
        <Text type="secondary">Runner</Text>
        <Select
          value={runner}
          onChange={onRunnerChange}
          disabled={running}
          style={{ width: 120 }}
          options={[
            { value: 'native', label: 'Native' },
            { value: 'ray', label: 'Ray' },
          ]}
        />
        {runner === 'ray' && (
          <Tooltip title="Ray head node address, e.g. ray://127.0.0.1:10001. Leave empty for auto.">
            <Input
              value={rayAddress}
              onChange={(e) => onRayAddressChange(e.target.value)}
              placeholder="ray://… (optional)"
              style={{ width: 220 }}
              disabled={running}
            />
          </Tooltip>
        )}
      </Space>
    </Space>
  );
}
