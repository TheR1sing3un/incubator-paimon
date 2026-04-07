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

import { DatabaseOutlined } from '@ant-design/icons';
import type { NodeProps } from '@xyflow/react';
import NodeShell from './NodeShell';
import type { DagFlowNode } from '../hooks/useDagStore';

export default function PaimonReadNode({ data }: NodeProps<DagFlowNode>) {
  const database = (data.config.database as string) || '—';
  const table = (data.config.table as string) || '—';
  return (
    <NodeShell
      icon={<DatabaseOutlined />}
      title={data.name}
      subtitle="Paimon Read"
      runtime={data.runtime}
      hasInput={false}
      hasOutput
      accent="#722ed1"
    >
      <div style={{ color: '#666' }}>
        {database}.{table}
      </div>
    </NodeShell>
  );
}
