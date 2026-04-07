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

import { useCallback, useState } from 'react';
import { Layout, message, Modal } from 'antd';
import { ReactFlowProvider } from '@xyflow/react';

import { useCatalog } from '../../store/catalogStore';
import { buildCatalogOptions, loadQueryServiceUrl } from '../../api/query';
import NodePalette from './NodePalette';
import Canvas from './Canvas';
import Inspector from './Inspector';
import Toolbar from './Toolbar';
import PreviewPanel from './PreviewPanel';
import { useDagStore, serializeDag } from './hooks/useDagStore';
import { useDagExecution } from './hooks/useDagExecution';
import { validateDag } from './validation';
import type { RunnerMode } from './types';

const { Sider, Content } = Layout;

export default function DagPage() {
  const { active } = useCatalog();
  const store = useDagStore();
  const execution = useDagExecution(store);
  const [runner, setRunner] = useState<RunnerMode>('native');
  const [rayAddress, setRayAddress] = useState('');

  const handleRun = useCallback(() => {
    const queryUrl = loadQueryServiceUrl();
    if (!queryUrl) {
      message.warning(
        'Please configure Query Service URL in settings (top-right gear icon)',
      );
      return;
    }
    const { nodes: wireNodes, edges: wireEdges } = serializeDag(
      store.nodes,
      store.edges,
    );
    const errors = validateDag(wireNodes, wireEdges);
    if (errors.length > 0) {
      Modal.error({
        title: 'Cannot run DAG',
        content: (
          <ul style={{ margin: 0, paddingLeft: 20 }}>
            {errors.map((e, i) => (
              <li key={i}>{e}</li>
            ))}
          </ul>
        ),
      });
      return;
    }
    execution.run(
      {
        runner,
        rayAddress: rayAddress || undefined,
        catalogOptions: buildCatalogOptions(active),
        queryServiceUrl: queryUrl,
      },
      {
        nodes: wireNodes,
        edges: wireEdges,
      },
    );
  }, [active, execution, rayAddress, runner, store.edges, store.nodes]);

  return (
    <Layout style={{ height: 'calc(100vh - 64px)' }}>
      <Sider
        width={220}
        theme="light"
        style={{ borderRight: '1px solid #f0f0f0' }}
      >
        <NodePalette />
      </Sider>
      <Layout>
        <Toolbar
          running={execution.running}
          runner={runner}
          rayAddress={rayAddress}
          onRunnerChange={setRunner}
          onRayAddressChange={setRayAddress}
          onRun={handleRun}
          onCancel={execution.cancel}
          onClear={store.clearAll}
        />
        <Layout>
          <Content style={{ display: 'flex', flexDirection: 'column' }}>
            <div style={{ flex: '1 1 auto', minHeight: 0 }}>
              <ReactFlowProvider>
                <Canvas store={store} />
              </ReactFlowProvider>
            </div>
            <div
              style={{
                flex: '0 0 280px',
                borderTop: '1px solid #f0f0f0',
                background: '#fff',
                overflow: 'hidden',
              }}
            >
              <PreviewPanel store={store} />
            </div>
          </Content>
          <Sider
            width={320}
            theme="light"
            style={{ borderLeft: '1px solid #f0f0f0', overflow: 'auto' }}
          >
            <Inspector store={store} />
          </Sider>
        </Layout>
      </Layout>
    </Layout>
  );
}
