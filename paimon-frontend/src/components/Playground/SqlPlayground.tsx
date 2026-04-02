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

import { useState, useCallback, useEffect, useRef } from 'react';
import { Button, Space, Alert, message } from 'antd';
import { PlayCircleOutlined, ClearOutlined, StopOutlined } from '@ant-design/icons';
import {
  executeQuery,
  buildCatalogOptions,
  loadQueryServiceUrl,
  type QueryResult,
} from '../../api/query';
import { useCatalog } from '../../store/catalogStore';
import SqlEditor from './SqlEditor';
import ResultsTable from './ResultsTable';

interface SqlPlaygroundProps {
  database: string;
  table: string;
}

export default function SqlPlayground({ database, table }: SqlPlaygroundProps) {
  const { active } = useCatalog();
  const defaultSql = `SELECT * FROM ${table} LIMIT 10`;
  const [sqlValue, setSqlValue] = useState(defaultSql);
  const [result, setResult] = useState<QueryResult | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const abortRef = useRef<AbortController | null>(null);

  useEffect(() => {
    setSqlValue(`SELECT * FROM ${table} LIMIT 10`);
    setResult(null);
    setError(null);
  }, [database, table]);

  const handleExecute = useCallback(async () => {
    const queryUrl = loadQueryServiceUrl();
    if (!queryUrl) {
      message.warning('Please configure Query Service URL in settings (top-right gear icon)');
      return;
    }
    const trimmed = sqlValue.trim();
    if (!trimmed) {
      message.warning('Please enter a SQL query');
      return;
    }

    abortRef.current?.abort();
    const controller = new AbortController();
    abortRef.current = controller;

    setLoading(true);
    setError(null);

    try {
      const res = await executeQuery(
        queryUrl,
        { sql: trimmed, database, catalog_options: buildCatalogOptions(active) },
        controller.signal,
      );
      setResult(res);
    } catch (err: unknown) {
      if (controller.signal.aborted) return;
      const errData = (err as { response?: { data?: { error?: string } } })?.response?.data;
      setError(errData?.error || (err instanceof Error ? err.message : 'Unknown error'));
      setResult(null);
    } finally {
      if (!controller.signal.aborted) {
        setLoading(false);
      }
    }
  }, [sqlValue, database, active]);

  const handleCancel = useCallback(() => {
    abortRef.current?.abort();
    abortRef.current = null;
    setLoading(false);
  }, []);

  const handleClear = useCallback(() => {
    setSqlValue(defaultSql);
    setResult(null);
    setError(null);
  }, [defaultSql]);

  if (!loadQueryServiceUrl()) {
    return (
      <Alert
        type="info"
        showIcon
        message="Query Service not configured"
        description='Please click the gear icon in the top-right corner to set the Query Service URL.'
      />
    );
  }

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
      <SqlEditor
        value={sqlValue}
        onChange={setSqlValue}
        onExecute={handleExecute}
        height="160px"
      />
      <Space>
        <Button
          type="primary"
          icon={<PlayCircleOutlined />}
          onClick={handleExecute}
          loading={loading}
        >
          Run (⌘↵)
        </Button>
        {loading && (
          <Button danger icon={<StopOutlined />} onClick={handleCancel}>
            Cancel
          </Button>
        )}
        <Button icon={<ClearOutlined />} onClick={handleClear}>
          Reset
        </Button>
      </Space>
      <ResultsTable result={result} error={error} loading={loading} />
    </div>
  );
}
