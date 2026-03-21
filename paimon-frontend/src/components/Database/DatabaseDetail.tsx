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

import { Table, Typography, Descriptions, Card, Spin, Alert } from 'antd';
import { TableOutlined } from '@ant-design/icons';
import { useParams, useNavigate } from 'react-router-dom';
import { useQuery } from '@tanstack/react-query';
import { getDatabase } from '../../api/databases';
import { listTables } from '../../api/tables';

const { Title } = Typography;

export default function DatabaseDetail() {
  const { db } = useParams<{ db: string }>();
  const navigate = useNavigate();

  const { data: dbInfo, isLoading: dbLoading, error: dbError } = useQuery({
    queryKey: ['database', db],
    queryFn: () => getDatabase(db!),
    enabled: !!db,
  });

  const { data: tables = [], isLoading: tablesLoading } = useQuery({
    queryKey: ['tables', db],
    queryFn: () => listTables(db!),
    enabled: !!db,
  });

  if (dbError) {
    return <Alert type="error" message="Failed to load database" description={String(dbError)} />;
  }

  return (
    <div>
      <Title level={3}>Database: {db}</Title>

      {dbLoading ? (
        <Spin />
      ) : dbInfo ? (
        <Card size="small" style={{ marginBottom: 16 }}>
          <Descriptions column={2} size="small">
            {dbInfo.owner && <Descriptions.Item label="Owner">{dbInfo.owner}</Descriptions.Item>}
            {dbInfo.comment && <Descriptions.Item label="Comment">{dbInfo.comment}</Descriptions.Item>}
            {dbInfo.properties &&
              Object.entries(dbInfo.properties).map(([k, v]) => (
                <Descriptions.Item key={k} label={k}>
                  {v}
                </Descriptions.Item>
              ))}
          </Descriptions>
        </Card>
      ) : null}

      <Title level={4}>Tables</Title>
      <Table
        loading={tablesLoading}
        dataSource={tables.map((t) => ({ name: t }))}
        rowKey="name"
        columns={[
          {
            title: 'Table Name',
            dataIndex: 'name',
            render: (name: string) => (
              <a onClick={() => navigate(`/databases/${db}/tables/${name}`)}>
                <TableOutlined style={{ marginRight: 8 }} />
                {name}
              </a>
            ),
          },
        ]}
        pagination={false}
      />
    </div>
  );
}
