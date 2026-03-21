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

import { Card, List, Typography, Empty } from 'antd';
import { DatabaseOutlined } from '@ant-design/icons';
import { useQuery } from '@tanstack/react-query';
import { useNavigate } from 'react-router-dom';
import { listDatabases } from '../../api/databases';
import { useCatalog } from '../../store/catalogStore';

const { Title } = Typography;

export default function DatabaseList() {
  const navigate = useNavigate();
  const { active } = useCatalog();

  const { data: databases = [], isLoading } = useQuery({
    queryKey: ['databases', active?.name],
    queryFn: listDatabases,
    enabled: !!active,
  });

  if (!active) {
    return (
      <div style={{ textAlign: 'center', paddingTop: 80 }}>
        <Empty description="Please add a Catalog connection using the header button to get started." />
      </div>
    );
  }

  return (
    <div>
      <Title level={3}>Databases</Title>
      <List
        grid={{ gutter: 16, xs: 1, sm: 2, md: 3, lg: 4, xl: 4 }}
        loading={isLoading}
        dataSource={databases}
        renderItem={(db) => (
          <List.Item>
            <Card
              hoverable
              onClick={() => navigate(`/databases/${db}`)}
              style={{ textAlign: 'center' }}
            >
              <DatabaseOutlined style={{ fontSize: 32, color: '#1677ff' }} />
              <div style={{ marginTop: 8, fontWeight: 500 }}>{db}</div>
            </Card>
          </List.Item>
        )}
      />
    </div>
  );
}
