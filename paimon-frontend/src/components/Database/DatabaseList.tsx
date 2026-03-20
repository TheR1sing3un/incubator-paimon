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
