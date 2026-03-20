import { Table, Tag, Typography } from 'antd';
import type { SchemaInfo } from '../../api/types';

const { Text } = Typography;

interface Props {
  schema: SchemaInfo;
}

export default function SchemaView({ schema }: Props) {
  const columns = [
    { title: 'ID', dataIndex: 'id', width: 60 },
    {
      title: 'Name',
      dataIndex: 'name',
      render: (name: string) => {
        const isPk = schema.primaryKeys.includes(name);
        const isPart = schema.partitionKeys.includes(name);
        return (
          <span>
            <Text strong={isPk}>{name}</Text>
            {isPk && (
              <Tag color="blue" style={{ marginLeft: 4 }}>
                PK
              </Tag>
            )}
            {isPart && (
              <Tag color="green" style={{ marginLeft: 4 }}>
                Partition
              </Tag>
            )}
          </span>
        );
      },
    },
    { title: 'Type', dataIndex: 'type' },
    { title: 'Description', dataIndex: 'description', render: (v: string) => v || '-' },
  ];

  return (
    <Table
      dataSource={schema.fields}
      columns={columns}
      rowKey="id"
      pagination={false}
      size="small"
    />
  );
}
