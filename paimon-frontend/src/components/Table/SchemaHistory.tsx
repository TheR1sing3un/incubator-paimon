import { Table, Collapse, Tag, Spin } from 'antd';
import { useQuery } from '@tanstack/react-query';
import { listSchemas } from '../../api/schemas';
import { formatTimestamp } from '../../utils/format';
import type { SchemaHistoryEntry, FieldInfo } from '../../api/types';

interface Props {
  database: string;
  table: string;
  branch?: string;
}

export default function SchemaHistory({ database, table, branch }: Props) {
  const { data = [], isLoading } = useQuery({
    queryKey: ['schemas', database, table, branch ?? 'main'],
    queryFn: () => listSchemas(database, table, branch),
  });

  const fieldColumns = [
    { title: 'ID', dataIndex: 'id', width: 60 },
    { title: 'Name', dataIndex: 'name' },
    { title: 'Type', dataIndex: 'type' },
  ];

  const items = data.map((entry: SchemaHistoryEntry) => ({
    key: String(entry.schemaId),
    label: (
      <span>
        <Tag color="blue">Schema {entry.schemaId}</Tag>
        {entry.timeMillis ? ` - ${formatTimestamp(entry.timeMillis)}` : ''}
        {entry.schema.comment ? ` - ${entry.schema.comment}` : ''}
      </span>
    ),
    children: (
      <div>
        <div style={{ marginBottom: 8 }}>
          <strong>Primary Keys:</strong> {entry.schema.primaryKeys?.join(', ') || '-'}
          {' | '}
          <strong>Partition Keys:</strong> {entry.schema.partitionKeys?.join(', ') || '-'}
        </div>
        <Table<FieldInfo>
          dataSource={entry.schema.fields}
          columns={fieldColumns}
          rowKey="id"
          pagination={false}
          size="small"
        />
      </div>
    ),
  }));

  if (isLoading) return <Spin />;

  return (
    <Collapse
      items={items}
      defaultActiveKey={data.length > 0 ? [String(data[0].schemaId)] : []}
    />
  );
}
