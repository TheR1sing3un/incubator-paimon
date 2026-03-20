import { Table } from 'antd';
import { useQuery } from '@tanstack/react-query';
import { listTags } from '../../api/tags';
import { formatTimestamp, formatNumber, formatBytes } from '../../utils/format';

interface Props {
  database: string;
  table: string;
}

export default function TagList({ database, table }: Props) {
  const { data = [], isLoading } = useQuery({
    queryKey: ['tags', database, table],
    queryFn: () => listTags(database, table),
  });

  return (
    <Table
      dataSource={data}
      rowKey="tagName"
      loading={isLoading}
      pagination={false}
      size="small"
      columns={[
        { title: 'Tag Name', dataIndex: 'tagName' },
        { title: 'Snapshot ID', dataIndex: 'snapshotId' },
        { title: 'Schema ID', dataIndex: 'schemaId' },
        { title: 'Records', dataIndex: 'recordCount', render: formatNumber },
        { title: 'File Size', dataIndex: 'fileSizeInBytes', render: formatBytes },
        { title: 'Files', dataIndex: 'fileCount', render: formatNumber },
        { title: 'Created', dataIndex: 'createTime', render: formatTimestamp },
      ]}
    />
  );
}
