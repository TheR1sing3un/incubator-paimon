import { Table } from 'antd';
import { useQuery } from '@tanstack/react-query';
import { listPartitions } from '../../api/partitions';
import { formatNumber, formatBytes, formatTimestamp } from '../../utils/format';

interface Props {
  database: string;
  table: string;
  branch?: string;
}

export default function PartitionList({ database, table, branch }: Props) {
  const { data = [], isLoading } = useQuery({
    queryKey: ['partitions', database, table, branch ?? 'main'],
    queryFn: () => listPartitions(database, table, branch),
  });

  return (
    <Table
      dataSource={data}
      rowKey={(r) => JSON.stringify(r.spec)}
      loading={isLoading}
      pagination={{ pageSize: 20 }}
      size="small"
      columns={[
        {
          title: 'Partition Spec',
          dataIndex: 'spec',
          render: (spec: Record<string, string>) =>
            Object.entries(spec)
              .map(([k, v]) => `${k}=${v}`)
              .join(', '),
        },
        { title: 'Records', dataIndex: 'recordCount', render: formatNumber },
        { title: 'File Size', dataIndex: 'fileSizeInBytes', render: formatBytes },
        { title: 'Files', dataIndex: 'fileCount', render: formatNumber },
        { title: 'Last File Created', dataIndex: 'lastFileCreationTime', render: formatTimestamp },
      ]}
    />
  );
}
