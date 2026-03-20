import { Table } from 'antd';
import { useQuery } from '@tanstack/react-query';
import { listConsumers } from '../../api/consumers';

interface Props {
  database: string;
  table: string;
  branch?: string;
}

export default function ConsumerList({ database, table, branch }: Props) {
  const { data = [], isLoading } = useQuery({
    queryKey: ['consumers', database, table, branch ?? 'main'],
    queryFn: () => listConsumers(database, table, branch),
  });

  return (
    <Table
      dataSource={data}
      rowKey="consumerId"
      loading={isLoading}
      pagination={false}
      size="small"
      columns={[
        { title: 'Consumer ID', dataIndex: 'consumerId' },
        { title: 'Next Snapshot ID', dataIndex: 'nextSnapshotId' },
      ]}
    />
  );
}
