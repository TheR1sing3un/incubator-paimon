import { Table } from 'antd';
import { useQuery } from '@tanstack/react-query';
import { listConsumers } from '../../api/consumers';

interface Props {
  database: string;
  table: string;
}

export default function ConsumerList({ database, table }: Props) {
  const { data = [], isLoading } = useQuery({
    queryKey: ['consumers', database, table],
    queryFn: () => listConsumers(database, table),
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
