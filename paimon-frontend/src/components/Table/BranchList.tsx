import { Table } from 'antd';
import { useQuery } from '@tanstack/react-query';
import { listBranches } from '../../api/branches';

interface Props {
  database: string;
  table: string;
}

export default function BranchList({ database, table }: Props) {
  const { data = [], isLoading } = useQuery({
    queryKey: ['branches', database, table],
    queryFn: () => listBranches(database, table),
  });

  return (
    <Table
      dataSource={data}
      rowKey="branch"
      loading={isLoading}
      pagination={false}
      size="small"
      columns={[
        { title: 'Branch Name', dataIndex: 'branch' },
        { title: 'Latest Snapshot ID', dataIndex: 'latestSnapshotId', render: (v) => v ?? '-' },
        { title: 'Latest Schema ID', dataIndex: 'latestSchemaId', render: (v) => v ?? '-' },
      ]}
    />
  );
}
