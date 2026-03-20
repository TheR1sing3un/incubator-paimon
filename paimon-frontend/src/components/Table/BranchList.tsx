import { Table, Divider, Typography } from 'antd';
import { useQuery } from '@tanstack/react-query';
import { listBranches } from '../../api/branches';
import BranchGraph from './BranchGraph';

const { Title } = Typography;

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
    <div>
      <Title level={5}>Branch Graph</Title>
      <BranchGraph database={database} table={table} />

      <Divider />

      <Title level={5}>Branch Details</Title>
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
    </div>
  );
}
