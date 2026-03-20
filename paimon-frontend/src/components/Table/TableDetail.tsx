import { Tabs, Typography, Spin, Alert } from 'antd';
import { useParams } from 'react-router-dom';
import { useQuery } from '@tanstack/react-query';
import { getTable } from '../../api/tables';
import SchemaView from './SchemaView';
import OptionsView from './OptionsView';
import SnapshotList from './SnapshotList';
import BranchList from './BranchList';
import TagList from './TagList';
import PartitionList from './PartitionList';
import SchemaHistory from './SchemaHistory';
import ConsumerList from './ConsumerList';

const { Title } = Typography;

export default function TableDetail() {
  const { db, table } = useParams<{ db: string; table: string }>();

  const { data: tableInfo, isLoading, error } = useQuery({
    queryKey: ['table', db, table],
    queryFn: () => getTable(db!, table!),
    enabled: !!db && !!table,
  });

  if (isLoading) return <Spin size="large" />;
  if (error) return <Alert type="error" message="Failed to load table" description={String(error)} />;
  if (!tableInfo) return null;

  const tabItems = [
    {
      key: 'schema',
      label: 'Schema',
      children: <SchemaView schema={tableInfo.schema} />,
    },
    {
      key: 'options',
      label: 'Options',
      children: <OptionsView options={tableInfo.schema.options} />,
    },
    {
      key: 'snapshots',
      label: 'Snapshots',
      children: <SnapshotList database={db!} table={table!} />,
    },
    {
      key: 'branches',
      label: 'Branches',
      children: <BranchList database={db!} table={table!} />,
    },
    {
      key: 'tags',
      label: 'Tags',
      children: <TagList database={db!} table={table!} />,
    },
    {
      key: 'partitions',
      label: 'Partitions',
      children: <PartitionList database={db!} table={table!} />,
    },
    {
      key: 'schema-history',
      label: 'Schema History',
      children: <SchemaHistory database={db!} table={table!} />,
    },
    {
      key: 'consumers',
      label: 'Consumers',
      children: <ConsumerList database={db!} table={table!} />,
    },
  ];

  return (
    <div>
      <Title level={3}>
        {db}.{table}
      </Title>
      <Tabs defaultActiveKey="schema" items={tabItems} />
    </div>
  );
}
