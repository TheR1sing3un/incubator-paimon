import { Table, Tag } from 'antd';
import { useQuery } from '@tanstack/react-query';
import { listTags } from '../../api/tags';
import { formatTimestamp, formatNumber } from '../../utils/format';
import type { TagInfo } from '../../api/types';

interface Props {
  database: string;
  table: string;
  branch?: string;
}

export default function TagList({ database, table, branch }: Props) {
  const { data = [], isLoading } = useQuery({
    queryKey: ['tags', database, table, branch ?? 'main'],
    queryFn: () => listTags(database, table, branch),
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
        {
          title: 'Snapshot ID',
          render: (_: unknown, r: TagInfo) => r.snapshot?.id ?? '-',
        },
        {
          title: 'Schema ID',
          render: (_: unknown, r: TagInfo) => r.snapshot?.schemaId ?? '-',
        },
        {
          title: 'Commit Kind',
          render: (_: unknown, r: TagInfo) =>
            r.snapshot?.commitKind ? <Tag>{r.snapshot.commitKind}</Tag> : '-',
        },
        {
          title: 'Total Records',
          render: (_: unknown, r: TagInfo) => formatNumber(r.snapshot?.totalRecordCount),
        },
        {
          title: 'Snapshot Time',
          render: (_: unknown, r: TagInfo) => formatTimestamp(r.snapshot?.timeMillis),
        },
        {
          title: 'Tag Created',
          dataIndex: 'tagCreateTime',
          render: formatTimestamp,
        },
      ]}
    />
  );
}
