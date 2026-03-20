import { Table, Button, Space, Tag } from 'antd';
import { LeftOutlined, RightOutlined } from '@ant-design/icons';
import { usePagedData } from '../../hooks/usePagedData';
import { listSnapshots } from '../../api/snapshots';
import { formatTimestamp, formatNumber } from '../../utils/format';
import type { SnapshotInfo } from '../../api/types';

interface Props {
  database: string;
  table: string;
  branch?: string;
}

export default function SnapshotList({ database, table, branch }: Props) {
  const { data, isLoading, hasNext, hasPrev, pageIndex, goNext, goPrev } = usePagedData<SnapshotInfo>({
    queryKey: ['snapshots', database, table, branch ?? 'main'],
    fetcher: (pageToken) =>
      listSnapshots(database, table, pageToken, branch).then((r) => ({
        data: r.snapshots,
        nextPageToken: r.nextPageToken,
      })),
  });

  const columns = [
    { title: 'ID', dataIndex: 'id', width: 80 },
    {
      title: 'Commit Kind',
      dataIndex: 'commitKind',
      render: (v: string) => <Tag>{v}</Tag>,
    },
    { title: 'Schema ID', dataIndex: 'schemaId', width: 100 },
    {
      title: 'Total Records',
      dataIndex: 'totalRecordCount',
      render: formatNumber,
    },
    {
      title: 'Delta Records',
      dataIndex: 'deltaRecordCount',
      render: formatNumber,
    },
    {
      title: 'Watermark',
      dataIndex: 'watermark',
      render: formatTimestamp,
    },
    {
      title: 'Time',
      dataIndex: 'timeMillis',
      render: formatTimestamp,
    },
  ];

  return (
    <div>
      <Table
        dataSource={data}
        columns={columns}
        rowKey="id"
        loading={isLoading}
        pagination={false}
        size="small"
      />
      <Space style={{ marginTop: 12 }}>
        <Button icon={<LeftOutlined />} disabled={!hasPrev} onClick={goPrev}>
          Prev
        </Button>
        <span>Page {pageIndex + 1}</span>
        <Button icon={<RightOutlined />} disabled={!hasNext} onClick={goNext}>
          Next
        </Button>
      </Space>
    </div>
  );
}
