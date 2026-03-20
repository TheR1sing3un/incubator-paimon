import { Table } from 'antd';

interface Props {
  options: Record<string, string>;
}

export default function OptionsView({ options }: Props) {
  const data = Object.entries(options).map(([key, value]) => ({ key, value }));

  return (
    <Table
      dataSource={data}
      rowKey="key"
      pagination={false}
      size="small"
      columns={[
        { title: 'Key', dataIndex: 'key', sorter: (a, b) => a.key.localeCompare(b.key) },
        { title: 'Value', dataIndex: 'value' },
      ]}
    />
  );
}
