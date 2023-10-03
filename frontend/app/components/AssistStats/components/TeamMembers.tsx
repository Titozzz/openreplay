import { DownOutlined, TableOutlined } from '@ant-design/icons';
import { Button, Dropdown, Space, Typography } from 'antd';
import { Member } from 'App/services/AssistStatsService';
import { getInitials } from 'App/utils';
import React from 'react';
import { Loader } from 'UI';

const items = [
  {
    label: 'Sessions Assisted',
    key: 'sessionsAssisted',
  },
  {
    label: 'Live Duration',
    key: 'liveDuration',
  },
  {
    label: 'Call Duration',
    key: 'callDuration',
  },
  {
    label: 'Remote Duration',
    key: 'remoteDuration',
  },
];

function TeamMembers({
  isLoading,
  topMembers,
  onMembersSort,
}: {
  isLoading: boolean;
  topMembers: { list: Member[]; total: number };
  onMembersSort: (v: string) => void;
}) {
  const [dateRange, setDateRange] = React.useState(items[0].label);
  const updateRange = ({ key }: { key: string }) => {
    const item = items.find((item) => item.key === key);
    setDateRange(item?.label || items[0].label);
    onMembersSort(item?.key || items[0].key);
  };

  return (
    <div className={'rounded bg-white border p-2 h-full w-full'}>
      <div className={'flex items-center'}>
        <Typography.Title style={{ marginBottom: 0 }} level={4}>
          Team Members
        </Typography.Title>
        <div className={'ml-auto flex items-center gap-2'}>
          <Dropdown menu={{ items, onClick: updateRange }}>
            <Button size={'small'}>
              <Space>
                <Typography.Text>{dateRange}</Typography.Text>
                <DownOutlined rev={undefined} />
              </Space>
            </Button>
          </Dropdown>
          <Button shape={'default'} size={'small'} icon={<TableOutlined rev={undefined} />} />
        </div>
      </div>
      {/*<div style={{ minHeight: 299 }}>*/}
      <Loader loading={isLoading} style={{ minHeight: 150, height: 300 }} size={48}>
        {topMembers.list.map((member) => (
          <div key={member.name} className={'w-full flex items-center gap-2 border-b pt-2 pb-1'}>
            <div className="relative flex items-center justify-center w-10 h-10">
              <div className="absolute left-0 right-0 top-0 bottom-0 mx-auto w-10 h-10 rounded-full opacity-30 bg-tealx" />
              <div className="text-lg uppercase color-tealx">{getInitials(member.name)}</div>
            </div>
            <div>{member.name}</div>
            <div className={'ml-auto'}>{member.count}</div>
          </div>
        ))}
      </Loader>
      {/*</div>*/}
      <div className={'flex items-center justify-center text-disabled-text pt-1'}>
        {isLoading || topMembers.list.length === 0
          ? ''
          : `Showing 1 to ${topMembers.total} of the total`}
      </div>
    </div>
  );
}

export default TeamMembers;
