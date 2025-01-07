import { Button, Flex, Layout, Menu, Skeleton, Space } from 'antd'
import { MinusOutlined, PlusOutlined } from '@ant-design/icons'
import { Outlet, useLocation, useNavigate } from '@tanstack/react-router'
import { OmitKeyof, UseQueryOptions, useSuspenseQuery } from '@tanstack/react-query'
import { Configuration } from './configuration'
import { init } from '@paralleldrive/cuid2'

const { Sider: AntSider } = Layout

type ConfigurationsParams = {
  url: string
  query: OmitKeyof<UseQueryOptions<Map<string, Configuration>, Error, Map<string, Configuration>, string[]>, "queryFn">
}

const createId = init({
  length: 8,
});

const Configurations: React.FC<ConfigurationsParams> = ({ url, query }) => {
  const navigate = useNavigate({})
  const location = useLocation()
  const { data } = useSuspenseQuery(query)

  let items: any[] = []
  if (data) {
    items = Array.from(data.keys()).map((id: string) => (
      {
        key: `/${id}`,
        label: id,
        onClick: () => navigate({
          to: `/${url}/${id}`,
        })
      }
    ))
  }

  const newConfiguration = () => {
    navigate({
      to: `/${url}/${createId()}`,
    })
  }

  let menu = <Skeleton />
  if (items && items.length != 0) {
    menu = <AntSider width={200}>
      <Menu
        mode="inline"
        defaultSelectedKeys={[location.pathname]}
        items={items}
      />
    </AntSider>
  }

  return <>
    <Flex gap="middle" justify='space-between'>
      <Space direction="vertical">
        {menu}
        <Space>
          <Button type="primary" shape="circle" icon={<PlusOutlined />} onClick={() => newConfiguration()} />
          <Button type="primary" shape="circle" icon={<MinusOutlined />} />
        </Space>
      </Space>
      <div style={{ width: '100%' }}>
        <Outlet />
      </div>
    </Flex>
  </>
}

export default Configurations