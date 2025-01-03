import { Button, Flex, Layout, Menu, Space } from 'antd'
import { MinusOutlined, PlusOutlined } from '@ant-design/icons'
import { Outlet, useLocation, useNavigate } from '@tanstack/react-router'
import { useSuspenseQuery } from '@tanstack/react-query'
import { init } from '@paralleldrive/cuid2'
import { getDatabasesQuery } from './queries'

const { Sider: AntSider } = Layout

const createId = init({
  length: 8,
});

const Databases: React.FC = () => {
  const navigate = useNavigate({})
  const location = useLocation()
  const { data: databases } = useSuspenseQuery(getDatabasesQuery)

  let items: any[] = []
  if (databases) {
    items = Array.from(databases.keys()).map((id: string) => (
      {
        key: `/${id}`,
        label: id,
        onClick: () => navigate({
          to: `/databases/${id}`,
        })
      }
    ))
  }

  const newDatabase = () => {
    navigate({
      to: `/databases/${createId()}`,
    })
  }

  return <>
    <Flex gap="middle" justify='space-between'>
      <Space direction="vertical">
        <AntSider width={200}>
          <Menu
            mode="inline"
            defaultSelectedKeys={[location.pathname]}
            items={items}
          />
        </AntSider>
        <Space>
          <Button type="primary" shape="circle" icon={<PlusOutlined />} onClick={() => newDatabase()} />
          <Button type="primary" shape="circle" icon={<MinusOutlined />} />
        </Space>
      </Space>
      <div style={{ width: '100%' }}>
        <Outlet />
      </div>
    </Flex>
  </>
}

export default Databases