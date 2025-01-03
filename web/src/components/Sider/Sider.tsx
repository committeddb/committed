import React from 'react'
import { DatabaseOutlined, ExportOutlined, HomeOutlined, ImportOutlined } from '@ant-design/icons'
import { Layout, Menu } from 'antd'
import { useLocation, useNavigate } from '@tanstack/react-router'

const { Sider: AntSider } = Layout

const Sider: React.FC = () => {
  const navigate = useNavigate({})
  const location = useLocation()

  const pathname = '/' + location.pathname.split('/')[1]

  const item = (label: string, url: string, icon: any) => {
    return {
      key: url,
      icon: React.createElement(icon),
      label: label,
      onClick: () => navigate({
        to: url,
      })
    }
  }

  const items = [
    item('Home', '/', HomeOutlined),
    item('Databases', '/databases', DatabaseOutlined),
    item('Ingestables', '/ingestables', ImportOutlined),
    item('Syncables', '/syncables', ExportOutlined)
  ]

  return (
    <AntSider width={200}>
      <Menu
        mode="inline"
        defaultSelectedKeys={[pathname]}
        style={{ height: '100%', borderRight: 0 }}
        items={items}
      />
    </AntSider>
  )
}

export default Sider