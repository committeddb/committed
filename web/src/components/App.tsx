import React from 'react'
import { Outlet } from '@tanstack/react-router'
import { Layout, theme } from 'antd'
import { Sider } from './Sider'

const { Header, Content } = Layout

const App: React.FC = () => {
  const {
    token: { colorBgContainer, borderRadiusLG },
  } = theme.useToken()

  return (
    <Layout>
      <Header style={{ display: 'flex', alignItems: 'center', height: '80px' }}></Header>
      <Layout style={{ height: "calc(100vh - 80px)" }}>
        <Sider />
        <Layout style={{ padding: '24px 24px' }}>
          <Content
            style={{
              padding: 24,
              margin: 0,
              minHeight: 280,
              background: colorBgContainer,
              borderRadius: borderRadiusLG,
            }}
          >
            <Outlet />
          </Content>
        </Layout>
      </Layout>
    </Layout>
  )
}

export default App
