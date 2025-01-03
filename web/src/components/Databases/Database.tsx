import { useState } from "react"
import { useParams } from '@tanstack/react-router'
import { useMutation, useSuspenseQuery } from '@tanstack/react-query'
import { getDatabasesQuery, saveDatabaseFunction } from './queries'
import { Button, Flex, Select, Space } from 'antd'
import ConfigurationEditor from './ConfigurationEditor'

const Database: React.FC = () => {
  const { databaseId } = useParams({ strict: false })
  const { data: databases } = useSuspenseQuery(getDatabasesQuery)

  const setDatabase = useMutation({ mutationFn: saveDatabaseFunction })

  let originalData = ''
  if (databases && databaseId) {
    const database = databases.get(databaseId)
    if (database && database.data) {
      originalData = database.data
    }
  }

  const [data, setData] = useState(originalData)

  const save = () => {
    const database = { id: databaseId ?? '', mimeType: 'text/toml', data: data }
    setDatabase.mutate(database)
  }

  return <>
    <Flex gap="middle" vertical>
      <Space>
        <Select options={[{ value: 'toml', label: 'text/toml' }]} defaultValue="toml" style={{ width: 120 }} />
        <Button type="primary" onClick={() => save()}>Save</Button>
      </Space>
      <ConfigurationEditor value={data} setValue={setData} />
    </Flex>
  </>
}

export default Database