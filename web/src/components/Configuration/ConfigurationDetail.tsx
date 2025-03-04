import { useEffect, useState } from "react"
import { useParams } from '@tanstack/react-router'
import { MutationFunction, OmitKeyof, useMutation, UseQueryOptions, useSuspenseQuery } from '@tanstack/react-query'
import { Button, Flex, Select, Space, Tabs } from 'antd'
import type { TabsProps } from 'antd';
import { Configuration, ConfigurationData } from './configuration'
import ConfigurationEditor from './ConfigurationEditor'

type ConfigurationParams = {
  paramName: string
  query: OmitKeyof<UseQueryOptions<Map<string, Configuration>, Error, Map<string, Configuration>, string[]>, "queryFn">
  mutationFn: MutationFunction<any, any>
  form?: React.FC<ConfigurationData>
}

const ConfigurationDetail: React.FC<ConfigurationParams> = ({ paramName, query, mutationFn, form = null }) => {
  const param: any = useParams({ strict: false })
  const id = param[paramName]
  const { data: configurations } = useSuspenseQuery(query)
  const setConfiguration = useMutation({ mutationFn })

  let originalData = ''
  if (configurations && id) {
    const configuration = configurations.get(id)
    if (configuration && configuration.data) {
      originalData = configuration.data
    }
  }

  const [data, setData] = useState(originalData)

  useEffect(() => {
    setData(originalData)
  }, [originalData])

  const save = () => {
    const configuration = { id: id ?? '', mimeType: 'text/toml', data: data }
    setConfiguration.mutate(configuration)
  }

  const editor = <ConfigurationEditor value={data} setValue={setData} />

  let tabs = editor
  if (form) {
    const items: TabsProps['items'] = [
      {
        key: 'form',
        label: 'Form',
        children: <>{form({ data, setData })}</>,
      },
      {
        key: 'editor',
        label: 'Editor',
        children: editor,
      },
    ]

    tabs = <Tabs defaultActiveKey="form" items={items} />
  }

  return <>
    <Flex gap="middle" vertical>
      <Space>
        <Select options={[{ value: 'toml', label: 'text/toml' }]} defaultValue="toml" style={{ width: 120 }} />
        <Button type="primary" onClick={() => save()}>Save</Button>
      </Space>
      {tabs}
    </Flex>
  </>
}

export default ConfigurationDetail