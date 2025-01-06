import { useState } from "react"
import { useParams } from '@tanstack/react-router'
import { MutationFunction, OmitKeyof, useMutation, UseQueryOptions, useSuspenseQuery } from '@tanstack/react-query'
import { Button, Flex, Select, Space } from 'antd'
import { Configuration } from './configuration'
import { ConfigurationEditor } from '.'

type ConfigurationParams = {
  paramName: string
  query: OmitKeyof<UseQueryOptions<Map<string, Configuration>, Error, Map<string, Configuration>, string[]>, "queryFn">
  mutationFn: MutationFunction<any, any>
}

const ConfigurationDetail: React.FC<ConfigurationParams> = ({ paramName, query, mutationFn }) => {
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

  const save = () => {
    const configuration = { id: id ?? '', mimeType: 'text/toml', data: data }
    setConfiguration.mutate(configuration)
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

export default ConfigurationDetail