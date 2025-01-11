import React from 'react'
import { Form, Input } from 'antd'
import { ConfigurationData } from '../configuration'
import { parse, stringify } from "@std/toml";
import { emptySyncable, Syncable } from './syncable'

type FieldType = {
  name?: string
}

const TypeForm: React.FC<ConfigurationData> = ({ data, setData }) => {
  let syncable = emptySyncable()
  if (data) {
    syncable = parse(data) as Syncable
  }

  const nameChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    syncable.syncable.name = e.target?.value
    setData(stringify(syncable))
  }

  return <Form name="basic" initialValues={{ name: syncable?.syncable?.name }}>
    <Form.Item<FieldType>
      label="Name"
      name="name"
      rules={[{ required: true, message: 'Type Name' }]}
    >
      <Input onChange={nameChange} />
    </Form.Item>
  </Form>
}

export default TypeForm