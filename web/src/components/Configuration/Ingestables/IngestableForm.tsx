import React from 'react'
import { Form, Input } from 'antd'
import { ConfigurationData } from '../configuration'
import { parse, stringify } from "@std/toml";
import { emptyIngestable, Ingestable } from './ingestable'

type FieldType = {
  name?: string
}

const TypeForm: React.FC<ConfigurationData> = ({ data, setData }) => {
  let ingestable = emptyIngestable()
  if (data) {
    ingestable = parse(data) as Ingestable
  }

  const nameChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    ingestable.ingestable.name = e.target?.value
    setData(stringify(ingestable))
  }

  return <Form name="basic" initialValues={{ name: ingestable?.ingestable?.name }}>
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