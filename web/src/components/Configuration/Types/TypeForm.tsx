import React from 'react'
import { Form, Input } from 'antd'
import { ConfigurationData } from '../configuration'
import { parse, stringify } from "@std/toml";
import { emptyType, Type } from './type'

type FieldType = {
  name?: string
}

const TypeForm: React.FC<ConfigurationData> = ({ data, setData }) => {
  let type = emptyType()
  if (data) {
    type = parse(data) as Type
  }

  const nameChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    type.type.name = e.target?.value
    setData(stringify(type))
  }

  return <Form name="basic" initialValues={{ name: type?.type?.name }}>
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