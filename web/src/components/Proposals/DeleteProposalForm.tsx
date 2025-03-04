import React from 'react'
import { Form, Input } from 'antd'
import Types from '../Types/Types'

type FieldType = {
  type?: string
  key?: string
  data?: string
}

const DeleteProposalForm: React.FC = () => {
  return <Form name="basic" initialValues={{ type: '' }}>
    <Form.Item<FieldType>
      label="Type"
      name="type"
      rules={[{ required: true, message: 'Type Name' }]}
    >
      <Types />
    </Form.Item>
    <Form.Item<FieldType>
      label="Key"
      name="key"
      rules={[{ required: true, message: 'Key' }]}
    >
      <Input />
    </Form.Item>
  </Form>
}

export default DeleteProposalForm