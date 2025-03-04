import React from 'react'
import { Form, Input, Select } from 'antd'
import { Editor } from '@monaco-editor/react'
import * as monaco from 'monaco-editor/esm/vs/editor/editor.api'
import Types from '../Types/Types'

type FieldType = {
  type?: string
  key?: string
  data?: string
}

const AddProposalForm: React.FC = () => {
  const options: monaco.editor.IStandaloneEditorConstructionOptions = {
    selectOnLineNumbers: false,
    colorDecorators: true,
  }

  const editor = <Editor
    defaultLanguage="json"
    height='45vh'
    // value={value}
    options={options}
  // onChange={onChange}
  // onMount={editorDidMount}
  />

  return <Form name="basic" initialValues={{ type: '', data: '{\n\n}' }}>
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
    <Form.Item<FieldType>
    >
      <Select options={[{ value: 'javascript', label: 'application/json' }]} defaultValue="javascript" style={{ width: 160 }} />
    </Form.Item>
    <Form.Item<FieldType>
      label="Data"
      name="data"
      rules={[{ required: true, message: 'Data' }]}
    >
      {editor}
    </Form.Item>
  </Form>
}

export default AddProposalForm