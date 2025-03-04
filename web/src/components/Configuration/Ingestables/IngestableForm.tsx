import React from 'react'
import { Button, Card, Divider, Empty, Form, Input, Select } from 'antd'
import { CloseOutlined } from '@ant-design/icons'
import { ConfigurationData } from '../configuration'
import { parse, stringify } from "@std/toml";
import { emptyIngestable, Ingestable } from './ingestable'

type FieldType = {
  name?: string
  type?: string
  dialect?: string
  connectionString?: string
  primaryKey?: string
  topic?: string
}

const IngestableForm: React.FC<ConfigurationData> = ({ data, setData }) => {
  let ingestable = emptyIngestable()
  if (data) {
    ingestable = parse(data) as Ingestable
  } else {
    setData(stringify(ingestable))
  }

  const nameChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    ingestable.ingestable.name = e.target?.value
    updateData()
  }

  const typeChange = (value: string) => {
    ingestable.ingestable.type = value
    updateData()
  }

  const dialectChange = (value: string) => {
    ingestable.sql.dialect = value
    updateData()
  }

  const connectionStringChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    ingestable.sql.connectionString = e.target?.value
    updateData()
  }

  const primaryKeyChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    ingestable.sql.primaryKey = e.target?.value
    updateData()
  }

  const topicChange = (value: string) => {
    ingestable.sql.topic = value
    updateData()
  }

  const jsonNameChange = (index: number) => (e: React.ChangeEvent<HTMLInputElement>) => {
    if (ingestable.sql.mappings) {
      ingestable.sql.mappings[index].jsonName = e.target?.value
      updateData()
    }
  }

  const columnChange = (index: number) => (e: React.ChangeEvent<HTMLInputElement>) => {
    if (ingestable.sql.mappings) {
      ingestable.sql.mappings[index].column = e.target?.value
      updateData()
    }
  }

  const updateData = () => {
    if (ingestable.sql.mappings && ingestable.sql.mappings.length == 0) {
      ingestable.sql.mappings = undefined
    }
    setData(stringify(ingestable))
  }

  const addMapping = () => {
    if (!ingestable.sql.mappings) {
      ingestable.sql.mappings = []
    }
    ingestable.sql.mappings.push({ jsonName: '', column: '' })
    updateData()
  }

  const removeMapping = (index: number) => {
    ingestable.sql.mappings?.splice(index, 1)
    updateData()
  }

  const topicOptions: any = []

  const values = {
    name: ingestable.ingestable.name,
    type: ingestable.ingestable.type,
    dialect: ingestable.sql.dialect,
    connectionString: ingestable.sql.connectionString,
    primaryKey: ingestable.sql.primaryKey,
    topic: ingestable.sql.topic
  }

  const empty = <Empty
    description="Please Create a Type first"
    image={Empty.PRESENTED_IMAGE_SIMPLE}
    style={{ margin: 8 }}
    imageStyle={{ height: 32 }}
  />

  return <Form name="basic" initialValues={values} style={{ overflow: 'auto' }}>
    <Form.Item<FieldType>
      label="Name"
      name="name"
      rules={[{ required: true, message: 'Type Name' }]}
    >
      <Input onChange={nameChange} />
    </Form.Item>
    <Form.Item<FieldType>
      label="Type"
      name="type"
      rules={[{ required: true, message: 'Database Type' }]}
    >
      <Select options={[{ value: 'sql', label: 'sql' }]} onChange={typeChange} />
    </Form.Item>
    <Divider />
    <Form.Item<FieldType>
      label="Dialect"
      name="dialect"
      rules={[{ required: true, message: 'SQL Dialect' }]}
    >
      <Select options={[{ value: 'mysql', label: 'mysql' }]} onChange={dialectChange} />
    </Form.Item>
    <Form.Item<FieldType>
      label="Connection String"
      name="connectionString"
      rules={[{ required: true, message: 'SQL Connection String' }]}
    >
      <Input.Password placeholder="connection string" onChange={connectionStringChange} />
    </Form.Item>
    <Form.Item<FieldType>
      label="Primary Key"
      name="primaryKey"
      rules={[{ required: true, message: 'Primary Key' }]}
    >
      <Input onChange={primaryKeyChange} />
    </Form.Item>
    <Form.Item<FieldType>
      label="Topic"
      name="topic"
      rules={[{ required: true, message: 'SQL Dialect' }]}
    >
      <Select options={topicOptions} onChange={topicChange} notFoundContent={empty} />
    </Form.Item>
    <Form.List name="mappings">
      {(fields, { add, remove }) => (
        <div style={{ display: 'flex', rowGap: 16, flexDirection: 'column' }}>
          {fields.map((field) => (
            <Card
              size="small"
              title={`SQL Mapping ${field.name + 1}`}
              key={field.key}
              extra={
                <CloseOutlined onClick={() => { remove(field.name); removeMapping(field.name) }} />
              }
            >
              <Form.Item
                label="JSON Path"
                name={[field.name, 'jsonName']}
                rules={[{ required: true, message: 'JSON Path from type' }]}
              >
                <Input onChange={jsonNameChange(field.name)} />
              </Form.Item>
              <Form.Item
                label="Column"
                name={[field.name, 'column']}
                rules={[{ required: true, message: 'Column Name from database' }]}
              >
                <Input onChange={columnChange(field.name)} />
              </Form.Item>
            </Card>
          ))}
          <Button type="dashed" onClick={() => { add(); addMapping() }} block>
            + Add SQL Mapping
          </Button>
        </div>
      )}
    </Form.List>
  </Form>
}

export default IngestableForm