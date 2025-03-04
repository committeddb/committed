import React from 'react'
import { Button, Card, Divider, Empty, Form, Input, Select } from 'antd'
import { CloseOutlined } from '@ant-design/icons'
import { ConfigurationData } from '../configuration'
import { parse, stringify } from "@std/toml";
import { emptySyncable, Syncable } from './syncable'

type FieldType = {
  name?: string
  type?: string
  db?: string
  topic?: string
  primaryKey?: string
  table?: string
}

const SycableForm: React.FC<ConfigurationData> = ({ data, setData }) => {
  let syncable = emptySyncable()
  if (data) {
    syncable = parse(data) as Syncable
  } else {
    setData(stringify(syncable))
  }

  const nameChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    syncable.syncable.name = e.target?.value
    setData(stringify(syncable))
  }

  const typeChange = (value: string) => {
    syncable.syncable.type = value
    updateData()
  }

  const databaseChange = (value: string) => {
    syncable.sql.db = value
    updateData()
  }

  const topicChange = (value: string) => {
    syncable.sql.topic = value
    updateData()
  }

  const primaryKeyChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    syncable.sql.primaryKey = e.target?.value
    updateData()
  }

  const tableChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    syncable.sql.table = e.target?.value
    updateData()
  }

  const jsonPathChange = (index: number) => (e: React.ChangeEvent<HTMLInputElement>) => {
    if (syncable.sql.mappings) {
      syncable.sql.mappings[index].jsonPath = e.target?.value
      updateData()
    }
  }

  const columnChange = (index: number) => (e: React.ChangeEvent<HTMLInputElement>) => {
    if (syncable.sql.mappings) {
      syncable.sql.mappings[index].column = e.target?.value
      updateData()
    }
  }

  const columnTypeChange = (index: number) => (e: React.ChangeEvent<HTMLInputElement>) => {
    if (syncable.sql.mappings) {
      syncable.sql.mappings[index].type = e.target?.value
      updateData()
    }
  }

  const indexNameChange = (index: number) => (e: React.ChangeEvent<HTMLInputElement>) => {
    if (syncable.sql.indexes) {
      syncable.sql.indexes[index].name = e.target?.value
      updateData()
    }
  }

  const indexChange = (index: number) => (value: string) => {
    if (syncable.sql.indexes) {
      syncable.sql.indexes[index].index = value
      updateData()
    }
  }

  const updateData = () => {
    if (syncable.sql.mappings && syncable.sql.mappings.length == 0) {
      syncable.sql.mappings = undefined
    }
    if (syncable.sql.indexes && syncable.sql.indexes.length == 0) {
      syncable.sql.indexes = undefined
    }
    setData(stringify(syncable))
  }

  const addMapping = () => {
    if (!syncable.sql.mappings) {
      syncable.sql.mappings = []
    }
    syncable.sql.mappings.push({ jsonPath: '', column: '', type: '' })
    updateData()
  }

  const removeMapping = (index: number) => {
    syncable.sql.mappings?.splice(index, 1)
    updateData()
  }

  const addIndex = () => {
    if (!syncable.sql.indexes) {
      syncable.sql.indexes = []
    }
    syncable.sql.indexes.push({ name: '', index: '' })
    updateData()
  }

  const removeIndex = (index: number) => {
    syncable.sql.indexes?.splice(index, 1)
    updateData()
  }

  const databaseOptions: any = []

  const topicOptions: any = []

  const indexOptions: any = []

  const empty = (type: string) => <Empty
    description={`Please Create a ${type} first`}
    image={Empty.PRESENTED_IMAGE_SIMPLE}
    style={{ margin: 8 }}
    imageStyle={{ height: 32 }}
  />

  const values = {
    name: syncable.syncable.name,
    type: syncable.syncable.type,
    db: syncable.sql.db,
    topic: syncable.sql.topic,
    primaryKey: syncable.sql.primaryKey,
    table: syncable.sql.table
  }

  return <Form name="basic" initialValues={values}>
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
      label="Database"
      name="db"
      rules={[{ required: true, message: 'Database' }]}
    >
      <Select options={databaseOptions} onChange={databaseChange} notFoundContent={empty('Database')} />
    </Form.Item>
    <Form.Item<FieldType>
      label="Topic"
      name="topic"
      rules={[{ required: true, message: 'SQL Dialect' }]}
    >
      <Select options={topicOptions} onChange={topicChange} notFoundContent={empty('Topic')} />
    </Form.Item>
    <Form.Item<FieldType>
      label="Primary Key"
      name="primaryKey"
      rules={[{ required: true, message: 'Primary Key' }]}
    >
      <Input onChange={primaryKeyChange} />
    </Form.Item>
    <Form.Item<FieldType>
      label="SQL Table"
      name="table"
      rules={[{ required: true, message: 'SQL Table' }]}
    >
      <Input.Password placeholder="connection string" onChange={tableChange} />
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
                name={[field.name, 'jsonPath']}
                rules={[{ required: true, message: 'JSON Path from type' }]}
              >
                <Input onChange={jsonPathChange(field.name)} />
              </Form.Item>
              <Form.Item
                label="Column"
                name={[field.name, 'column']}
                rules={[{ required: true, message: 'Column Name from database' }]}
              >
                <Input onChange={columnChange(field.name)} />
              </Form.Item>
              <Form.Item
                label="Type"
                name={[field.name, 'type']}
                rules={[{ required: true, message: 'Type Info for column' }]}
              >
                <Input onChange={columnTypeChange(field.name)} />
              </Form.Item>
            </Card>
          ))}
          <Button type="dashed" onClick={() => { add(); addMapping() }} block>
            + Add SQL Mapping
          </Button>
        </div>
      )}
    </Form.List>
    <Divider />
    <Form.List name="indexes">
      {(fields, { add, remove }) => (
        <div style={{ display: 'flex', rowGap: 16, flexDirection: 'column' }}>
          {fields.map((field) => (
            <Card
              size="small"
              title={`SQL Index ${field.name + 1}`}
              key={field.key}
              extra={
                <CloseOutlined onClick={() => { remove(field.name); removeIndex(field.name) }} />
              }
            >
              <Form.Item
                label="Name"
                name={[field.name, 'name']}
                rules={[{ required: true, message: 'SQL Index Name' }]}
              >
                <Input onChange={indexNameChange(field.name)} />
              </Form.Item>
              <Form.Item
                label="Index"
                name={[field.name, 'index']}
                rules={[{ required: true, message: 'SQL Index' }]}
              >
                <Select options={indexOptions} onChange={indexChange} notFoundContent={empty('Mapping')} />
              </Form.Item>
            </Card>
          ))}
          <Button type="dashed" onClick={() => { add(); addIndex() }} block>
            + Add SQL Index
          </Button>
        </div>
      )}
    </Form.List>
  </Form>
}

export default SycableForm