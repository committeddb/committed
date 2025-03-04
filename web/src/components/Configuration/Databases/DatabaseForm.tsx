import React from 'react'
import { Divider, Form, Input, Select } from 'antd'
import { ConfigurationData } from '../configuration'
import { parse, stringify } from "@std/toml";
import { Database, emptyDatabase } from './database'

type FieldType = {
  name?: string
  type?: string
  dialect?: string
  connectionString?: string
}

const DatabaseForm: React.FC<ConfigurationData> = ({ data, setData }) => {
  let database = emptyDatabase()
  if (data) {
    database = parse(data) as Database
  } else {
    setData(stringify(database))
  }

  const nameChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    database.database.name = e.target?.value
    setData(stringify(database))
  }

  const typeChange = (value: string) => {
    database.database.type = value
    setData(stringify(database))
  }

  const dialectChange = (value: string) => {
    database.sql.dialect = value
    setData(stringify(database))
  }

  const connectionStringChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    database.sql.connectionString = e.target?.value
    setData(stringify(database))
  }

  const values = {
    name: database.database.name,
    type: database.database.type,
    dialect: database.sql.dialect,
    connectionString: database.sql.connectionString
  }

  return <Form name="basic" initialValues={values}>
    <Form.Item<FieldType>
      label="Name"
      name="name"
      rules={[{ required: true, message: 'Database Name' }]}
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
  </Form>
}

export default DatabaseForm