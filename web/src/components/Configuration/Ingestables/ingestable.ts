export type Ingestable = {
  ingestable: IngestableDetail
  sql: SQLDetail
}

type IngestableDetail = {
  name: string
  type: string
}

type SQLDetail = {
  dialect: string
  connectionString: string
  primaryKey: string
  topic: string
  mappings?: SQLMapping[]
}

type SQLMapping = {
  jsonName: string
  column: string
}

export const emptyIngestable = (): Ingestable => {
  return {
    ingestable: {
      name: '',
      type: 'sql'
    },
    sql: {
      dialect: 'mysql',
      connectionString: '',
      primaryKey: '',
      topic: ''
    }
  }
}