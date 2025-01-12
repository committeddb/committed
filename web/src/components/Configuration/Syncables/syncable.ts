export type Syncable = {
  syncable: SyncableDetail
  sql: SQLDetail
}

type SyncableDetail = {
  name: string
  type: string
}

type SQLDetail = {
  db: string
  topic: string
  primaryKey: string
  table: string
  indexes?: SQLIndex[]
  mappings?: SQLMapping[]
}

type SQLIndex = {
  name: string
  index: string
}

type SQLMapping = {
  jsonPath: string
  column: string
  type: string
}

export const emptySyncable = (): Syncable => {
  return {
    syncable: {
      name: '',
      type: 'sql'
    },
    sql: {
      db: 'mysql',
      topic: '',
      primaryKey: '',
      table: ''
    }
  }
}