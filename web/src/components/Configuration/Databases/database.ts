export type Database = {
  database: DatabaseDetail
  sql: SQLDetail
}

type DatabaseDetail = {
  name: string
  type: string
}

type SQLDetail = {
  dialect: string
  connectionString: string
}

export const emptyDatabase = (): Database => {
  return {
    database: {
      name: '',
      type: 'sql'
    },
    sql: {
      dialect: 'mysql',
      connectionString: ''
    }
  }
}