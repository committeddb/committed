import { queryOptions } from "@tanstack/react-query"

export type Database = {
  id: string
  mimeType: string
  data: string
}

export const url = (fragment: string): string => {
  return (import.meta.env.VITE_API_URL ?? '') + fragment
}

export const getDatabasesQuery = queryOptions({
  queryKey: ['getDatabases'],
  queryFn: async (): Promise<Map<string, Database>> => {

    console.log(`fetching ${url('/database')}`)
    const response = await fetch(url('/database'))
    const databases = await response.json()

    const map = new Map();
    databases.forEach((database: Database) => {
      map.set(database.id, database)
    });

    return map
  },
  staleTime: 10000
})

export const saveDatabaseFunction = (database: Database) => {
  return fetch(url(`/database/${database.id}`), {
    method: 'POST',
    body: database.data,
    headers: {
      'Content-Type': database.mimeType,
    }
  })
}