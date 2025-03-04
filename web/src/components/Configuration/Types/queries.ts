import { queryOptions } from "@tanstack/react-query"
import { createQuery, createSaveFunction, url } from '../../queries'
import { TypeGraph } from './type-graph'

const typePath = '/type'

export const getTypesQuery = createQuery({ queryKey: 'getTypes', path: typePath, })

export const saveTypeFunction = createSaveFunction({ path: typePath })

export const getTypeGraphQuery = (id: string) => {
  const end = new Date()
  const start = new Date(end.getTime())
  start.setHours(end.getHours() - 1)

  const u = url(`${typePath}/${id}?start=${start.toISOString()}&end=${end.toISOString()}`)

  return queryOptions({
    queryKey: ['getType', id],
    queryFn: async (): Promise<TypeGraph> => {
      console.log(`fetching ${u}`)

      const response = await fetch(u)
      if (response.ok) {
        return await response.json()
      }

      return []
    },
    staleTime: 10000
  })
}