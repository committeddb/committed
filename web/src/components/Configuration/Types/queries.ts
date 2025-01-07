import { queryOptions } from "@tanstack/react-query"

export type Type = {
  id: string
  mimeType: string
  data: string
}

export const url = (fragment: string): string => {
  return (import.meta.env.VITE_API_URL ?? '') + fragment
}

export const getTypesQuery = queryOptions({
  queryKey: ['getTypes'],
  queryFn: async (): Promise<Map<string, Type>> => {

    console.log(`fetching ${url('/type')}`)
    const response = await fetch(url('/type'))
    const types = await response.json()

    const map = new Map();
    types.forEach((type: Type) => {
      map.set(type.id, type)
    });

    return map
  },
  staleTime: 10000
})

export const saveTypeFunction = (type: Type) => {
  return fetch(url(`/type/${type.id}`), {
    method: 'POST',
    body: type.data,
    headers: {
      'Content-Type': type.mimeType,
    }
  })
}