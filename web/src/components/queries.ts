import { queryOptions } from "@tanstack/react-query"
import { Configuration } from "./Configuration/configuration"

export const url = (fragment: string): string => {
  return (import.meta.env.VITE_API_URL ?? '') + fragment
}

type createQueryParameters = {
  queryKey: string
  path: string
}

export const createQuery = ({ queryKey, path }: createQueryParameters) => {
  return queryOptions({
    queryKey: [queryKey],
    queryFn: async (): Promise<Map<string, Configuration>> => {
      console.log(`fetching ${url(path)}`)
      const response = await fetch(url(path))

      const map = new Map()
      if (response.ok) {
        const types = await response.json()

        types.forEach((type: Configuration) => {
          map.set(type.id, type)
        })
      }

      return map
    },
    staleTime: 10000
  })
}

type createSaveFunctionParameters = {
  path: string
}

export const createSaveFunction = ({ path }: createSaveFunctionParameters) => {
  return (type: Configuration) => {
    return fetch(url(`${path}/${type.id}`), {
      method: 'POST',
      body: type.data,
      headers: {
        'Content-Type': type.mimeType,
      }
    })
  }
}