import { queryOptions } from "@tanstack/react-query"
import { url } from '../queries'
import { Proposal } from './proposal'

const proposalPath = '/proposal'

export const getProposalsQuery = (id: string) => {
  const u = url(`${proposalPath}/${id}`)

  return queryOptions({
    queryKey: ['getProposals', id],
    queryFn: async (): Promise<Proposal[]> => {
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