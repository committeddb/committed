import { createFileRoute } from '@tanstack/react-router'
import { getIngestablesQuery, Ingestables } from '../components/Configuration'

export const Route = createFileRoute('/ingestables')({
  loader: async ({ context: { queryClient } }) => queryClient.ensureQueryData(getIngestablesQuery),
  component: Ingestables,
})
