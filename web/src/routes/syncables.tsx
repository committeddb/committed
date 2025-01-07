import { createFileRoute } from '@tanstack/react-router'
import { getSyncablesQuery, Syncables } from '../components/Configuration'

export const Route = createFileRoute('/syncables')({
  loader: async ({ context: { queryClient } }) => queryClient.ensureQueryData(getSyncablesQuery),
  component: Syncables,
})
