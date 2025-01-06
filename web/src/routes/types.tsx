import { createFileRoute } from '@tanstack/react-router'
import { getTypesQuery, Types } from '../components'

export const Route = createFileRoute('/types')({
  loader: async ({ context: { queryClient } }) => queryClient.ensureQueryData(getTypesQuery),
  component: Types,
})
