import { createFileRoute } from '@tanstack/react-router'
import { Databases, getDatabasesQuery } from '../components'

export const Route = createFileRoute('/databases')({
  loader: async ({context: { queryClient }}) => queryClient.ensureQueryData(getDatabasesQuery),
  component: Databases,
})
