import { createLazyFileRoute } from '@tanstack/react-router'
import { Database } from '../components'

export const Route = createLazyFileRoute('/databases/$databaseId')({
  component: Database,
})
