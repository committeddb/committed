import { createLazyFileRoute } from '@tanstack/react-router'
import { Ingestables } from '../components'

export const Route = createLazyFileRoute('/ingestables')({
  component: Ingestables,
})
