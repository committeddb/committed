import { createLazyFileRoute } from '@tanstack/react-router'
import { Ingestable } from '../components/Configuration'

export const Route = createLazyFileRoute('/ingestables/$ingestableId')({
  component: Ingestable,
})
