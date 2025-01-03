import { createLazyFileRoute } from '@tanstack/react-router'
import { Syncables } from '../components'

export const Route = createLazyFileRoute('/syncables')({
  component: Syncables,
})
