import { createLazyFileRoute } from '@tanstack/react-router'
import { Syncable } from '../components/Configuration'

export const Route = createLazyFileRoute('/syncables/$syncableId')({
  component: Syncable,
})
