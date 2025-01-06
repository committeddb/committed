import { createLazyFileRoute } from '@tanstack/react-router'
import { Type } from '../components'

export const Route = createLazyFileRoute('/types/$typeId')({
  component: Type,
})