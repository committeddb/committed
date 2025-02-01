import { createFileRoute } from '@tanstack/react-router'
import { Type } from '../components'
import { getTypeGraphQuery } from '../components/Configuration/Types/queries'

export const Route = createFileRoute('/types/$typeId')({
  loader: async ({ context: { queryClient }, params: { typeId } }) => queryClient.ensureQueryData(getTypeGraphQuery(typeId)),
  component: Type,
})