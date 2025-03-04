import { createQuery, createSaveFunction } from '../../queries'

const path = '/syncable'

export const getSyncablesQuery = createQuery({ queryKey: 'getSyncables', path, })

export const saveSyncableFunction = createSaveFunction({ path })