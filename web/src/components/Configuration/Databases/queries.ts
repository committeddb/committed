import { createQuery, createSaveFunction } from '../../queries'

const path = '/database'

export const getDatabasesQuery = createQuery({ queryKey: 'getDatabases', path, })

export const saveDatabaseFunction = createSaveFunction({ path })