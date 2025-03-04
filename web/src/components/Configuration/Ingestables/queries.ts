import { createQuery, createSaveFunction } from '../../queries'

const path = '/ingestable'

export const getIngestablesQuery = createQuery({ queryKey: 'getIngestables', path, })

export const saveIngestableFunction = createSaveFunction({ path })