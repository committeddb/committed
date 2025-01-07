import { createQuery, createSaveFunction } from '../queries'

const path = '/type'

export const getTypesQuery = createQuery({ queryKey: 'getTypes', path, })

export const saveTypeFunction = createSaveFunction({ path })