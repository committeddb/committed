import { ConfigurationDetail } from '../ConfigurationEditor'
import { getTypesQuery, saveTypeFunction } from './queries'

const Type: React.FC = () => {
  return <ConfigurationDetail paramName='typeId' mutationFn={saveTypeFunction} query={getTypesQuery} />
}

export default Type