import ConfigurationDetail from '../ConfigurationDetail'
import TypeForm from './TypeForm'
import { getTypesQuery, saveTypeFunction } from './queries'

const TypeDetail: React.FC = () => {
  return <ConfigurationDetail
    paramName='typeId'
    mutationFn={saveTypeFunction}
    query={getTypesQuery}
    form={TypeForm}
  />
}

export default TypeDetail