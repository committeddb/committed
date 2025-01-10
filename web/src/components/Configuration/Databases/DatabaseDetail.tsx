import ConfigurationDetail from '../ConfigurationDetail'
import DatabaseForm from './DatabaseForm'
import { getDatabasesQuery, saveDatabaseFunction } from './queries'

const DatabaseDetail: React.FC = () => {
  return <ConfigurationDetail
    paramName='databaseId'
    mutationFn={saveDatabaseFunction}
    query={getDatabasesQuery}
    form={DatabaseForm}
  />
}

export default DatabaseDetail