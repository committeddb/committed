import { ConfigurationDetail } from '../ConfigurationEditor'
import { getDatabasesQuery, saveDatabaseFunction } from './queries'

const Databases: React.FC = () => {
  return <ConfigurationDetail paramName='databaseId' mutationFn={saveDatabaseFunction} query={getDatabasesQuery} />
}

export default Databases