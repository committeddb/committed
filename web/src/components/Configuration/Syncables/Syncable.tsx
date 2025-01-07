import ConfigurationDetail from '../ConfigurationDetail'
import { getSyncablesQuery, saveSyncableFunction } from './queries'

const Syncable: React.FC = () => {
  return <ConfigurationDetail paramName='syncableId' mutationFn={saveSyncableFunction} query={getSyncablesQuery} />
}

export default Syncable