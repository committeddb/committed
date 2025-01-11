import ConfigurationDetail from '../ConfigurationDetail'
import { getSyncablesQuery, saveSyncableFunction } from './queries'
import SyncableForm from './SyncableForm'

const SyncableDetail: React.FC = () => {
  return <ConfigurationDetail
    paramName='syncableId'
    mutationFn={saveSyncableFunction}
    query={getSyncablesQuery}
    form={SyncableForm}
  />
}

export default SyncableDetail