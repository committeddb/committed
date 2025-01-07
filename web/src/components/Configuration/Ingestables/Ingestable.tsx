import ConfigurationDetail from '../ConfigurationDetail'
import { getIngestablesQuery, saveIngestableFunction } from './queries'

const Ingestable: React.FC = () => {
  return <ConfigurationDetail paramName='ingestableId' mutationFn={saveIngestableFunction} query={getIngestablesQuery} />
}

export default Ingestable