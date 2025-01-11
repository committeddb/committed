import ConfigurationDetail from '../ConfigurationDetail'
import IngestableForm from './IngestableForm'
import { getIngestablesQuery, saveIngestableFunction } from './queries'

const IngestableDetail: React.FC = () => {
  return <ConfigurationDetail
    paramName='ingestableId'
    mutationFn={saveIngestableFunction}
    query={getIngestablesQuery}
    form={IngestableForm}
  />
}

export default IngestableDetail