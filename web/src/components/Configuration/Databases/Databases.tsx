import Configurations from '../Configurations'
import { getDatabasesQuery } from './queries'

const Databases: React.FC = () => {
  return <Configurations url='databases' query={getDatabasesQuery} />
}

export default Databases