import Configurations from '../Configurations'
import { getSyncablesQuery } from './queries'

const Syncables: React.FC = () => {
    return <Configurations url='syncables' query={getSyncablesQuery} />
}

export default Syncables