import Configurations from '../Configurations'
import { getIngestablesQuery } from './queries'

const Ingestables: React.FC = () => {
    return <Configurations url='ingestables' query={getIngestablesQuery} />
}

export default Ingestables