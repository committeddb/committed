import Configurations from '../Configurations'
import { getTypesQuery } from './queries'

const Types: React.FC = () => {
	return <Configurations url='types' query={getTypesQuery} />
}

export default Types