import { useParams } from '@tanstack/react-router'
import ConfigurationDetail from '../ConfigurationDetail'
import TypeForm from './TypeForm'
import { Divider, Space } from 'antd'
import { getTypesQuery, getTypeGraphQuery, saveTypeFunction } from './queries'
import { useSuspenseQuery } from '@tanstack/react-query'
import { Line } from 'react-chartjs-2'
import 'chartjs-adapter-date-fns'
import { enUS } from 'date-fns/locale';
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  TimeScale,
  Tooltip,
} from 'chart.js';

ChartJS.register(
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  TimeScale,
  Tooltip,
)

const TypeDetail: React.FC = () => {
  const paramName = 'typeId'
  const param: any = useParams({ strict: false })
  const id = param[paramName]
  const { data: graph } = useSuspenseQuery(getTypeGraphQuery(id))

  const options = {
    adapters: {
      date: {
        locale: enUS
      }
    },
    responsive: true,
    scales: {
      x: {
        type: 'time' as const,
        time: {
          displayFormats: {
            minute: 'HH:mm'
          },
          unit: 'minute' as const,
        },
        title: {
          display: false,
        }
      },
    }
  }

  let graphElement = <></>
  if (graph && graph.length > 0) {
    const data = {
      datasets: [
        {
          label: 'Type',
          data: graph,
          borderColor: 'rgb(53, 162, 235)',
          backgroundColor: 'rgba(53, 162, 235, 0.5)',
        },
      ],
    }
    graphElement = <><Divider /><Line options={options} data={data} /></>
  }

  return <Space direction="vertical">
    <ConfigurationDetail
      paramName={paramName}
      mutationFn={saveTypeFunction}
      query={getTypesQuery}
      form={TypeForm}
    />
    {graphElement}
  </Space>
}

export default TypeDetail