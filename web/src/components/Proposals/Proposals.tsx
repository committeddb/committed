import { Table } from "antd"
import { Proposal } from "./proposal"

type ProposalsProps = {
  proposals: Proposal[]
}

const Proposals: React.FC<ProposalsProps> = ({ proposals }) => {
  // const dataSource = [
  //   {
  //     typeId: '1',
  //     typeName: 'Simple',
  //     key: '32',
  //     data: '10 Downing Street',
  //   },
  //   {
  //     typeId: '1',
  //     typeName: 'Simple',
  //     key: '100',
  //     data: '177A Bleecker Street',
  //   },
  // ]

  const columns = [
    {
      title: 'Type',
      dataIndex: 'typeName',
      key: 'typeName',
    },
    {
      title: 'Key',
      dataIndex: 'key',
      key: 'key',
    },
    {
      title: 'Data',
      dataIndex: 'data',
      key: 'data',
    },
  ]

  return <Table dataSource={proposals} columns={columns} pagination={{ position: ['none'] }} size='small'></Table>
}

export default Proposals