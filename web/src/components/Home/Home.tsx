import { useState } from "react"
import { Button, Modal, Space } from "antd"
import AddProposalForm from "../Proposals/AddProposalForm"
import Proposals from "../Proposals/Proposals"
import DeleteProposalForm from "../Proposals/DeleteProposalForm"

const Home: React.FC = () => {
  const [isAddProposalModalOpen, setIsAddProposalModalOpen] = useState(false)
  const [isDeleteProposalModalOpen, setIsDeleteProposalModalOpen] = useState(false)

  const showAddProposalModal = () => {
    setIsAddProposalModalOpen(true);
  };

  const handleAddProposalOk = () => {
    setIsAddProposalModalOpen(false);
  };

  const handleAddProposalCancel = () => {
    setIsAddProposalModalOpen(false);
  }

  const showDeleteProposalModal = () => {
    setIsDeleteProposalModalOpen(true);
  };

  const handleDeleteProposalOk = () => {
    setIsDeleteProposalModalOpen(false);
  };

  const handleDeleteProposalCancel = () => {
    setIsDeleteProposalModalOpen(false);
  }

  const dataSource = [
    {
      entities: [
        {
          typeId: '1',
          typeName: 'Simple',
          key: '32',
          data: '10 Downing Street',
        },
        {
          typeId: '1',
          typeName: 'Simple',
          key: '100',
          data: '177A Bleecker Street',
        },
      ]
    }
  ]

  return (
    <Space direction="vertical">
      <Space>
        <Button onClick={showAddProposalModal}>Add Proposal...</Button>
        <Button onClick={showDeleteProposalModal}>Delete Proposal...</Button>
      </Space>
      <Modal title="Add Proposal" open={isAddProposalModalOpen} onOk={handleAddProposalOk} onCancel={handleAddProposalCancel} width={1000}>
        <AddProposalForm />
      </Modal>
      <Modal title="Delete Proposal" open={isDeleteProposalModalOpen} onOk={handleDeleteProposalOk} onCancel={handleDeleteProposalCancel}>
        <DeleteProposalForm />
      </Modal>
      <Proposals proposals={dataSource} />
    </Space>
  )
}

export default Home