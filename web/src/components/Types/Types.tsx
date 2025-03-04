import { Select } from "antd"

const Types: React.FC = () => {
  const typeOptions = [
    {
      value: '1',
      label: 'Simple',
    }
  ]

  return <Select
    showSearch
    style={{ width: 200 }}
    placeholder="Search to Select"
    optionFilterProp="label"
    filterSort={(optionA, optionB) =>
      (optionA?.label ?? '').toLowerCase().localeCompare((optionB?.label ?? '').toLowerCase())
    }
    options={typeOptions}
  />
}

export default Types