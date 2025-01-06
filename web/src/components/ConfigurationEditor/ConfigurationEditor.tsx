import { Dispatch, SetStateAction } from 'react';
import { Controlled as CodeMirror } from 'react-codemirror2'
import 'codemirror/lib/codemirror.css'

type ConfigurationEditorProps = {
  value: string,
  setValue: Dispatch<SetStateAction<string>>
}

const ConfigurationEditor: React.FC<ConfigurationEditorProps> = ({ value, setValue }) => {
  return <CodeMirror
    value={value}
    options={{
      lineNumbers: true,
    }}
    onBeforeChange={(_editor, _data, value) => {
      console.log("About to change")
      setValue(value)
    }}
  />
}

export default ConfigurationEditor