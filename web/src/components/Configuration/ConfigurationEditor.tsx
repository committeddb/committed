import Editor from '@monaco-editor/react'
import * as monaco from 'monaco-editor/esm/vs/editor/editor.api'

type ConfigurationEditorProps = {
  value: string,
  setValue: (value: string) => void
}

const ConfigurationEditor: React.FC<ConfigurationEditorProps> = ({ value, setValue }) => {
  const editorDidMount = (editor: monaco.editor.IStandaloneCodeEditor, _monaco: typeof monaco) => {
    editor.focus();
  }

  const onChange = (value: string | undefined, _e: monaco.editor.IModelContentChangedEvent) => {
    if (value) setValue(value)
  }

  const options: monaco.editor.IStandaloneEditorConstructionOptions = {
    selectOnLineNumbers: false,
    colorDecorators: true,
    readOnly: true
  }

  return <Editor
    height='65vh'
    value={value}
    options={options}
    onChange={onChange}
    onMount={editorDidMount}
  />
}

export default ConfigurationEditor