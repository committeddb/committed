export type Configuration = {
  id: string
  mimeType: string
  data: string
}

export type ConfigurationData = {
  data: string,
  setData: React.Dispatch<React.SetStateAction<string>>
}