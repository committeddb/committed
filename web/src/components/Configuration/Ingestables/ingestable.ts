export type Ingestable = {
  ingestable: IngestableDetail
}

type IngestableDetail = {
  name: string
}

export const emptyIngestable = (): Ingestable => {
  return {
    ingestable: {
      name: ''
    }
  }
}