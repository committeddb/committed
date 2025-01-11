export type Syncable = {
  syncable: SyncableDetail
}

type SyncableDetail = {
  name: string
}

export const emptySyncable = (): Syncable => {
  return {
    syncable: {
      name: ''
    }
  }
}