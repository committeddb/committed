export type Proposal = {
    entities: Entity[]
}

type Entity = {
    typeId: string
    typeName: string
    key: string
    data: string
}