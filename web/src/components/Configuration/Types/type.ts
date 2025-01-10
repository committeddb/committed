export type Type = {
  type: TypeDetail
}

type TypeDetail = {
  name: string
}

export const emptyType = (): Type => {
  return {
    type: {
      name: ''
    }
  }
}