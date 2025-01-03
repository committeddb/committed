import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { RouterProvider, createRouter } from '@tanstack/react-router'
import { routeTree } from './routeTree.gen'
import { Skeleton, Spin } from 'antd'
import './index.css'

// Great tanstack router + query video: https://www.youtube.com/watch?v=rqAa8Xpc0Zc
const queryClient = new QueryClient()
const router = createRouter({
  routeTree,
  defaultPendingComponent: Spin,
  defaultErrorComponent: Skeleton,
  context: { queryClient }
})

// Register the router instance for type safety
declare module '@tanstack/react-router' {
  interface Register {
    router: typeof router
  }
}

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <QueryClientProvider client={queryClient}>
      <RouterProvider router={router} />
    </QueryClientProvider>
  </StrictMode>,
)
