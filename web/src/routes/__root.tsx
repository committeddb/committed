import { createRootRouteWithContext } from '@tanstack/react-router'
import { TanStackRouterDevtools } from '@tanstack/router-devtools'
import { QueryClient } from '@tanstack/react-query'
import App from '../components/App'

export const Route = createRootRouteWithContext<{queryClient: QueryClient}>()({
  component: () => (
    <>
        <App/>
        <TanStackRouterDevtools/>
    </>
  ),
})