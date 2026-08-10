// The app's single ConsoleClient: a GVR client + QueryClient + WatchManager
// scoped to one project. Origin and project come from build-time env
// (VITE_CLRK_API_URL / VITE_CLRK_PROJECT_ID), defaulting to the serving origin
// so a co-hosted console "just works". clrk's apiserver isn't project-scoped, so
// the project id rides as a header the apiserver simply ignores; the scopeKey it
// produces still gives the WatchManager a stable identity to key watches by.

import { createConsoleClient, ProjectRequestDecorator, type ConsoleClient } from '@apoxy/console-core'
import { baseUrl, projectId } from './project-context'

export const consoleClient: ConsoleClient = createConsoleClient({
  decorator: new ProjectRequestDecorator({ baseUrl, projectId }),
  watchTransport: 'websocket',
})
