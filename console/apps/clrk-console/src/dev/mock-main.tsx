// Dev-only entry for the mocked console instance (served at /mock.html). It is
// the production bootstrap (src/main.tsx) with the live apiserver swapped for
// the in-memory MockBackend, so the feature views (lists, detail, YAML tray,
// palette, keyboard nav) are exercisable without a backend. Not referenced by
// the production build.

import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import { RouterProvider } from '@tanstack/react-router'
import { ConsoleProvider, createConsoleClient, ProjectRequestDecorator } from '@apoxy/console-core'
import '@apoxy/console-core/tokens.css'
import '../styles.css'
import { router } from '../router'
import { MockBackend } from './mock-backend'

const backend = new MockBackend()
backend.seedDemo()

// The GVR client (lists/watches/SSA) takes the backend's fetch directly. The
// bespoke agent traces subresource is a raw `fetch` (the decorated absolute URL),
// so point the global fetch at the backend too — otherwise the swimlane's read
// would escape to the network. Dev-only; production keeps the real global fetch.
globalThis.fetch = backend.fetch

// The MockBackend matches on path only, so the baseUrl host is a sentinel.
const consoleClient = createConsoleClient({
  decorator: new ProjectRequestDecorator({ baseUrl: 'http://mock.clrk.local', projectId: 'default' }),
  fetch: backend.fetch,
})

// The mock entry is served at /mock.html, but the router matches on pathname --
// so rewrite the initial location to a real route before first render. Land on
// the Notification Center (the feature under active design) so opening the URL
// drops straight into it; the rail then navigates anywhere client-side.
if (
  window.location.pathname === '/mock.html' ||
  window.location.pathname === '/'
) {
  window.history.replaceState(null, '', '/notifications')
}

const rootEl = document.getElementById('root')
if (!rootEl) throw new Error('#root not found')

createRoot(rootEl).render(
  <StrictMode>
    <ConsoleProvider client={consoleClient}>
      <RouterProvider router={router} />
    </ConsoleProvider>
  </StrictMode>,
)
