import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import tailwindcss from '@tailwindcss/vite'
import { tanstackRouter } from '@tanstack/router-plugin/vite'

// Optional dev proxy: when VITE_CLRK_PROXY points at a running clrk apiserver
// (e.g. `http://localhost:18080`, the controller-manager console proxy), the dev
// server forwards k8s API traffic there so the app runs against a live cluster's
// data with hot reload -- same-origin from the browser's view, so no CORS. Unset
// by default; has no effect on the production build.
const apiProxy = process.env.VITE_CLRK_PROXY

// Vite 8 / Rolldown. The router plugin must precede @vitejs/plugin-react.
// `tanstackRouter` generates src/routeTree.gen.ts on dev/build via the
// file-routing Vite plugin, not the sidecar CLI.
export default defineConfig({
  plugins: [
    tanstackRouter({ target: 'react', autoCodeSplitting: true }),
    react(),
    tailwindcss(),
  ],
  server: apiProxy
    ? {
        proxy: {
          '/api': { target: apiProxy, changeOrigin: true },
          '/apis': { target: apiProxy, changeOrigin: true },
        },
      }
    : undefined,
})
