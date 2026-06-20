// The pathless layout route: it owns the fixed chrome (collapsible rail +
// topbar) and renders the matched page through <Outlet>. Sidebar and
// breadcrumbs derive from the registry + current location, so the shell never
// names a kind directly. It also installs the keyboard layer: a scope-stack
// provider, the ⌘K command palette (registry-fed), and g-navigation. Collapse
// state is persisted to localStorage.
//
// No TrayEditorProvider is installed yet, so console-core's YAML tray uses its
// dependency-free TextAreaEditor; the CodeMirror editor (and its ~145 kB chunk)
// is an APO-785 polish for the clrk feature views.

import { useCallback, useEffect, useMemo, useState } from 'react'
import {
  createFileRoute,
  Outlet,
  useLocation,
  useRouter,
} from '@tanstack/react-router'
import {
  AppShell,
  Breadcrumbs,
  CommandButton,
  CommandPalette,
  CreateProvider,
  IconButton,
  KeyboardScopeProvider,
  LinkProvider,
  Sidebar,
  Topbar,
  buildBreadcrumbs,
  buildResourceCommands,
  buildSidebar,
  cn,
  useCommandKeyBindings,
  useCreate,
  useDiscovery,
  useK8sList,
  useKeyboardScope,
  type Command,
  type GVR,
} from '@apoxy/console-core'
import {
  Asleep,
  Document,
  Light,
  Notification,
  SidePanelClose,
  SidePanelOpen,
} from '@carbon/icons-react'
import { registry } from '../registry'
import wordmark from '../assets/apoxy-wordmark.svg'
import { RouterLink } from '../router-link'
import { rootCrumbLabel } from '../project-context'
import {
  applyTheme,
  readTheme,
  storeTheme,
  THEME_KEY,
  type Theme,
} from '../theme'

const DOCS_URL = 'https://docs.apoxy.dev'

// One prompt shared by the top-bar trigger and the palette input.
const SEARCH_PLACEHOLDER = 'Search resources, actions…'

export const Route = createFileRoute('/_shell')({ component: Shell })

// A never-served GVR placeholder so the breadcrumb sibling query can be declared
// unconditionally (rules of hooks) and simply disabled off detail views.
const EMPTY_GVR: GVR = { group: '', version: '', resource: '' }

const COLLAPSE_KEY = 'clrk.console.sidebar-collapsed'
function readCollapsed(): boolean {
  try {
    return globalThis.localStorage?.getItem(COLLAPSE_KEY) === '1'
  } catch {
    return false
  }
}
function writeCollapsed(v: boolean): void {
  try {
    globalThis.localStorage?.setItem(COLLAPSE_KEY, v ? '1' : '0')
  } catch {
    /* storage unavailable — collapse just won't persist */
  }
}

function Shell() {
  // The scope provider must wrap the whole shell so the palette, list nav, and
  // tray all register against one stack. CreateProvider owns the single shared
  // "new object" tray, opened from the list "New" button and the ⌘K palette.
  return (
    <KeyboardScopeProvider>
      <CreateProvider>
        <ShellBody />
      </CreateProvider>
    </KeyboardScopeProvider>
  )
}

function ShellBody() {
  const router = useRouter()
  const navigate = useCallback(
    (to: string) => {
      void router.navigate({ to: to as never })
    },
    [router],
  )

  const { pathname } = useLocation()
  const segments = pathname.split('/').filter(Boolean)
  const slug = segments[0]
  // k8s names are URL-safe single segments — use the raw segment (no decode, so
  // a malformed %-escape in the URL can't throw and crash the chrome).
  const name = segments[1]
  const entry = slug ? registry.byPath(slug) : undefined

  const [collapsed, setCollapsed] = useState(readCollapsed)
  const toggleCollapsed = useCallback(() => {
    setCollapsed((c) => {
      const next = !c
      writeCollapsed(next)
      return next
    })
  }, [])

  const { isServed } = useDiscovery()
  const model = useMemo(() => buildSidebar(registry, { isServed }), [isServed])

  // Breadcrumb object-switcher: on a detail view (slug + name) offer a dropdown
  // of sibling objects of the same kind. The list is only queried there; lists
  // and the overview pass `enabled: false`, so the leaf crumb stays plain.
  const siblings = useK8sList(entry?.gvr ?? EMPTY_GVR, {
    enabled: Boolean(entry && name),
  })
  const leafSwitch = useMemo(() => {
    if (!entry || !name) return undefined
    const options = (siblings.data?.items ?? [])
      .map((o) => o.metadata?.name)
      .filter((n): n is string => Boolean(n))
      .map((n) => ({ id: n, label: n }))
    if (options.length < 2) return undefined
    return {
      value: name,
      options,
      ariaLabel: `Switch ${entry.kind}`,
      onSelect: (id: string) => {
        if (id !== name) navigate(`/${entry.path}/${id}`)
      },
    }
  }, [entry, name, siblings.data, navigate])

  // The root crumb names the deployment: the project slug, or `localhost` when
  // self-hosted — not a fixed brand label.
  const crumbs = buildBreadcrumbs(entry, name, {
    root: { label: rootCrumbLabel, to: '/' },
    leafSwitch,
  })
  const activePath = slug ? `/${slug}` : '/'

  // ⌘K command palette, fed by the registry (+ an Overview entry). `onCreate`
  // adds a "New <kind>" command for each editable kind, opening the shared tray.
  const { openCreate } = useCreate() ?? {}
  const [paletteOpen, setPaletteOpen] = useState(false)
  const commands = useMemo<Command[]>(
    () => [
      {
        id: 'home',
        title: 'Overview',
        group: 'Go to',
        keywords: ['home', 'dashboard'],
        keys: 'g h',
        run: () => navigate('/'),
      },
      ...buildResourceCommands(registry, {
        navigate,
        isServed,
        onCreate: openCreate,
      }),
    ],
    [navigate, isServed, openCreate],
  )

  // g-navigation is derived from the command list: every command with a `keys`
  // spec (Overview's `g h`, each kind's `g <shortcut>`) registers its binding
  // from the same source the palette renders, so the two can't drift.
  useCommandKeyBindings(commands)
  useKeyboardScope({
    level: 'global',
    bindings: [{ keys: 'mod+k', run: () => setPaletteOpen(true) }],
  })

  return (
    <LinkProvider component={RouterLink} navigate={navigate}>
      <AppShell
        sidebar={
          <Sidebar
            model={model}
            activePath={activePath}
            collapsed={collapsed}
            onToggleCollapsed={toggleCollapsed}
            toggleIcon={
              collapsed ? (
                <SidePanelOpen size={16} />
              ) : (
                <SidePanelClose size={16} />
              )
            }
            brand={<Brand />}
            footer={<UserFooter collapsed={collapsed} />}
          />
        }
        topbar={
          <Topbar
            breadcrumbs={<Breadcrumbs items={crumbs} />}
            actions={
              <>
                <CommandButton
                  placeholder={SEARCH_PLACEHOLDER}
                  onOpen={() => setPaletteOpen(true)}
                />
                <IconButton label="Documentation" href={DOCS_URL}>
                  {DocsIcon}
                </IconButton>
                <ThemeToggle />
                <IconButton label="Notifications" badge>
                  {BellIcon}
                </IconButton>
              </>
            }
          />
        }
      >
        <Outlet />
      </AppShell>
      <CommandPalette
        open={paletteOpen}
        onClose={() => setPaletteOpen(false)}
        commands={commands}
        placeholder={SEARCH_PLACEHOLDER}
        brand="clrk"
      />
    </LinkProvider>
  )
}

function Brand() {
  // The Apoxy wordmark (near-black art) inverted to read white on the dark rail,
  // with the product suffix — "apoxy CLRK" — matching the CLRK dashboard design.
  return (
    <>
      <img
        src={wordmark}
        alt="Apoxy"
        className="h-[20px] w-auto [filter:invert(1)_brightness(2)]"
      />
      <span className="font-mono text-[length:var(--t-micro)] uppercase tracking-[0.14em] text-[color:var(--rail-text-dim)]">
        clrk
      </span>
    </>
  )
}

// Topbar glyphs — IBM Carbon, matching the rail icons and the CLRK dashboard
// design. (Sun/moon swap with the theme; docs + notifications are static.)
const SunIcon = <Light size={16} />
const MoonIcon = <Asleep size={16} />
const DocsIcon = <Document size={16} />
const BellIcon = <Notification size={16} />

function ThemeToggle() {
  const [theme, setTheme] = useState<Theme>(readTheme)
  useEffect(() => {
    applyTheme(theme)
  }, [theme])
  // Follow a theme change made in another tab.
  useEffect(() => {
    const onStorage = (e: StorageEvent) => {
      if (e.key === THEME_KEY) setTheme(readTheme())
    }
    window.addEventListener('storage', onStorage)
    return () => window.removeEventListener('storage', onStorage)
  }, [])
  const dark = theme === 'dark'
  const toggle = () => {
    setTheme((cur) => {
      const next: Theme = cur === 'dark' ? 'light' : 'dark'
      storeTheme(next)
      return next
    })
  }
  return (
    <IconButton
      label={dark ? 'Switch to light mode' : 'Switch to dark mode'}
      pressed={dark}
      onClick={toggle}
    >
      {dark ? SunIcon : MoonIcon}
    </IconButton>
  )
}

function UserFooter({ collapsed }: { collapsed: boolean }) {
  return (
    <div
      className={cn(
        'flex items-center',
        collapsed ? 'justify-center gap-0' : 'gap-[10px]',
      )}
    >
      <span className="flex h-7 w-7 flex-none items-center justify-center rounded-full bg-[var(--rail-text)] text-[length:var(--t-overline)] font-semibold text-[color:var(--rail-bg)]">
        U
      </span>
      {!collapsed && (
        <div className="min-w-0">
          <div className="truncate text-[length:var(--t-body-sm)] font-medium text-[color:var(--rail-text)]">
            Signed in
          </div>
          <div className="truncate font-mono text-[length:var(--t-overline)] text-[color:var(--rail-text-dim)]">
            console
          </div>
        </div>
      )}
    </div>
  )
}
