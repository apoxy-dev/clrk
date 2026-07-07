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
  type K8sObject,
} from '@apoxy/console-core'
import {
  Asleep,
  Dashboard,
  Document,
  Light,
  SidePanelClose,
  SidePanelOpen,
} from '@carbon/icons-react'
import { registry, daemonAgentEntry } from '../registry'
import { NotificationBell } from '../views/notification-bell'
import { POLICY_RESOURCE, POLICY_SHORT, type PolicyKind } from '../views/policies-data'
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

const v1alpha1 = (resource: string): GVR => ({
  group: 'clrk.apoxy.dev',
  version: 'v1alpha1',
  resource,
})

// Short CRD name (cip/frp/edp/rlp/lp) → kind, for resolving the policy detail
// route's `<kindShort>` segment back to a kind.
const POLICY_KIND_BY_SHORT: Record<string, PolicyKind> = Object.fromEntries(
  (Object.entries(POLICY_SHORT) as Array<[PolicyKind, string]>).map(([kind, short]) => [
    short,
    kind,
  ]),
)

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
  const entry = slug ? registry.byPath(slug) : undefined
  // Most detail routes are two-segment (`/<slug>/<name>`), so the leaf object is
  // segments[1]. The Policies detail is the lone bespoke three-segment route
  // (`/policies/<kindShort>/<name>`): there the object is segments[2] and
  // segments[1] names its kind. Resolve the leaf name from the right segment so
  // the breadcrumb labels the policy, not its kind.
  // k8s names are URL-safe single segments — use the raw segment (no decode, so
  // a malformed %-escape in the URL can't throw and crash the chrome).
  const isPolicyLeaf = slug === 'policies' && segments.length >= 3
  const policyShort = isPolicyLeaf ? segments[1] : undefined
  const name = isPolicyLeaf ? segments[2] : segments[1]

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
  // and the overview pass `enabled: false`, so the leaf crumb stays plain. The
  // policy leaf has its own (cross-kind) switcher below, so it disables this one.
  const siblings = useK8sList(entry?.gvr ?? EMPTY_GVR, {
    enabled: Boolean(entry && name && !isPolicyLeaf),
  })
  // The combined `Agent` entry (path `agents`) aggregates two kinds under one
  // route, so its sibling switcher must offer BOTH TaskAgents and DaemonAgents —
  // fetch the daemon list alongside the entry's own (task) list. Disabled
  // elsewhere, and graceful (empty) when daemonagents isn't a served GVR.
  const isAgentLeaf = entry?.path === 'agents'
  const daemonSiblings = useK8sList(daemonAgentEntry.gvr, {
    enabled: Boolean(isAgentLeaf && name),
  })
  // The combined `Policy` entry spans five kinds, and its detail route is
  // three-segment (`/policies/<short>/<name>`). Its switcher offers EVERY policy
  // across the five kinds — one list per kind, queried only on the policy leaf —
  // and navigates back to the kind-qualified URL. Mirrors the agent leaf, scaled
  // to five kinds.
  const polEnabled = Boolean(isPolicyLeaf && name)
  const cipList = useK8sList(v1alpha1(POLICY_RESOURCE.CredentialInjectionPolicy), { enabled: polEnabled })
  const frpList = useK8sList(v1alpha1(POLICY_RESOURCE.FallbackRoutingPolicy), { enabled: polEnabled })
  const edpList = useK8sList(v1alpha1(POLICY_RESOURCE.EgressDenyPolicy), { enabled: polEnabled })
  const rlpList = useK8sList(v1alpha1(POLICY_RESOURCE.RateLimitPolicy), { enabled: polEnabled })
  const lpList = useK8sList(v1alpha1(POLICY_RESOURCE.LoggingPolicy), { enabled: polEnabled })

  const leafSwitch = useMemo(() => {
    if (!entry || !name) return undefined

    // Policy leaf: a cross-kind switcher keyed by `<short>/<name>`.
    if (isPolicyLeaf) {
      const groups: Array<[PolicyKind, K8sObject[] | undefined]> = [
        ['CredentialInjectionPolicy', cipList.data?.items],
        ['FallbackRoutingPolicy', frpList.data?.items],
        ['EgressDenyPolicy', edpList.data?.items],
        ['RateLimitPolicy', rlpList.data?.items],
        ['LoggingPolicy', lpList.data?.items],
      ]
      const options = groups
        .flatMap(([kind, items]) =>
          (items ?? [])
            .map((o) => o.metadata?.name)
            .filter((n): n is string => Boolean(n))
            .map((n) => ({ id: `${POLICY_SHORT[kind]}/${n}`, label: n })),
        )
        .sort((a, b) => a.label.localeCompare(b.label))
      if (options.length < 2) return undefined
      const current = `${policyShort}/${name}`
      const kind = policyShort ? POLICY_KIND_BY_SHORT[policyShort] : undefined
      return {
        value: current,
        options,
        ariaLabel: kind ? `Switch ${kind}` : 'Switch policy',
        onSelect: (id: string) => {
          if (id !== current) navigate(`/policies/${id}`)
        },
      }
    }

    const items = isAgentLeaf
      ? [...(siblings.data?.items ?? []), ...(daemonSiblings.data?.items ?? [])]
      : siblings.data?.items ?? []
    const names = items
      .map((o) => o.metadata?.name)
      .filter((n): n is string => Boolean(n))
    const options = [...new Set(names)]
      .sort((a, b) => a.localeCompare(b))
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
  }, [
    entry,
    name,
    isPolicyLeaf,
    policyShort,
    cipList.data,
    frpList.data,
    edpList.data,
    rlpList.data,
    lpList.data,
    isAgentLeaf,
    siblings.data,
    daemonSiblings.data,
    navigate,
  ])

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
            home={{
              to: '/',
              label: 'Overview',
              icon: <Dashboard size={16} />,
            }}
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
                <NotificationBell />
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
