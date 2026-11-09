import { useEffect, useRef, useState } from 'react';
import { createBrowserRouter, RouterProvider, Outlet, useLocation, useNavigate } from 'react-router';
import './bank.css';
import { api, type AppConfig, type AutoOff } from '@/lib/api';
import { BankView } from '@/views/BankView';
import { DashboardView } from '@/views/DashboardView';
import { HowView } from '@/views/HowView';

const TABS: { path: string; label: string }[] = [
  { path: '/', label: '1 · Lakeshore Bank (customer view)' },
  { path: '/dashboard', label: '2 · Live stream' },
  { path: '/how', label: '3 · How it works' },
];

export interface ShellContext {
  config: AppConfig | null;
}

const HEARTBEAT_MS = 60_000; // at most one activity heartbeat a minute
const WARN_S = 5 * 60; // show the Keep on banner in the last 5 minutes before auto-off

// Mouse, scroll, touch, or keys count as someone using the demo, so a presenter talking over a tab keeps it on.
// A tab left open with nobody at it sends nothing and still turns off. Checks the countdown every 20 s.
function useAutoOff() {
  const [status, setStatus] = useState<AutoOff | null>(null);
  const lastSent = useRef(0);
  useEffect(() => {
    const beat = () => {
      if (document.visibilityState !== 'visible' || Date.now() - lastSent.current < HEARTBEAT_MS) return;
      lastSent.current = Date.now();
      api.activity().then(setStatus, () => undefined);
    };
    const events = ['pointermove', 'pointerdown', 'wheel', 'scroll', 'keydown', 'touchstart'] as const;
    events.forEach((e) => window.addEventListener(e, beat, { passive: true, capture: true }));
    const poll = () => api.autoOff().then(setStatus, () => undefined);
    void poll();
    const t = setInterval(poll, 20_000);
    return () => {
      events.forEach((e) => window.removeEventListener(e, beat, { capture: true }));
      clearInterval(t);
    };
  }, []);
  const keepOn = () => {
    lastSent.current = Date.now();
    api.activity().then(setStatus, () => undefined);
  };
  return { status, keepOn };
}

function Shell() {
  const loc = useLocation();
  const nav = useNavigate();
  const [xray, setXray] = useState(false);
  const [config, setConfig] = useState<AppConfig | null>(null);
  const { status: autoOff, keepOn } = useAutoOff();
  const offIn = autoOff?.off_in_s ?? null;

  useEffect(() => {
    api.config().then(setConfig).catch(() => setConfig(null));
  }, []);

  const onDash = loc.pathname.startsWith('/dashboard');
  const onHow = loc.pathname.startsWith('/how');
  const onBank = !onDash && !onHow;
  const isOn = (path: string) => (path === '/' ? onBank : loc.pathname.startsWith(path));
  const ctx: ShellContext = { config };

  return (
    <div className={`nbo2${xray ? ' xray' : ''}`}>
      <div className="mockbar">
        <b>NBO Accelerator</b>
        <span style={{ opacity: 0.6 }} className="mono">
          {config ? `${config.catalog}.${config.schema}` : ''}
        </span>
        <div className="tabs">
          {TABS.map((t) => (
            <button
              key={t.path}
              className={`tab${isOn(t.path) ? ' on' : ''}`}
              onClick={() => {
                nav(t.path);
                window.scrollTo(0, 0);
              }}
            >
              {t.label}
            </button>
          ))}
        </div>
        <div className="spacer" />
        <span style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
          Databricks layer{' '}
          <button className={`sw${xray ? ' on' : ''}`} aria-label="Toggle Databricks layer" onClick={() => setXray((v) => !v)} />
        </span>
      </div>
      {offIn !== null && offIn <= WARN_S && (
        <div
          role="alert"
          style={{
            display: 'flex', alignItems: 'center', justifyContent: 'center', gap: 12, padding: '8px 16px',
            background: '#fff7e6', borderBottom: '1px solid #f0d9a8', fontSize: 13, color: '#5c4400',
          }}
        >
          <span>
            No activity for a while. The live pipelines turn off in {Math.max(1, Math.ceil(offIn / 60))} min to save cost.
          </span>
          <button
            onClick={keepOn}
            style={{ padding: '4px 12px', borderRadius: 8, border: '1px solid #c99a2e', background: 'white', fontWeight: 600, cursor: 'pointer' }}
          >
            Keep on
          </button>
        </div>
      )}
      {/* The bank site stays mounted so the visitor's session (sign-in, answers, ranks) survives tab switches. */}
      <div style={{ display: onBank ? 'block' : 'none' }}>
        <BankView config={config} />
      </div>
      <Outlet context={ctx} />
    </div>
  );
}

const router = createBrowserRouter([
  {
    path: '/',
    element: <Shell />,
    children: [
      { index: true, element: null },
      { path: 'dashboard', element: <DashboardView /> },
      { path: 'how', element: <HowView /> },
      { path: '*', element: null },
    ],
  },
]);

export default function App() {
  return <RouterProvider router={router} />;
}
