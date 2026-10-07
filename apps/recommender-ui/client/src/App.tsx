import { useEffect, useState } from 'react';
import { createBrowserRouter, RouterProvider, Outlet, useLocation, useNavigate } from 'react-router';
import './bank.css';
import { api, type AppConfig } from '@/lib/api';
import { BankView } from '@/views/BankView';
import { DashboardView } from '@/views/DashboardView';
import { HowView } from '@/views/HowView';

const TABS: { path: string; label: string }[] = [
  { path: '/', label: '1 · Lakeshore Bank (customer view)' },
  { path: '/dashboard', label: '2 · Scale dashboard' },
  { path: '/how', label: '3 · How it works' },
];

export interface ShellContext {
  config: AppConfig | null;
}

function Shell() {
  const loc = useLocation();
  const nav = useNavigate();
  const [xray, setXray] = useState(false);
  const [config, setConfig] = useState<AppConfig | null>(null);

  useEffect(() => {
    api.config().then(setConfig).catch(() => setConfig(null));
  }, []);

  const onDash = loc.pathname.startsWith('/dashboard');
  const onHow = loc.pathname.startsWith('/how');
  const onBank = !onDash && !onHow;
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
              className={`tab${(t.path === '/' ? onBank : t.path === '/dashboard' ? onDash : onHow) ? ' on' : ''}`}
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
