import { useEffect, useState } from 'react';
import { createBrowserRouter, RouterProvider, Outlet } from 'react-router';
import { Sidebar } from '@/shell/Sidebar';
import { RecommenderView } from '@/views/RecommenderView';
import { ArchitectureView } from '@/views/ArchitectureView';

function Shell() {
  // Catalog/schema are read from the server (env-driven), never hardcoded, so the header
  // reflects whatever schema this deployment was pointed at.
  const [cfg, setCfg] = useState<{ catalog: string; schema: string } | null>(null);
  useEffect(() => {
    fetch('/api/config')
      .then((r) => r.json())
      .then((d) => setCfg({ catalog: d.catalog, schema: d.schema }))
      .catch(() => setCfg(null));
  }, []);

  return (
    <div className="flex h-screen overflow-hidden bg-[var(--nbo-canvas)]">
      <Sidebar />
      <div className="flex min-w-0 flex-1 flex-col">
        <header className="flex items-center justify-between border-b border-[var(--nbo-line)] bg-white/60 px-6 py-2.5 backdrop-blur-sm">
          <div className="flex items-center gap-2 text-[12px] text-neutral-500">
            <span className="font-medium text-neutral-700">{cfg?.catalog ?? 'Unity Catalog'}</span>
            <span className="text-neutral-300">/</span>
            <span>{cfg?.schema ?? '…'}</span>
          </div>
          <span className="rounded-full bg-white px-3 py-1 text-[11px] font-medium text-neutral-600 shadow-sm ring-1 ring-[var(--nbo-line)]">
            Feature Views · Model Serving
          </span>
        </header>
        <div className="min-h-0 flex-1 overflow-auto">
          <Outlet />
        </div>
      </div>
    </div>
  );
}

const router = createBrowserRouter([
  {
    path: '/',
    element: <Shell />,
    children: [
      { index: true, element: <RecommenderView /> },
      { path: 'architecture', element: <ArchitectureView /> },
    ],
  },
]);

export default function App() {
  return <RouterProvider router={router} />;
}
