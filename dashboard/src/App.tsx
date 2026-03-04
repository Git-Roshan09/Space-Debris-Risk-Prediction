import { BrowserRouter, Routes, Route } from 'react-router-dom';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import AppShell from './components/layout/AppShell';
import CommandCenter from './pages/CommandCenter';
import CollisionAlerts from './pages/CollisionAlerts';
import DebrisObjects from './pages/DebrisObjects';
import Analytics from './pages/Analytics';
import SystemStatus from './pages/SystemStatus';
import GlobePage from './pages/GlobePage';

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      retry: 2,
      refetchOnWindowFocus: false,
      staleTime: 10000,
    },
  },
});

export default function App() {
  return (
    <QueryClientProvider client={queryClient}>
      <BrowserRouter>
        <Routes>
          <Route element={<AppShell />}>
            <Route path="/"            element={<CommandCenter />} />
            <Route path="/collisions"  element={<CollisionAlerts />} />
            <Route path="/globe"       element={<GlobePage />} />
            <Route path="/debris"      element={<DebrisObjects />} />
            <Route path="/analytics"   element={<Analytics />} />
            <Route path="/system"      element={<SystemStatus />} />
          </Route>
        </Routes>
      </BrowserRouter>
    </QueryClientProvider>
  );
}
