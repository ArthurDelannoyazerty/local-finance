import { lazy, Suspense, useEffect, useMemo, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { format, startOfYear } from "date-fns";
import { Navigate, Route, Routes } from "react-router-dom";
import { api } from "./api";
import Layout from "./components/Layout";
import { Loading } from "./components/ui";
import type { DateRangeValue } from "./types";

const Dashboard = lazy(() => import("./pages/Dashboard"));
const Transactions = lazy(() => import("./pages/Transactions"));
const Flows = lazy(() => import("./pages/Flows"));
const Portfolio = lazy(() => import("./pages/Portfolio"));
const Allocation = lazy(() => import("./pages/Allocation"));
const Fire = lazy(() => import("./pages/Fire"));
const DataPage = lazy(() => import("./pages/Data"));

const defaultRange = (): DateRangeValue => ({
  start: format(startOfYear(new Date()), "yyyy-MM-dd"),
  end: format(new Date(), "yyyy-MM-dd"),
});

export default function App() {
  const bounds = useQuery({
    queryKey: ["date-bounds"],
    queryFn: () =>
      api<{ min: string; max: string; today: string }>("/api/meta/date-bounds"),
  });
  const [range, setRange] = useState<DateRangeValue>(() => {
    const saved = localStorage.getItem("local-finance-date-range");
    if (!saved) return defaultRange();
    try {
      return JSON.parse(saved) as DateRangeValue;
    } catch {
      return defaultRange();
    }
  });
  useEffect(() => {
    localStorage.setItem("local-finance-date-range", JSON.stringify(range));
  }, [range]);
  const pageProps = useMemo(() => ({ range }), [range]);

  return (
    <Layout range={range} minDate={bounds.data?.min} onRange={setRange}>
      <Suspense fallback={<Loading label="Ouverture de la page…" />}>
        <Routes>
          <Route path="/" element={<Dashboard {...pageProps} />} />
          <Route
            path="/transactions"
            element={<Transactions {...pageProps} />}
          />
          <Route path="/flux" element={<Flows />} />
          <Route path="/portfolio" element={<Portfolio {...pageProps} />} />
          <Route path="/allocation" element={<Allocation />} />
          <Route path="/fire" element={<Fire />} />
          <Route path="/donnees" element={<DataPage />} />
          <Route path="*" element={<Navigate to="/" replace />} />
        </Routes>
      </Suspense>
    </Layout>
  );
}
