import { useState, type ReactNode } from "react";
import {
  ArrowLeftRight,
  BarChart3,
  Database,
  Flame,
  LayoutDashboard,
  Menu,
  PieChart,
  Search,
  TrendingUp,
  WalletCards,
  X,
} from "lucide-react";
import { NavLink, useLocation } from "react-router-dom";
import DateRangePicker from "./DateRangePicker";
import type { DateRangeValue } from "../types";

const groups = [
  {
    label: "Quotidien",
    items: [
      { to: "/", label: "Vue d’ensemble", icon: LayoutDashboard, end: true },
      { to: "/transactions", label: "Transactions", icon: Search },
      { to: "/flux", label: "Flux", icon: ArrowLeftRight },
    ],
  },
  {
    label: "Patrimoine",
    items: [
      { to: "/portfolio", label: "Portfolio", icon: TrendingUp },
      { to: "/allocation", label: "Allocation", icon: PieChart },
      { to: "/fire", label: "Projections FIRE", icon: Flame },
    ],
  },
  {
    label: "Système",
    items: [{ to: "/donnees", label: "Données & comptes", icon: Database }],
  },
];

const rangeRoutes = new Set(["/", "/transactions", "/portfolio"]);

export default function Layout({
  children,
  range,
  minDate,
  onRange,
}: {
  children: ReactNode;
  range: DateRangeValue;
  minDate?: string;
  onRange: (range: DateRangeValue) => void;
}) {
  const [open, setOpen] = useState(false);
  const location = useLocation();
  return (
    <div className="app-shell">
      <aside className={open ? "sidebar sidebar-open" : "sidebar"}>
        <div className="brand-row">
          <NavLink to="/" className="brand" onClick={() => setOpen(false)}>
            <span className="brand-mark">
              <WalletCards size={21} />
            </span>
            <span>
              <strong>Local</strong> Finance
            </span>
          </NavLink>
          <button
            className="icon-button sidebar-close"
            onClick={() => setOpen(false)}
            aria-label="Fermer le menu"
          >
            <X size={19} />
          </button>
        </div>
        <nav>
          {groups.map((group) => (
            <div className="nav-group" key={group.label}>
              <span>{group.label}</span>
              {group.items.map((item) => (
                <NavLink
                  key={item.to}
                  to={item.to}
                  end={item.end}
                  onClick={() => setOpen(false)}
                  className={({ isActive }) =>
                    isActive ? "nav-item nav-active" : "nav-item"
                  }
                >
                  <item.icon size={18} />
                  {item.label}
                </NavLink>
              ))}
            </div>
          ))}
        </nav>
        <div className="sidebar-foot">
          <span className="status-dot" />
          Données locales · utilisateur unique
        </div>
      </aside>
      {open && (
        <button
          className="sidebar-scrim"
          onClick={() => setOpen(false)}
          aria-label="Fermer le menu"
        />
      )}
      <div className="app-main">
        <div className="topbar">
          <button
            className="icon-button menu-button"
            onClick={() => setOpen(true)}
            aria-label="Ouvrir le menu"
          >
            <Menu size={20} />
          </button>
          <div className="topbar-title">
            <BarChart3 size={17} />
            <span>Finance personnelle</span>
          </div>
          {rangeRoutes.has(location.pathname) && (
            <DateRangePicker value={range} min={minDate} onChange={onRange} />
          )}
        </div>
        <main className="content">{children}</main>
      </div>
    </div>
  );
}
