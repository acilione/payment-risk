import { ChevronRight, CircleHelp, Menu } from "lucide-react";
import type { Page } from "../app/navigation";

interface TopBarProps {
  activePage: Page;
  navigationOpen: boolean;
  onToggleNavigation: () => void;
  onAbout: () => void;
  demo: boolean;
  error: string;
  loading: boolean;
  hasData: boolean;
}
export function TopBar({
  activePage,
  navigationOpen,
  onToggleNavigation,
  onAbout,
  demo,
  error,
  loading,
  hasData,
}: TopBarProps) {
  return (
    <header className="topbar">
      <div className="breadcrumbs">
        <button
          className="mobile-menu icon-button"
          onClick={onToggleNavigation}
          aria-label="Toggle navigation"
          aria-expanded={navigationOpen}
          aria-controls="sidebar"
        >
          <Menu size={20} />
        </button>
        <span>Dashboard</span>
        <ChevronRight size={13} />
        <strong>{activePage}</strong>
      </div>
      <div className="topbar-right">
        <span className={`connection ${error ? "offline" : ""}`}>
          <i />
          {demo
            ? "Demo data"
            : error
              ? "Connection unavailable"
              : loading && !hasData
                ? "Connecting"
                : "Live data"}
        </span>
        <span className="topbar-divider" />
        <button
          className="icon-button"
          onClick={onAbout}
          aria-label="About this project"
        >
          <CircleHelp size={18} />
        </button>
        <span className="avatar avatar-small">AC</span>
      </div>
    </header>
  );
}
