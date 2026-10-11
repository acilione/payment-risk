import {
  Activity,
  ArrowUpRight,
  CircleHelp,
  Code2,
  GitFork,
  X,
} from "lucide-react";
import { documentation, github } from "../app/config";
import { pages, type Page } from "../app/navigation";
import { useMediaQuery } from "../hooks/useMediaQuery";

interface SidebarProps {
  activePage: Page;
  isOpen: boolean;
  onClose: () => void;
  onNavigate: (page: Page) => void;
  onAbout: () => void;
}
export function Sidebar({
  activePage,
  isOpen,
  onClose,
  onNavigate,
  onAbout,
}: SidebarProps) {
  const isMobile = useMediaQuery("(max-width: 760px)");
  return (
    <aside
      id="sidebar"
      className={`sidebar ${isOpen ? "open" : ""}`}
      inert={isMobile && !isOpen}
    >
      {isMobile && isOpen && (
        <button
          className="sidebar-close icon-button"
          onClick={onClose}
          aria-label="Close navigation"
        >
          <X size={18} />
        </button>
      )}
      <a
        className="brand"
        href="#"
        onClick={(e) => {
          e.preventDefault();
          onNavigate("Overview");
        }}
        aria-label="Payment risk home"
      >
        <span className="brand-mark">
          <Activity size={25} />
        </span>
        <span>Payment risk</span>
      </a>
      <div className="nav-label">DASHBOARD</div>
      <nav aria-label="Main navigation">
        {pages.map(({ icon: Icon, name }) => {
          return (
            <button
              key={name}
              className={`nav-item ${activePage === name ? "active" : ""}`}
              aria-current={activePage === name ? "page" : undefined}
              onClick={() => onNavigate(name)}
            >
              <Icon size={18} />
              {name}
              {name === "Risk rules" && <span className="nav-count">05</span>}
            </button>
          );
        })}
      </nav>
      <div className="nav-label nav-resources">RESOURCES</div>
      <a
        className="nav-item"
        href={documentation}
        target="_blank"
        rel="noreferrer"
      >
        <Code2 size={18} />
        Documentation
        <ArrowUpRight size={14} className="nav-out" />
      </a>
      <a className="nav-item" href={github} target="_blank" rel="noreferrer">
        <GitFork size={18} />
        Source code
        <ArrowUpRight size={14} className="nav-out" />
      </a>
      <div className="sidebar-bottom">
        <button className="profile" onClick={onAbout}>
          <span className="avatar">AC</span>
          <span>
            Antonino Cilione<small>Project maintainer</small>
          </span>
          <CircleHelp size={16} />
        </button>
      </div>
    </aside>
  );
}
