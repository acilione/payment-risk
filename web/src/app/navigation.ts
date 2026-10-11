import {
  ArrowDownLeft,
  LayoutDashboard,
  Play,
  Search,
  ShieldCheck,
  Workflow,
  type LucideIcon,
} from "lucide-react";
export type Page =
  | "Overview"
  | "Showcase"
  | "Transactions"
  | "Investigations"
  | "Risk rules"
  | "Architecture";
export const pages: { name: Page; icon: LucideIcon; description: string }[] = [
  {
    name: "Overview",
    icon: LayoutDashboard,
    description: "Payment totals, risk decisions, and service status.",
  },
  {
    name: "Showcase",
    icon: Play,
    description:
      "Generate customer payments and follow a verified pipeline run.",
  },
  {
    name: "Transactions",
    icon: ArrowDownLeft,
    description:
      "Search payment decisions and review their scores and matched rules.",
  },
  {
    name: "Investigations",
    icon: Search,
    description:
      "Customer histories, recorded evidence, and decisions to review.",
  },
  {
    name: "Risk rules",
    icon: ShieldCheck,
    description: "Default rules, score weights, and decision thresholds.",
  },
  {
    name: "Architecture",
    icon: Workflow,
    description:
      "How payments are processed, stored, and recovered after a restart.",
  },
];
