export const staticDemo = import.meta.env.VITE_DEMO_MODE === "true";
export const github = "https://github.com/acilione/payment-risk";
export const documentation = `${github}/blob/main/README.md`;
export function openLocalDashboard() {
  globalThis.open("http://localhost:23001", "_blank", "noopener,noreferrer");
}
