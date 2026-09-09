import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
export default defineConfig({
  plugins: [react()],
  base: process.env.BASE_PATH || "/",
  server: {
    host: "127.0.0.1",
    port: 23001,
    proxy: { "/api": "http://127.0.0.1:23002" },
  },
  build: { sourcemap: false },
});
