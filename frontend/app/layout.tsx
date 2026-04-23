import type { Metadata } from "next";
import { NavBar } from "./components/NavBar";
import "./globals.css";

export const metadata: Metadata = {
  title: "Cyclinn Pricing DB",
  description: "SQL explorer para a base de pricing",
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="pt-BR">
      <body
        style={{
          display: "flex",
          flexDirection: "column",
          height: "100vh",
          margin: 0,
        }}
      >
        <NavBar />
        <div style={{ flex: 1, overflow: "hidden" }}>{children}</div>
      </body>
    </html>
  );
}
