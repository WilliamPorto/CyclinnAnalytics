import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "Cyclinn Pricing DB",
  description: "SQL explorer para a base de pricing",
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="pt-BR">
      <body>{children}</body>
    </html>
  );
}
