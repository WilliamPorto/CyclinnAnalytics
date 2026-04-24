"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";

const NAV_HEIGHT = 44;

export function NavBar() {
  const pathname = usePathname() ?? "/";

  const link = (href: string, label: string) => {
    const active = href === "/" ? pathname === "/" : pathname.startsWith(href);
    return (
      <Link
        href={href}
        style={{
          padding: "0 16px",
          height: NAV_HEIGHT,
          display: "flex",
          alignItems: "center",
          color: active ? "#1d4ed8" : "#475569",
          borderBottom: active ? "2px solid #1d4ed8" : "2px solid transparent",
          textDecoration: "none",
          fontWeight: active ? 600 : 500,
          fontSize: 13,
        }}
      >
        {label}
      </Link>
    );
  };

  return (
    <nav
      style={{
        display: "flex",
        alignItems: "center",
        borderBottom: "1px solid #e2e8f0",
        background: "#f8fafc",
        height: NAV_HEIGHT,
        paddingLeft: 16,
        flex: "0 0 auto",
      }}
    >
      <span
        style={{
          fontWeight: 700,
          color: "#1d4ed8",
          marginRight: 24,
          fontSize: 13,
          letterSpacing: 0.3,
        }}
      >
        Cyclinn Pricing
      </span>
      {link("/", "SQL Explorer")}
      {link("/dashboards", "Dashboards")}
      {link("/regras", "Regras")}
    </nav>
  );
}

export { NAV_HEIGHT };
