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
          padding: "0 14px",
          height: NAV_HEIGHT,
          display: "flex",
          alignItems: "center",
          color: active ? "#4f46e5" : "#64748b",
          borderBottom: active ? "2px solid #4f46e5" : "2px solid transparent",
          textDecoration: "none",
          fontWeight: active ? 600 : 500,
          fontSize: 13,
          letterSpacing: -0.1,
          transition: "color 100ms",
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
        background: "#ffffff",
        height: NAV_HEIGHT,
        paddingLeft: 20,
        flex: "0 0 auto",
        boxShadow: "0 1px 0 rgba(15, 23, 42, 0.06)",
        position: "relative",
        zIndex: 10,
      }}
    >
      <span
        style={{
          fontWeight: 700,
          color: "#4338ca",
          marginRight: 28,
          fontSize: 14,
          letterSpacing: -0.2,
          display: "inline-flex",
          alignItems: "center",
          gap: 7,
        }}
      >
        <Diamond />
        Cyclinn
      </span>
      {link("/", "SQL Explorer")}
      {link("/precificacao", "Precificação")}
      {link("/regras", "Regras")}
      {link("/auditoria", "Auditoria")}
    </nav>
  );
}

function Diamond() {
  return (
    <svg width="14" height="14" viewBox="0 0 14 14" fill="none">
      <path
        d="M7 1L13 7L7 13L1 7L7 1Z"
        fill="#4f46e5"
        stroke="#4338ca"
        strokeWidth="1"
        strokeLinejoin="round"
      />
    </svg>
  );
}

export { NAV_HEIGHT };
