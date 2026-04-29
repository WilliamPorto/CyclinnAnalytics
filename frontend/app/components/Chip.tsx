"use client";

import { ReactNode, useEffect, useRef, useState } from "react";

export function Chip({
  icon,
  value,
  children,
  align = "right",
  width,
}: {
  icon?: ReactNode;
  value: ReactNode;
  children: (close: () => void) => ReactNode;
  align?: "left" | "right";
  width?: number;
}) {
  const [open, setOpen] = useState(false);
  const wrapperRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (!open) return;
    const onClick = (e: MouseEvent) => {
      if (!wrapperRef.current?.contains(e.target as Node)) setOpen(false);
    };
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") setOpen(false);
    };
    document.addEventListener("mousedown", onClick);
    document.addEventListener("keydown", onKey);
    return () => {
      document.removeEventListener("mousedown", onClick);
      document.removeEventListener("keydown", onKey);
    };
  }, [open]);

  return (
    <div ref={wrapperRef} style={{ position: "relative" }}>
      <button
        onClick={() => setOpen((o) => !o)}
        style={{
          display: "inline-flex",
          alignItems: "center",
          gap: 6,
          height: 28,
          padding: "0 10px",
          background: open ? "#eef2ff" : "#ffffff",
          border: `1px solid ${open ? "#c7d2fe" : "#e2e8f0"}`,
          borderRadius: 6,
          color: "#1e293b",
          fontSize: 12,
          fontWeight: 500,
          fontFamily: "inherit",
          cursor: "pointer",
          transition: "background 80ms, border-color 80ms",
        }}
        onMouseEnter={(e) => {
          if (!open) e.currentTarget.style.background = "#f8fafc";
        }}
        onMouseLeave={(e) => {
          if (!open) e.currentTarget.style.background = "#ffffff";
        }}
      >
        {icon && <span style={{ color: "#64748b", display: "inline-flex" }}>{icon}</span>}
        <span>{value}</span>
        <Caret />
      </button>

      {open && (
        <div
          style={{
            position: "absolute",
            top: "calc(100% + 4px)",
            [align]: 0,
            zIndex: 50,
            minWidth: width ?? 220,
            background: "#ffffff",
            border: "1px solid #e2e8f0",
            borderRadius: 8,
            boxShadow:
              "0 4px 6px -1px rgba(15,23,42,0.08), 0 2px 4px -2px rgba(15,23,42,0.06)",
            padding: 12,
          }}
        >
          {children(() => setOpen(false))}
        </div>
      )}
    </div>
  );
}

function Caret() {
  return (
    <svg
      width="10"
      height="10"
      viewBox="0 0 12 12"
      fill="none"
      style={{ color: "#94a3b8", marginLeft: 2 }}
    >
      <path
        d="M3 4.5L6 7.5L9 4.5"
        stroke="currentColor"
        strokeWidth="1.5"
        strokeLinecap="round"
        strokeLinejoin="round"
      />
    </svg>
  );
}
