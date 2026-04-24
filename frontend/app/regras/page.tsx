"use client";

import { useState } from "react";
import SazonalidadeTab from "./sazonalidade";
import DiaSemanaTab from "./dia_semana";

const SUB_TABS: { key: string; label: string; enabled: boolean }[] = [
  { key: "sazonalidade", label: "Sazonalidade", enabled: true },
  { key: "dia_semana", label: "Dia da semana", enabled: true },
  { key: "eventos", label: "Eventos", enabled: false },
  { key: "antecedencia", label: "Antecedência", enabled: false },
];

export default function RegrasPage() {
  const [activeTab, setActiveTab] = useState<string>("sazonalidade");

  return (
    <main style={{ height: "100%", display: "flex", flexDirection: "column", overflow: "hidden" }}>
      <div
        style={{
          display: "flex",
          borderBottom: "1px solid #e2e8f0",
          background: "#ffffff",
          flex: "0 0 auto",
        }}
      >
        {SUB_TABS.map((t) => {
          const active = t.key === activeTab;
          return (
            <button
              key={t.key}
              onClick={() => t.enabled && setActiveTab(t.key)}
              disabled={!t.enabled}
              title={!t.enabled ? "em breve" : undefined}
              style={{
                padding: "10px 16px",
                background: "transparent",
                border: 0,
                borderBottom: active ? "2px solid #1d4ed8" : "2px solid transparent",
                color: !t.enabled ? "#cbd5e1" : active ? "#1d4ed8" : "#475569",
                fontWeight: active ? 600 : 500,
                fontSize: 13,
                whiteSpace: "nowrap",
                fontFamily: "inherit",
                cursor: t.enabled ? "pointer" : "not-allowed",
              }}
            >
              {t.label}
              {!t.enabled && <span style={{ fontSize: 10, marginLeft: 6 }}>(em breve)</span>}
            </button>
          );
        })}
      </div>

      <div style={{ flex: 1, overflow: "hidden" }}>
        {activeTab === "sazonalidade" && <SazonalidadeTab />}
        {activeTab === "dia_semana" && <DiaSemanaTab />}
      </div>
    </main>
  );
}
