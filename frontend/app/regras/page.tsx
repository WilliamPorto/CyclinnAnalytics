"use client";

import { useState } from "react";
import SazonalidadeTab from "./sazonalidade";
import DiaSemanaTab from "./dia_semana";
import EventosTab from "./eventos";
import AntecedenciaTab from "./antecedencia";
import OcupacaoTab from "./ocupacao";

const SUB_TABS: { key: string; label: string; enabled: boolean }[] = [
  { key: "sazonalidade", label: "Sazonalidade", enabled: true },
  { key: "dia_semana", label: "Dia da semana", enabled: true },
  { key: "eventos", label: "Eventos", enabled: true },
  { key: "antecedencia", label: "Antecedência", enabled: true },
  { key: "ocupacao", label: "Ocupação", enabled: true },
];

export default function RegrasPage() {
  const [activeTab, setActiveTab] = useState<string>("sazonalidade");

  return (
    <main style={{ height: "100%", display: "flex", flexDirection: "column", overflow: "hidden" }}>
      <div
        style={{
          display: "flex",
          alignItems: "center",
          gap: 2,
          padding: "4px 16px",
          height: 44,
          background: "#ffffff",
          flex: "0 0 auto",
          boxShadow: "0 1px 0 rgba(15,23,42,0.06)",
          position: "relative",
          zIndex: 5,
        }}
      >
        {SUB_TABS.map((t) => {
          const active = t.key === activeTab;
          const disabled = !t.enabled;
          return (
            <button
              key={t.key}
              onClick={() => t.enabled && setActiveTab(t.key)}
              disabled={disabled}
              title={disabled ? "em breve" : undefined}
              style={{
                padding: "6px 14px",
                background: active ? "#eef2ff" : "transparent",
                border: 0,
                borderRadius: 7,
                color: disabled ? "#cbd5e1" : active ? "#4338ca" : "#64748b",
                fontWeight: active ? 600 : 500,
                fontSize: 13,
                whiteSpace: "nowrap",
                fontFamily: "inherit",
                letterSpacing: -0.1,
                transition: "background 100ms, color 100ms",
                cursor: disabled ? "not-allowed" : "pointer",
                outline: "none",
              }}
              onMouseEnter={(e) => {
                if (!active && !disabled) {
                  e.currentTarget.style.color = "#334155";
                  e.currentTarget.style.background = "#f8fafc";
                }
              }}
              onMouseLeave={(e) => {
                if (!active && !disabled) {
                  e.currentTarget.style.color = "#64748b";
                  e.currentTarget.style.background = "transparent";
                }
              }}
            >
              {t.label}
              {disabled && <span style={{ fontSize: 10, marginLeft: 6 }}>(em breve)</span>}
            </button>
          );
        })}
      </div>

      <div style={{ flex: 1, overflow: "hidden" }}>
        {activeTab === "sazonalidade" && <SazonalidadeTab />}
        {activeTab === "dia_semana" && <DiaSemanaTab />}
        {activeTab === "eventos" && <EventosTab />}
        {activeTab === "antecedencia" && <AntecedenciaTab />}
        {activeTab === "ocupacao" && <OcupacaoTab />}
      </div>
    </main>
  );
}
