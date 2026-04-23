"use client";

import { useCallback, useEffect, useMemo, useState } from "react";

const TABS: { key: string; label: string }[] = [
  { key: "pb", label: "pb" },
  { key: "fat_sazonalidade", label: "Sazonalidade" },
  { key: "fat_dia_semana", label: "Dia da semana" },
  { key: "fat_eventos", label: "Eventos" },
  { key: "fat_antecedencia", label: "Antecedência" },
  { key: "fat_ajuste_portfolio", label: "Ajuste portfólio" },
  { key: "fat_ajuste_individual", label: "Ajuste individual" },
  { key: "pi", label: "pi" },
  { key: "d", label: "d (final)" },
  { key: "ocupacao_portfolio", label: "Ocupação real" },
  { key: "expectativa_portfolio", label: "Ocupação esperada" },
];

type Matrix = {
  table: string;
  data_referencia: string;
  data_inicio: string;
  data_fim: string;
  format: "currency" | "percent";
  row_type: "unidade" | "portfolio";
  columns: string[];
  rows: { id: number; label: string; values: (number | null)[] }[];
  total_rows: number;
  page: number;
  page_size: number;
  min: number;
  max: number;
};

const MONTHS_PT = ["jan", "fev", "mar", "abr", "mai", "jun", "jul", "ago", "set", "out", "nov", "dez"];

function formatDateHeader(iso: string): string {
  const [y, m, d] = iso.split("-").map(Number);
  return `${String(d).padStart(2, "0")}/${MONTHS_PT[m - 1]}`;
}

function isWeekend(iso: string): boolean {
  const [y, m, d] = iso.split("-").map(Number);
  const dow = new Date(Date.UTC(y, m - 1, d)).getUTCDay(); // 0=Sun, 6=Sat
  return dow === 0 || dow === 6;
}

function formatValue(v: number | null, fmt: "currency" | "percent"): string {
  if (v === null || v === undefined) return "";
  if (fmt === "currency") {
    return v.toLocaleString("pt-BR", {
      style: "currency",
      currency: "BRL",
      maximumFractionDigits: 0,
    });
  }
  // percent (stored as decimal)
  const pct = v * 100;
  const sign = pct > 0 ? "+" : "";
  return `${sign}${pct.toFixed(1)}%`;
}

function cellColor(
  v: number | null,
  vmin: number,
  vmax: number,
  fmt: "currency" | "percent"
): string {
  if (v === null || v === undefined) return "#f8fafc";
  // Diverging scale quando o range atravessa zero (típico de fat_* com neg e pos)
  if (vmin < 0 && vmax > 0) {
    if (v === 0) return "#ffffff";
    if (v > 0) {
      const t = Math.min(1, v / vmax);
      // verde suave
      const r = Math.round(220 - (220 - 22) * t);
      const g = Math.round(252 - (252 - 163) * t);
      const b = Math.round(231 - (231 - 74) * t);
      return `rgb(${r},${g},${b})`;
    } else {
      const t = Math.min(1, v / vmin);
      // vermelho suave
      const r = Math.round(254 - (254 - 220) * t);
      const g = Math.round(226 - (226 - 38) * t);
      const b = Math.round(226 - (226 - 38) * t);
      return `rgb(${r},${g},${b})`;
    }
  }
  // Gradiente simples (todos positivos ou todos negativos)
  const denom = vmax - vmin;
  const t = denom === 0 ? 0 : (v - vmin) / denom;
  // azul para currency, verde para percent-positivo
  if (fmt === "currency") {
    const r = Math.round(239 - (239 - 29) * t);
    const g = Math.round(246 - (246 - 78) * t);
    const b = Math.round(255 - (255 - 216) * t);
    return `rgb(${r},${g},${b})`;
  }
  // percent todos positivos (ex: ocupação)
  const r = Math.round(240 - (240 - 22) * t);
  const g = Math.round(253 - (253 - 163) * t);
  const b = Math.round(244 - (244 - 74) * t);
  return `rgb(${r},${g},${b})`;
}

function textColor(bg: string): string {
  // Escolhe preto ou branco conforme luminância simples do fundo
  const m = bg.match(/rgb\((\d+),(\d+),(\d+)\)/);
  if (!m) return "#0f172a";
  const r = Number(m[1]);
  const g = Number(m[2]);
  const b = Number(m[3]);
  const lum = 0.299 * r + 0.587 * g + 0.114 * b;
  return lum > 155 ? "#0f172a" : "#ffffff";
}

function addDaysISO(iso: string, days: number): string {
  const [y, m, d] = iso.split("-").map(Number);
  const dt = new Date(Date.UTC(y, m - 1, d));
  dt.setUTCDate(dt.getUTCDate() + days);
  return dt.toISOString().slice(0, 10);
}

export default function DashboardsPage() {
  const [dataRefs, setDataRefs] = useState<string[]>([]);
  const [dataRef, setDataRef] = useState<string>("");
  const [dataInicio, setDataInicio] = useState<string>("");
  const [dataFim, setDataFim] = useState<string>("");
  const [activeTab, setActiveTab] = useState<string>(TABS[0].key);
  const [pageSize, setPageSize] = useState<number>(25);
  const [page, setPage] = useState<number>(1);
  const [matrix, setMatrix] = useState<Matrix | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // Inicialização: busca data_referencias disponíveis, seta default
  useEffect(() => {
    fetch("/api/simulador/data-referencias")
      .then((r) => r.json())
      .then((d) => {
        const vals: string[] = d.values ?? [];
        setDataRefs(vals);
        if (vals.length > 0) {
          setDataRef(vals[0]);
          setDataInicio(vals[0]);
          setDataFim(addDaysISO(vals[0], 30));
        }
      })
      .catch((e) => setError(String(e)));
  }, []);

  // Reset page quando mudam filtros ou aba
  useEffect(() => {
    setPage(1);
  }, [dataRef, dataInicio, dataFim, activeTab, pageSize]);

  // Busca matriz
  useEffect(() => {
    if (!dataRef || !dataInicio || !dataFim) return;
    setLoading(true);
    setError(null);
    const qs = new URLSearchParams({
      data_referencia: dataRef,
      data_inicio: dataInicio,
      data_fim: dataFim,
      page: String(page),
      page_size: String(pageSize),
    });
    fetch(`/api/simulador/matrix/${activeTab}?${qs}`)
      .then(async (r) => {
        if (!r.ok) throw new Error((await r.json()).detail ?? `HTTP ${r.status}`);
        return r.json();
      })
      .then((d: Matrix) => setMatrix(d))
      .catch((e) => {
        setError(String(e));
        setMatrix(null);
      })
      .finally(() => setLoading(false));
  }, [activeTab, dataRef, dataInicio, dataFim, page, pageSize]);

  const totalPages = matrix ? Math.max(1, Math.ceil(matrix.total_rows / pageSize)) : 1;

  return (
    <main style={{ height: "100%", display: "flex", flexDirection: "column", overflow: "hidden" }}>
      {/* Filtros */}
      <div
        style={{
          display: "flex",
          alignItems: "center",
          gap: 16,
          padding: "10px 16px",
          background: "#f8fafc",
          borderBottom: "1px solid #e2e8f0",
          flex: "0 0 auto",
        }}
      >
        <LabeledInput label="Data referência">
          <select
            value={dataRef}
            onChange={(e) => setDataRef(e.target.value)}
            style={sel}
          >
            {dataRefs.map((v) => (
              <option key={v} value={v}>
                {v}
              </option>
            ))}
          </select>
        </LabeledInput>
        <LabeledInput label="De">
          <input type="date" value={dataInicio} onChange={(e) => setDataInicio(e.target.value)} style={inp} />
        </LabeledInput>
        <LabeledInput label="Até">
          <input type="date" value={dataFim} onChange={(e) => setDataFim(e.target.value)} style={inp} />
        </LabeledInput>
        <LabeledInput label="Linhas por página">
          <select value={pageSize} onChange={(e) => setPageSize(Number(e.target.value))} style={sel}>
            {[10, 25, 50, 100].map((n) => (
              <option key={n} value={n}>
                {n}
              </option>
            ))}
          </select>
        </LabeledInput>
        {loading && <span style={{ color: "#64748b", fontSize: 12 }}>carregando…</span>}
        {error && <span style={{ color: "#dc2626", fontSize: 12 }}>{error}</span>}
      </div>

      {/* Abas */}
      <div
        style={{
          display: "flex",
          overflowX: "auto",
          borderBottom: "1px solid #e2e8f0",
          background: "#ffffff",
          flex: "0 0 auto",
        }}
      >
        {TABS.map((t) => {
          const active = t.key === activeTab;
          return (
            <button
              key={t.key}
              onClick={() => setActiveTab(t.key)}
              style={{
                padding: "10px 14px",
                background: "transparent",
                border: 0,
                borderBottom: active ? "2px solid #1d4ed8" : "2px solid transparent",
                color: active ? "#1d4ed8" : "#475569",
                fontWeight: active ? 600 : 500,
                fontSize: 13,
                whiteSpace: "nowrap",
                fontFamily: "inherit",
              }}
            >
              {t.label}
            </button>
          );
        })}
      </div>

      {/* Paginação */}
      {matrix && (
        <div
          style={{
            display: "flex",
            alignItems: "center",
            gap: 8,
            padding: "6px 16px",
            background: "#f1f5f9",
            borderBottom: "1px solid #e2e8f0",
            fontSize: 12,
            color: "#475569",
            flex: "0 0 auto",
          }}
        >
          <button onClick={() => setPage(1)} disabled={page === 1} style={btn}>« Primeira</button>
          <button onClick={() => setPage((p) => Math.max(1, p - 1))} disabled={page === 1} style={btn}>‹ Anterior</button>
          <span>Página <strong>{page}</strong> de <strong>{totalPages}</strong></span>
          <button onClick={() => setPage((p) => Math.min(totalPages, p + 1))} disabled={page >= totalPages} style={btn}>Próxima ›</button>
          <button onClick={() => setPage(totalPages)} disabled={page >= totalPages} style={btn}>Última »</button>
          <span style={{ marginLeft: 12 }}>
            {matrix.total_rows} {matrix.row_type === "unidade" ? "unidades" : "portfólios"} · {matrix.columns.length} dias
          </span>
          {matrix.rows.length > 0 && (
            <span style={{ marginLeft: "auto", color: "#64748b" }}>
              min: {formatValue(matrix.min, matrix.format)} &nbsp; max: {formatValue(matrix.max, matrix.format)}
            </span>
          )}
        </div>
      )}

      {/* Matriz */}
      <div style={{ flex: 1, overflow: "auto", background: "#ffffff" }}>
        {matrix && <MatrixTable matrix={matrix} />}
      </div>
    </main>
  );
}

function MatrixTable({ matrix }: { matrix: Matrix }) {
  if (matrix.rows.length === 0) {
    return <div style={{ padding: 24, color: "#64748b" }}>Sem dados para exibir.</div>;
  }

  const firstColStyle: React.CSSProperties = {
    position: "sticky",
    left: 0,
    background: "#f8fafc",
    padding: "6px 10px",
    borderRight: "1px solid #cbd5e1",
    borderBottom: "1px solid #e2e8f0",
    fontWeight: 500,
    fontSize: 12,
    color: "#0f172a",
    whiteSpace: "nowrap",
    zIndex: 2,
  };

  return (
    <table style={{ borderCollapse: "collapse", fontSize: 11.5 }}>
      <thead>
        <tr>
          <th style={{ ...firstColStyle, position: "sticky", top: 0, left: 0, zIndex: 3, color: "#1d4ed8", fontWeight: 600 }}>
            {matrix.row_type === "unidade" ? "unidade" : "portfólio"}
          </th>
          {matrix.columns.map((c) => {
            const wknd = isWeekend(c);
            return (
              <th
                key={c}
                style={{
                  position: "sticky",
                  top: 0,
                  background: wknd ? "#e0e7ff" : "#f1f5f9",
                  padding: "6px 8px",
                  borderBottom: "1px solid #cbd5e1",
                  color: "#1d4ed8",
                  fontWeight: 600,
                  whiteSpace: "nowrap",
                  minWidth: 64,
                  zIndex: 1,
                }}
              >
                {formatDateHeader(c)}
              </th>
            );
          })}
        </tr>
      </thead>
      <tbody>
        {matrix.rows.map((row) => (
          <tr key={row.id}>
            <th style={firstColStyle}>{row.label}</th>
            {row.values.map((v, i) => {
              const bg = cellColor(v, matrix.min, matrix.max, matrix.format);
              return (
                <td
                  key={i}
                  style={{
                    background: bg,
                    color: v === null ? "#cbd5e1" : textColor(bg),
                    padding: "4px 8px",
                    borderBottom: "1px solid #f1f5f9",
                    borderRight: "1px solid #f8fafc",
                    textAlign: "right",
                    whiteSpace: "nowrap",
                    minWidth: 64,
                    fontVariantNumeric: "tabular-nums",
                  }}
                  title={v === null ? "sem dado" : formatValue(v, matrix.format)}
                >
                  {v === null ? "—" : formatValue(v, matrix.format)}
                </td>
              );
            })}
          </tr>
        ))}
      </tbody>
    </table>
  );
}

function LabeledInput({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <label style={{ display: "flex", flexDirection: "column", gap: 2, fontSize: 11, color: "#64748b" }}>
      <span>{label}</span>
      {children}
    </label>
  );
}

const inp: React.CSSProperties = {
  padding: "4px 8px",
  border: "1px solid #cbd5e1",
  borderRadius: 4,
  fontSize: 12,
  fontFamily: "inherit",
};
const sel: React.CSSProperties = { ...inp, paddingRight: 24 };
const btn: React.CSSProperties = {
  background: "#ffffff",
  color: "#1e293b",
  border: "1px solid #cbd5e1",
  borderRadius: 4,
  padding: "3px 10px",
  fontSize: 12,
  fontFamily: "inherit",
};
