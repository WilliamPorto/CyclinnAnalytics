"use client";

import { useCallback, useEffect, useMemo, useState } from "react";
import { apiFetch } from "../lib/api";

const TABS: { key: string; label: string }[] = [
  { key: "pb", label: "Preço Base" },
  { key: "pi", label: "Preço Inicial" },
  { key: "expectativa_portfolio", label: "Ocupação esperada" },
  { key: "ocupacao_portfolio", label: "Ocupação real" },
  { key: "d", label: "Preço Final" },
];

// Abas que só fazem sentido na view "Por portfólio"
const PORTFOLIO_ONLY_TABS = new Set([
  "expectativa_portfolio",
  "ocupacao_portfolio",
]);

// Label contextual do "delta" mostrado no tooltip quando há color_values
const COLOR_DELTA_LABEL: Record<string, string> = {
  pi: "vs Pb",
  d: "vs Pb",
  ocupacao_portfolio: "vs esperada",
};

type Matrix = {
  table: string;
  data_referencia: string;
  data_inicio: string;
  data_fim: string;
  format: "currency" | "percent";
  row_type: "unidade" | "portfolio";
  columns: string[];
  rows: {
    id: number;
    label: string;
    values: (number | null)[];
    color_values?: (number | null)[];
  }[];
  total_rows: number;
  page: number;
  page_size: number;
  min: number;
  max: number;
  color_min?: number | null;
  color_max?: number | null;
  color_format?: "currency" | "percent" | null;
  day_totals?: (number | null)[] | null;
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

function formatValue(
  v: number | null,
  fmt: "currency" | "percent",
  opts: { signed?: boolean } = { signed: true }
): string {
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
  const sign = opts.signed && pct > 0 ? "+" : "";
  return `${sign}${pct.toFixed(1)}%`;
}

// Tabelas de ocupação (sempre positivo): mostra % sem sinal "+"
const TABLES_UNSIGNED_PCT = new Set([
  "ocupacao_portfolio",
  "expectativa_portfolio",
]);

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
  const [view, setView] = useState<"unidade" | "portfolio">("unidade");
  const [matrix, setMatrix] = useState<Matrix | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [refreshKey, setRefreshKey] = useState(0);

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

  // Reset page quando mudam filtros, aba ou view
  useEffect(() => {
    setPage(1);
  }, [dataRef, dataInicio, dataFim, activeTab, pageSize, view]);

  // Força view=portfolio nas abas que só fazem sentido nessa granularidade
  useEffect(() => {
    if (PORTFOLIO_ONLY_TABS.has(activeTab) && view !== "portfolio") {
      setView("portfolio");
    }
  }, [activeTab, view]);

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
      view,
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
  }, [activeTab, dataRef, dataInicio, dataFim, page, pageSize, view, refreshKey]);

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
        <LabeledInput label="Visão">
          <div
            style={{
              display: "inline-flex",
              border: "1px solid #cbd5e1",
              borderRadius: 6,
              overflow: "hidden",
              background: "#ffffff",
            }}
          >
            {(["unidade", "portfolio"] as const).map((v) => {
              const active = view === v;
              const disabled =
                v === "unidade" && PORTFOLIO_ONLY_TABS.has(activeTab);
              return (
                <button
                  key={v}
                  onClick={() => !disabled && setView(v)}
                  disabled={disabled}
                  title={
                    disabled
                      ? "Esta aba só faz sentido na visão por portfólio"
                      : undefined
                  }
                  style={{
                    background: active ? "#1d4ed8" : "transparent",
                    color: disabled
                      ? "#cbd5e1"
                      : active
                      ? "#ffffff"
                      : "#475569",
                    border: 0,
                    padding: "4px 12px",
                    fontSize: 12,
                    fontWeight: active ? 600 : 500,
                    fontFamily: "inherit",
                    cursor: disabled ? "not-allowed" : "pointer",
                  }}
                >
                  {v === "unidade" ? "Por unidade" : "Por portfólio"}
                </button>
              );
            })}
          </div>
        </LabeledInput>
        {loading && <span style={{ color: "#64748b", fontSize: 12 }}>carregando…</span>}
        {error && <span style={{ color: "#dc2626", fontSize: 12 }}>{error}</span>}
        <div style={{ flex: 1 }} />
        {/* TEMPORÁRIO — remover quando não precisar mais testar */}
        <FakeOcupacaoButton onDone={() => setRefreshKey((k) => k + 1)} />
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
              min: {formatValue(matrix.min, matrix.format, { signed: !TABLES_UNSIGNED_PCT.has(matrix.table) })}
              &nbsp; max: {formatValue(matrix.max, matrix.format, { signed: !TABLES_UNSIGNED_PCT.has(matrix.table) })}
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
              const semHeatmap = matrix.table === "pb" || matrix.table === "expectativa_portfolio";
              // Se o backend mandou color_values, usamos esses valores (em %)
              // pra colorir; os valores "values" continuam sendo mostrados na célula.
              const cv = row.color_values?.[i] ?? null;
              const useColorDriver = cv !== null && matrix.color_min != null && matrix.color_max != null;
              const bg = v === null
                ? "#f8fafc"
                : semHeatmap
                ? "#dbeafe"
                : useColorDriver
                ? cellColor(
                    cv!,
                    matrix.color_min!,
                    matrix.color_max!,
                    matrix.color_format ?? "percent"
                  )
                : cellColor(v, matrix.min, matrix.max, matrix.format);
              const deltaLabel = COLOR_DELTA_LABEL[matrix.table] ?? "delta";
              // Em ocupacao_portfolio o próprio valor da célula já mostra a real,
              // então o tooltip traz apenas o gap. Nas demais (ex: pi) mantém valor + delta.
              const tooltip = v === null
                ? "sem dado"
                : useColorDriver && matrix.table === "ocupacao_portfolio"
                ? `${formatValue(cv, "percent")} ${deltaLabel}`
                : useColorDriver
                ? `${formatValue(v, matrix.format)}  ·  ${formatValue(cv, "percent")} ${deltaLabel}`
                : formatValue(v, matrix.format);
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
                  title={tooltip}
                >
                  {v === null
                    ? "—"
                    : formatValue(v, matrix.format, {
                        signed: !TABLES_UNSIGNED_PCT.has(matrix.table),
                      })}
                </td>
              );
            })}
          </tr>
        ))}
      </tbody>
      {matrix.day_totals && (
        <tfoot>
          <tr>
            <th
              style={{
                ...firstColStyle,
                position: "sticky",
                bottom: 0,
                background: "#f1f5f9",
                borderTop: "2px solid #94a3b8",
                fontWeight: 600,
                color: "#0f172a",
                zIndex: 2,
              }}
              title="Soma diária do impacto (Preço Final − Preço Base) em todas as unidades do portfólio"
            >
              Impacto (R$)
            </th>
            {matrix.day_totals.map((t, i) => {
              const color = t === null || t === 0 ? "#475569" : t > 0 ? "#15803d" : "#b91c1c";
              const txt =
                t === null
                  ? "—"
                  : `${t > 0 ? "+" : ""}${t.toLocaleString("pt-BR", {
                      style: "currency",
                      currency: "BRL",
                      maximumFractionDigits: 0,
                    })}`;
              return (
                <td
                  key={i}
                  style={{
                    position: "sticky",
                    bottom: 0,
                    background: "#f1f5f9",
                    borderTop: "2px solid #94a3b8",
                    padding: "4px 8px",
                    textAlign: "right",
                    whiteSpace: "nowrap",
                    fontVariantNumeric: "tabular-nums",
                    fontWeight: 600,
                    color,
                  }}
                >
                  {txt}
                </td>
              );
            })}
          </tr>
        </tfoot>
      )}
    </table>
  );
}

// TEMPORÁRIO — botão de teste que chama POST /regras/fake-ocupacao.
// Remover este componente junto com o endpoint quando não precisar mais.
function FakeOcupacaoButton({ onDone }: { onDone: () => void }) {
  const [state, setState] = useState<"idle" | "running" | "ok" | "error">("idle");
  const [msg, setMsg] = useState("");

  const run = async () => {
    setState("running");
    setMsg("");
    try {
      const r = await apiFetch("/api/regras/fake-ocupacao", { method: "POST" });
      const d = await r.json();
      if (!r.ok) throw new Error(d.detail ?? `HTTP ${r.status}`);
      setState("ok");
      setMsg(`feito em ${d.duration_ms} ms`);
      onDone();
    } catch (e) {
      setState("error");
      setMsg(String(e));
    }
  };

  return (
    <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
      <button
        onClick={run}
        disabled={state === "running"}
        title="Preenche a ocupação real com dados sintéticos próximos da esperada (teste de UI)"
        style={{
          background: "#fef3c7",
          color: "#b45309",
          border: "1px solid #fcd34d",
          borderRadius: 5,
          padding: "4px 10px",
          fontSize: 11,
          fontWeight: 600,
          fontFamily: "inherit",
          cursor: state === "running" ? "not-allowed" : "pointer",
        }}
      >
        🧪 {state === "running" ? "gerando…" : "fake ocupação (teste)"}
      </button>
      {state === "ok" && <span style={{ color: "#15803d", fontSize: 11 }}>✓ {msg}</span>}
      {state === "error" && <span style={{ color: "#dc2626", fontSize: 11 }}>✕ {msg}</span>}
    </div>
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
