"use client";

import { useEffect, useState } from "react";
import { apiFetch } from "../lib/api";
import { Chip } from "../components/Chip";

const TABS: { key: string; label: string }[] = [
  { key: "pb", label: "Preço Base" },
  { key: "pi", label: "Preço Inicial" },
  { key: "expectativa_regiao", label: "Ocupação esperada" },
  { key: "ocupacao_regiao", label: "Ocupação real" },
  { key: "d", label: "Preço Final" },
];

// Abas que só fazem sentido em views agregadas (região ou prédio),
// já que não existe ocupação por unidade individual no modelo.
const AGGREGATED_ONLY_TABS = new Set([
  "expectativa_regiao",
  "ocupacao_regiao",
]);

// Label contextual do "delta" mostrado no tooltip quando há color_values
const COLOR_DELTA_LABEL: Record<string, string> = {
  pi: "vs Pb",
  d: "vs Pb",
  ocupacao_regiao: "vs esperada",
};

type Matrix = {
  table: string;
  data_referencia: string;
  data_inicio: string;
  data_fim: string;
  format: "currency" | "percent";
  row_type: "unidade" | "regiao" | "predio";
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
  "ocupacao_regiao",
  "expectativa_regiao",
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
  const [view, setView] = useState<"unidade" | "regiao" | "predio">("unidade");
  const [matrix, setMatrix] = useState<Matrix | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [refreshKey, setRefreshKey] = useState(0);
  const [publishOpen, setPublishOpen] = useState(false);
  const [publishResult, setPublishResult] = useState<PublishResult | null>(null);
  const [publishDetailOpen, setPublishDetailOpen] = useState(false);
  const [explainCell, setExplainCell] = useState<{ unidade_id: number; data: string } | null>(null);

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

  // Em abas agregadas (ocupação), se a view atual for "unidade", força "regiao"
  // (mantém "predio" se já estiver, pois agora também é granularidade válida).
  useEffect(() => {
    if (AGGREGATED_ONLY_TABS.has(activeTab) && view === "unidade") {
      setView("regiao");
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

  const viewLabel =
    view === "unidade" ? "Por unidade" : view === "predio" ? "Por prédio" : "Por região";

  return (
    <main style={{ height: "100%", display: "flex", flexDirection: "column", overflow: "hidden" }}>
      {/* Toolbar única: tabs (esquerda) + chips (direita) */}
      <div
        style={{
          display: "flex",
          alignItems: "center",
          gap: 8,
          padding: "0 16px",
          height: 44,
          background: "#ffffff",
          flex: "0 0 auto",
          boxShadow: "0 1px 0 rgba(15,23,42,0.06)",
          position: "relative",
          zIndex: 5,
        }}
      >
        {/* Tabs de matriz */}
        <div style={{ display: "flex", alignItems: "center", gap: 2, padding: "4px 0" }}>
          {TABS.map((t) => {
            const active = t.key === activeTab;
            return (
              <button
                key={t.key}
                onClick={() => setActiveTab(t.key)}
                style={{
                  padding: "6px 14px",
                  background: active ? "#eef2ff" : "transparent",
                  border: 0,
                  borderRadius: 7,
                  color: active ? "#4338ca" : "#64748b",
                  fontWeight: active ? 600 : 500,
                  fontSize: 13,
                  whiteSpace: "nowrap",
                  fontFamily: "inherit",
                  letterSpacing: -0.1,
                  transition: "background 100ms, color 100ms",
                  cursor: "pointer",
                  outline: "none",
                }}
                onMouseEnter={(e) => {
                  if (!active) {
                    e.currentTarget.style.color = "#334155";
                    e.currentTarget.style.background = "#f1f5f9";
                  }
                }}
                onMouseLeave={(e) => {
                  if (!active) {
                    e.currentTarget.style.color = "#64748b";
                    e.currentTarget.style.background = "transparent";
                  }
                }}
              >
                {t.label}
              </button>
            );
          })}
        </div>

        <div style={{ flex: 1 }} />

        {/* Indicadores de status */}
        {loading && (
          <span style={{ color: "#94a3b8", fontSize: 11, fontWeight: 500 }}>carregando…</span>
        )}
        {error && (
          <span style={{ color: "#dc2626", fontSize: 11, fontWeight: 500 }} title={error}>
            erro
          </span>
        )}

        {/* Chips de filtro */}
        <Chip icon={<IconCalendar />} value={formatPeriodChip(dataInicio, dataFim)} width={300}>
          {() => (
            <PeriodPopover
              dataRef={dataRef}
              dataRefs={dataRefs}
              dataInicio={dataInicio}
              dataFim={dataFim}
              onDataRefChange={setDataRef}
              onDataInicioChange={setDataInicio}
              onDataFimChange={setDataFim}
            />
          )}
        </Chip>

        <Chip icon={<IconEye />} value={viewLabel} width={180}>
          {(close) => (
            <ViewPopover
              view={view}
              activeTab={activeTab}
              onChange={(v) => {
                setView(v);
                close();
              }}
            />
          )}
        </Chip>

        <Chip icon={<IconMore />} value="" width={240}>
          {(close) => (
            <MoreMenu
              pageSize={pageSize}
              onPageSizeChange={(n) => {
                setPageSize(n);
                close();
              }}
              onFakeOcupacao={() => {
                setRefreshKey((k) => k + 1);
                close();
              }}
              exportContext={{
                table: activeTab,
                view,
                dataRef,
                dataInicio,
                dataFim,
              }}
              onExportDone={close}
            />
          )}
        </Chip>
      </div>

      {/* Matriz */}
      <div style={{ flex: 1, overflow: "auto", padding: "8px 0 12px" }}>
        {matrix && (
          <MatrixTable
            matrix={matrix}
            onCellClick={
              matrix.row_type === "unidade"
                ? (id, data) => setExplainCell({ unidade_id: id, data })
                : undefined
            }
          />
        )}
      </div>

      {/* Footer: paginação + estatísticas */}
      {matrix && (
        <div
          style={{
            display: "flex",
            alignItems: "center",
            gap: 8,
            padding: "0 16px",
            height: 36,
            background: "#f8fafc",
            borderTop: "1px solid #e2e8f0",
            fontSize: 11,
            color: "#64748b",
            flex: "0 0 auto",
          }}
        >
          <button onClick={() => setPage(1)} disabled={page === 1} style={btnFooter}>« Primeira</button>
          <button onClick={() => setPage((p) => Math.max(1, p - 1))} disabled={page === 1} style={btnFooter}>‹ Anterior</button>
          <span>
            Página <strong style={{ color: "#1e293b" }}>{page}</strong> de <strong style={{ color: "#1e293b" }}>{totalPages}</strong>
          </span>
          <button onClick={() => setPage((p) => Math.min(totalPages, p + 1))} disabled={page >= totalPages} style={btnFooter}>Próxima ›</button>
          <button onClick={() => setPage(totalPages)} disabled={page >= totalPages} style={btnFooter}>Última »</button>
          <span style={{ marginLeft: 12 }}>
            {matrix.total_rows} {matrix.row_type === "unidade" ? "unidades" : matrix.row_type === "predio" ? "prédios" : "regiões"} · {matrix.columns.length} dias
          </span>
          {matrix.rows.length > 0 && (
            <span style={{ marginLeft: "auto" }}>
              min{" "}
              <strong style={{ color: "#1e293b" }}>
                {formatValue(matrix.min, matrix.format, { signed: !TABLES_UNSIGNED_PCT.has(matrix.table) })}
              </strong>
              <span style={{ margin: "0 8px", color: "#cbd5e1" }}>·</span>
              max{" "}
              <strong style={{ color: "#1e293b" }}>
                {formatValue(matrix.max, matrix.format, { signed: !TABLES_UNSIGNED_PCT.has(matrix.table) })}
              </strong>
            </span>
          )}
          {activeTab === "d" && (
            <button
              onClick={() => setPublishOpen(true)}
              style={{
                marginLeft: 16,
                background: "#4f46e5",
                color: "#ffffff",
                border: 0,
                padding: "6px 14px",
                borderRadius: 6,
                fontSize: 12,
                fontWeight: 600,
                fontFamily: "inherit",
                cursor: "pointer",
                display: "inline-flex",
                alignItems: "center",
                gap: 6,
                boxShadow: "0 1px 2px rgba(79,70,229,0.25)",
              }}
              onMouseEnter={(e) => (e.currentTarget.style.background = "#4338ca")}
              onMouseLeave={(e) => (e.currentTarget.style.background = "#4f46e5")}
              title="Empurra os preços do `d` pra Guesty"
            >
              <IconUpload />
              Publicar no Guesty
            </button>
          )}
        </div>
      )}

      {/* Modal de publicação */}
      {publishOpen && (
        <PublishModal
          dataRef={dataRef}
          dataInicio={dataInicio}
          dataFim={dataFim}
          onClose={() => setPublishOpen(false)}
          onSuccess={(r) => {
            setPublishResult(r);
            setPublishOpen(false);
            // auto-close só quando 100% sucesso; com falhas, fica aberto pra inspecionar
            if (r.falhas === 0) {
              window.setTimeout(() => setPublishResult(null), 6000);
            }
          }}
        />
      )}

      {/* Toast de resultado */}
      {publishResult && !publishDetailOpen && (
        <PublishToast
          result={publishResult}
          onClose={() => setPublishResult(null)}
          onShowDetails={() => setPublishDetailOpen(true)}
        />
      )}

      {/* Modal com detalhes das falhas */}
      {publishDetailOpen && publishResult && (
        <PublishDetailModal
          result={publishResult}
          onClose={() => setPublishDetailOpen(false)}
          onUpdate={(r) => setPublishResult(r)}
        />
      )}

      {/* Painel lateral: "Por que esse preço?" */}
      {explainCell && (
        <ExplainPanel
          unidadeId={explainCell.unidade_id}
          data={explainCell.data}
          dataReferencia={dataRef}
          onClose={() => setExplainCell(null)}
        />
      )}
    </main>
  );
}

// ============================================================
// Publicação no Guesty (mock)
// ============================================================

type PublishPreview = {
  unidades: number;
  dias: number;
  total_precos: number;
  impacto_total: number;
  preco_medio: number;
};

type PublishError = {
  unidade_id: number;
  unidade_label: string;
  data: string;
  motivo: string;
  motivo_label: string;
  recuperavel: boolean;
};

type PublishErrorSummary = {
  motivo: string;
  motivo_label: string;
  recuperavel: boolean;
  quantidade: number;
};

type PublishResult = {
  ok: boolean;
  modo: string;
  duration_ms: number;
  total_precos: number;
  sucessos: number;
  falhas: number;
  impacto_total: number;
  erros: PublishError[];
  resumo_falhas: PublishErrorSummary[];
};

function IconUpload() {
  return (
    <svg width="13" height="13" viewBox="0 0 14 14" fill="none">
      <path
        d="M7 9.5V2M7 2L4 5M7 2l3 3M2 11.5h10"
        stroke="currentColor"
        strokeWidth="1.4"
        strokeLinecap="round"
        strokeLinejoin="round"
      />
    </svg>
  );
}

function PublishModal({
  dataRef,
  dataInicio,
  dataFim,
  onClose,
  onSuccess,
}: {
  dataRef: string;
  dataInicio: string;
  dataFim: string;
  onClose: () => void;
  onSuccess: (r: PublishResult) => void;
}) {
  const [preview, setPreview] = useState<PublishPreview | null>(null);
  const [previewLoading, setPreviewLoading] = useState(true);
  const [periodIni, setPeriodIni] = useState(dataInicio);
  const [periodFim, setPeriodFim] = useState(dataFim);
  const [escopo, setEscopo] = useState<"todas" | "regiao">("todas");
  const [regiaoId, setRegiaoId] = useState<number | null>(null);
  const [regioes, setRegioes] = useState<{ id: number; nome: string }[]>([]);
  const [sobrescreverTravados, setSobrescreverTravados] = useState(false);
  const [pularBloqueios, setPularBloqueios] = useState(true);
  const [submitting, setSubmitting] = useState(false);
  const [submitErr, setSubmitErr] = useState<string | null>(null);

  useEffect(() => {
    const ctl = new AbortController();
    setPreviewLoading(true);
    const qs = new URLSearchParams({
      data_referencia: dataRef,
      data_inicio: periodIni,
      data_fim: periodFim,
    });
    if (escopo === "regiao" && regiaoId !== null) qs.set("regiao_id", String(regiaoId));
    fetch(`/api/guesty/publicar/preview?${qs}`, { signal: ctl.signal })
      .then(async (r) => {
        if (!r.ok) throw new Error(`HTTP ${r.status}`);
        return r.json();
      })
      .then((d: PublishPreview) => setPreview(d))
      .catch(() => {})
      .finally(() => setPreviewLoading(false));
    return () => ctl.abort();
  }, [dataRef, periodIni, periodFim, escopo, regiaoId]);

  useEffect(() => {
    fetch("/api/regras/escopo/regiao")
      .then((r) => (r.ok ? r.json() : []))
      .then((items: { id: number; nome: string }[]) => {
        setRegioes(items);
        if (items.length && regiaoId === null) setRegiaoId(items[0].id);
      })
      .catch(() => {});
  }, []); // eslint-disable-line react-hooks/exhaustive-deps

  const submit = async () => {
    setSubmitting(true);
    setSubmitErr(null);
    try {
      const body = {
        data_referencia: dataRef,
        data_inicio: periodIni,
        data_fim: periodFim,
        regiao_id: escopo === "regiao" ? regiaoId : null,
        sobrescrever_travados: sobrescreverTravados,
        pular_bloqueios: pularBloqueios,
      };
      const r = await apiFetch("/api/guesty/publicar", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      });
      const d = await r.json();
      if (!r.ok) throw new Error(d.detail ?? `HTTP ${r.status}`);
      onSuccess(d as PublishResult);
    } catch (e) {
      setSubmitErr(String(e));
      setSubmitting(false);
    }
  };

  return (
    <div
      onClick={onClose}
      style={{
        position: "fixed",
        inset: 0,
        background: "rgba(15,23,42,0.45)",
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
        zIndex: 100,
      }}
    >
      <div
        onClick={(e) => e.stopPropagation()}
        style={{
          width: 460,
          maxWidth: "94vw",
          background: "#ffffff",
          borderRadius: 10,
          boxShadow: "0 20px 25px -5px rgba(15,23,42,0.15), 0 8px 10px -6px rgba(15,23,42,0.10)",
          padding: 20,
          fontSize: 13,
          color: "#1e293b",
        }}
      >
        <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 14 }}>
          <span style={{ fontSize: 15, fontWeight: 600 }}>Publicar preços no Guesty</span>
          <span
            style={{
              fontSize: 9,
              fontWeight: 700,
              letterSpacing: 0.4,
              color: "#b45309",
              background: "#fef3c7",
              padding: "2px 6px",
              borderRadius: 4,
              textTransform: "uppercase",
            }}
          >
            mock
          </span>
        </div>

        {/* Preview de stats */}
        <div
          style={{
            background: "#f8fafc",
            border: "1px solid #e2e8f0",
            borderRadius: 8,
            padding: "12px 14px",
            marginBottom: 14,
            fontSize: 12,
            display: "flex",
            flexDirection: "column",
            gap: 6,
          }}
        >
          {previewLoading || !preview ? (
            <span style={{ color: "#94a3b8" }}>calculando preview…</span>
          ) : (
            <>
              <Row label="Unidades">
                <strong>{preview.unidades}</strong>
              </Row>
              <Row label="Dias">
                <strong>{preview.dias}</strong>
              </Row>
              <Row label="Preços diários">
                <strong>{preview.total_precos.toLocaleString("pt-BR")}</strong>
              </Row>
              <Row label="Impacto total vs Pb">
                <strong style={{ color: preview.impacto_total >= 0 ? "#15803d" : "#b91c1c" }}>
                  {preview.impacto_total >= 0 ? "+" : ""}
                  {preview.impacto_total.toLocaleString("pt-BR", {
                    style: "currency",
                    currency: "BRL",
                    maximumFractionDigits: 0,
                  })}
                </strong>
              </Row>
            </>
          )}
        </div>

        {/* Período */}
        <div style={{ marginBottom: 12 }}>
          <Label>Período</Label>
          <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 8 }}>
            <input type="date" value={periodIni} onChange={(e) => setPeriodIni(e.target.value)} style={popInp} />
            <input type="date" value={periodFim} onChange={(e) => setPeriodFim(e.target.value)} style={popInp} />
          </div>
        </div>

        {/* Escopo */}
        <div style={{ marginBottom: 12 }}>
          <Label>Escopo</Label>
          <div style={{ display: "flex", flexDirection: "column", gap: 4 }}>
            <label style={{ display: "flex", alignItems: "center", gap: 6, cursor: "pointer" }}>
              <input type="radio" checked={escopo === "todas"} onChange={() => setEscopo("todas")} />
              Todas as unidades
            </label>
            <label style={{ display: "flex", alignItems: "center", gap: 6, cursor: "pointer" }}>
              <input type="radio" checked={escopo === "regiao"} onChange={() => setEscopo("regiao")} />
              Só uma região:
              <select
                value={regiaoId ?? ""}
                onChange={(e) => setRegiaoId(Number(e.target.value))}
                disabled={escopo !== "regiao"}
                style={{ ...popInp, width: 180, marginLeft: 4 }}
              >
                {regioes.map((r) => (
                  <option key={r.id} value={r.id}>
                    {r.nome}
                  </option>
                ))}
              </select>
            </label>
          </div>
        </div>

        {/* Opções */}
        <div style={{ marginBottom: 14 }}>
          <Label>Opções</Label>
          <div style={{ display: "flex", flexDirection: "column", gap: 6 }}>
            <label style={{ display: "flex", alignItems: "center", gap: 6, cursor: "pointer", fontSize: 12 }}>
              <input
                type="checkbox"
                checked={sobrescreverTravados}
                onChange={(e) => setSobrescreverTravados(e.target.checked)}
              />
              Sobrescrever preços travados manualmente no Guesty
            </label>
            <label style={{ display: "flex", alignItems: "center", gap: 6, cursor: "pointer", fontSize: 12 }}>
              <input
                type="checkbox"
                checked={pularBloqueios}
                onChange={(e) => setPularBloqueios(e.target.checked)}
              />
              Pular dias bloqueados (manutenção, etc.)
            </label>
          </div>
        </div>

        <div
          style={{
            fontSize: 11,
            color: "#64748b",
            paddingTop: 10,
            borderTop: "1px solid #f1f5f9",
            marginBottom: 14,
          }}
        >
          Esta ação é registrada na Auditoria. Em modo <strong>mock</strong>, nenhum preço é
          enviado pra Guesty — só simulamos o fluxo até a integração OAuth ficar pronta.
        </div>

        {submitErr && (
          <div style={{ color: "#dc2626", fontSize: 12, marginBottom: 8 }}>{submitErr}</div>
        )}

        <div style={{ display: "flex", justifyContent: "flex-end", gap: 8 }}>
          <button onClick={onClose} disabled={submitting} style={btnSecondary}>
            Cancelar
          </button>
          <button onClick={submit} disabled={submitting || !preview} style={btnPrimary}>
            {submitting
              ? "Publicando…"
              : preview
              ? `Publicar ${preview.total_precos.toLocaleString("pt-BR")}`
              : "Publicar"}
          </button>
        </div>
      </div>
    </div>
  );
}

function PublishToast({
  result,
  onClose,
  onShowDetails,
}: {
  result: PublishResult;
  onClose: () => void;
  onShowDetails: () => void;
}) {
  return (
    <div
      style={{
        position: "fixed",
        bottom: 20,
        right: 20,
        zIndex: 90,
        background: "#ffffff",
        border: "1px solid #e2e8f0",
        borderLeft: `3px solid ${result.falhas > 0 ? "#f59e0b" : "#15803d"}`,
        borderRadius: 8,
        boxShadow: "0 10px 15px -3px rgba(15,23,42,0.10)",
        padding: "12px 16px",
        minWidth: 280,
        fontSize: 12,
        color: "#1e293b",
      }}
    >
      <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 6 }}>
        <span style={{ fontSize: 14 }}>{result.falhas > 0 ? "⚠" : "✓"}</span>
        <strong style={{ fontSize: 13 }}>
          {result.sucessos.toLocaleString("pt-BR")} preços publicados
        </strong>
        {result.falhas > 0 && (
          <button
            onClick={onShowDetails}
            style={{
              background: "transparent",
              border: 0,
              padding: 0,
              color: "#b45309",
              fontSize: 12,
              fontWeight: 500,
              fontFamily: "inherit",
              cursor: "pointer",
              textDecoration: "underline",
              textDecorationStyle: "dotted",
              textUnderlineOffset: 2,
            }}
            title="Ver detalhes das falhas"
          >
            · {result.falhas} falharam
          </button>
        )}
        <button
          onClick={onClose}
          style={{
            marginLeft: "auto",
            background: "transparent",
            border: 0,
            color: "#94a3b8",
            cursor: "pointer",
            padding: 0,
            fontSize: 14,
          }}
          aria-label="fechar"
        >
          ×
        </button>
      </div>
      <div style={{ color: "#64748b", fontSize: 11 }}>
        em {(result.duration_ms / 1000).toFixed(1)}s · modo {result.modo}
      </div>
    </div>
  );
}

function PublishDetailModal({
  result,
  onClose,
  onUpdate,
}: {
  result: PublishResult;
  onClose: () => void;
  onUpdate: (r: PublishResult) => void;
}) {
  const [retryState, setRetryState] = useState<"idle" | "running" | "done">("idle");
  const [retryMsg, setRetryMsg] = useState("");

  const recuperaveis = result.erros.filter((e) => e.recuperavel);
  const permanentes = result.erros.filter((e) => !e.recuperavel);

  const retryRecuperaveis = async () => {
    setRetryState("running");
    // Mock: simula 1s, 90% das recuperáveis viram sucesso, 10% continuam.
    await new Promise((r) => window.setTimeout(r, 1000));
    const ainda_falham = recuperaveis.filter((_, i) => i % 10 === 0); // 10% persistem
    const recuperados = recuperaveis.length - ainda_falham.length;
    const novos_erros = [...permanentes, ...ainda_falham];
    const novo_resumo: Record<string, PublishErrorSummary> = {};
    for (const e of novos_erros) {
      const k = e.motivo;
      if (!novo_resumo[k]) {
        novo_resumo[k] = {
          motivo: k,
          motivo_label: e.motivo_label,
          recuperavel: e.recuperavel,
          quantidade: 0,
        };
      }
      novo_resumo[k].quantidade += 1;
    }
    onUpdate({
      ...result,
      sucessos: result.sucessos + recuperados,
      falhas: novos_erros.length,
      erros: novos_erros,
      resumo_falhas: Object.values(novo_resumo).sort(
        (a, b) => b.quantidade - a.quantidade
      ),
    });
    setRetryState("done");
    setRetryMsg(`${recuperados} de ${recuperaveis.length} recuperados`);
  };

  return (
    <div
      onClick={onClose}
      style={{
        position: "fixed",
        inset: 0,
        background: "rgba(15,23,42,0.45)",
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
        zIndex: 100,
      }}
    >
      <div
        onClick={(e) => e.stopPropagation()}
        style={{
          width: 560,
          maxWidth: "94vw",
          maxHeight: "82vh",
          background: "#ffffff",
          borderRadius: 10,
          boxShadow:
            "0 20px 25px -5px rgba(15,23,42,0.15), 0 8px 10px -6px rgba(15,23,42,0.10)",
          padding: 20,
          fontSize: 13,
          color: "#1e293b",
          display: "flex",
          flexDirection: "column",
        }}
      >
        <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 14 }}>
          <span style={{ fontSize: 15, fontWeight: 600 }}>
            {result.falhas} preços não publicados
          </span>
          <span
            style={{
              fontSize: 9,
              fontWeight: 700,
              letterSpacing: 0.4,
              color: "#b45309",
              background: "#fef3c7",
              padding: "2px 6px",
              borderRadius: 4,
              textTransform: "uppercase",
            }}
          >
            mock
          </span>
          <button
            onClick={onClose}
            style={{
              marginLeft: "auto",
              background: "transparent",
              border: 0,
              color: "#94a3b8",
              cursor: "pointer",
              padding: 4,
              fontSize: 16,
              lineHeight: 1,
            }}
            aria-label="fechar"
          >
            ×
          </button>
        </div>

        {/* Resumo por categoria */}
        <div
          style={{
            background: "#f8fafc",
            border: "1px solid #e2e8f0",
            borderRadius: 8,
            padding: 12,
            marginBottom: 14,
            fontSize: 12,
          }}
        >
          <div
            style={{
              fontSize: 10,
              fontWeight: 500,
              color: "#64748b",
              marginBottom: 8,
              letterSpacing: 0.2,
              textTransform: "uppercase",
            }}
          >
            Categorias
          </div>
          <div style={{ display: "flex", flexDirection: "column", gap: 6 }}>
            {result.resumo_falhas.map((r) => (
              <div
                key={r.motivo}
                style={{ display: "flex", alignItems: "center", gap: 10 }}
              >
                <span style={{ flex: 1 }}>{r.motivo_label}</span>
                <span
                  style={{
                    fontVariantNumeric: "tabular-nums",
                    fontWeight: 600,
                    minWidth: 30,
                    textAlign: "right",
                  }}
                >
                  {r.quantidade}
                </span>
                <span
                  style={{
                    fontSize: 10,
                    fontWeight: 600,
                    color: r.recuperavel ? "#15803d" : "#64748b",
                    background: r.recuperavel ? "#dcfce7" : "#f1f5f9",
                    padding: "2px 7px",
                    borderRadius: 10,
                    minWidth: 96,
                    textAlign: "center",
                  }}
                >
                  {r.recuperavel ? "↻ recuperável" : "permanente"}
                </span>
              </div>
            ))}
          </div>
        </div>

        {/* Lista detalhada (scrollable) */}
        <div
          style={{
            border: "1px solid #e2e8f0",
            borderRadius: 8,
            overflow: "hidden",
            marginBottom: 14,
            flex: 1,
            minHeight: 120,
            maxHeight: 280,
            overflowY: "auto",
          }}
        >
          <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 11.5 }}>
            <thead>
              <tr>
                <th style={detailTh}>Unidade</th>
                <th style={detailTh}>Data</th>
                <th style={detailTh}>Motivo</th>
                <th style={detailTh}></th>
              </tr>
            </thead>
            <tbody>
              {result.erros.map((e, i) => (
                <tr key={i} style={{ borderTop: i === 0 ? 0 : "1px solid #f1f5f9" }}>
                  <td style={detailTd}>
                    <code style={{ fontSize: 11, color: "#1e293b" }}>{e.unidade_label}</code>
                  </td>
                  <td style={{ ...detailTd, fontVariantNumeric: "tabular-nums" }}>
                    {formatBrDate(e.data)}
                  </td>
                  <td style={detailTd}>{e.motivo_label}</td>
                  <td style={{ ...detailTd, textAlign: "right" }}>
                    {e.recuperavel ? (
                      <span style={{ fontSize: 10, color: "#15803d", fontWeight: 600 }}>↻</span>
                    ) : null}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>

        <div
          style={{
            fontSize: 11,
            color: "#64748b",
            paddingTop: 10,
            borderTop: "1px solid #f1f5f9",
            marginBottom: 14,
          }}
        >
          Recuperáveis (rate limit, 5xx) podem ser tentadas de novo automaticamente.
          Permanentes (listing inativo, moeda incompatível) precisam intervenção
          operacional — geralmente ressincronizar o cadastro com o Guesty.
        </div>

        <div style={{ display: "flex", justifyContent: "flex-end", alignItems: "center", gap: 8 }}>
          {retryState === "done" && (
            <span style={{ fontSize: 11, color: "#15803d", marginRight: "auto" }}>✓ {retryMsg}</span>
          )}
          <button onClick={onClose} style={btnSecondary}>
            Fechar
          </button>
          {recuperaveis.length > 0 && (
            <button
              onClick={retryRecuperaveis}
              disabled={retryState === "running"}
              style={{
                ...btnPrimary,
                background: retryState === "running" ? "#94a3b8" : "#4f46e5",
              }}
            >
              {retryState === "running"
                ? "Tentando novamente…"
                : `↻ Retry recuperáveis (${recuperaveis.length})`}
            </button>
          )}
        </div>
      </div>
    </div>
  );
}

function formatBrDate(iso: string): string {
  const [y, m, d] = iso.split("-").map(Number);
  return `${String(d).padStart(2, "0")}/${MONTHS_PT[m - 1]}`;
}

const detailTh: React.CSSProperties = {
  textAlign: "left",
  padding: "6px 10px",
  background: "#f8fafc",
  borderBottom: "1px solid #e2e8f0",
  color: "#4338ca",
  fontWeight: 600,
  fontSize: 10,
  whiteSpace: "nowrap",
  letterSpacing: 0.2,
  textTransform: "uppercase",
  position: "sticky",
  top: 0,
};

const detailTd: React.CSSProperties = {
  padding: "5px 10px",
  whiteSpace: "nowrap",
};

function Row({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
      <span style={{ color: "#64748b" }}>{label}</span>
      <span>{children}</span>
    </div>
  );
}

const btnPrimary: React.CSSProperties = {
  background: "#4f46e5",
  color: "#ffffff",
  border: 0,
  padding: "7px 14px",
  borderRadius: 6,
  fontSize: 12,
  fontWeight: 600,
  fontFamily: "inherit",
  cursor: "pointer",
};

const btnSecondary: React.CSSProperties = {
  background: "#ffffff",
  color: "#475569",
  border: "1px solid #e2e8f0",
  padding: "7px 14px",
  borderRadius: 6,
  fontSize: 12,
  fontWeight: 500,
  fontFamily: "inherit",
  cursor: "pointer",
};

// ============================================================
// Toolbar helpers: ícones + popovers
// ============================================================

function formatPeriodChip(ini: string, fim: string): string {
  if (!ini || !fim) return "—";
  const fmt = (iso: string) => {
    const [y, m, d] = iso.split("-").map(Number);
    return `${String(d).padStart(2, "0")}/${MONTHS_PT[m - 1]}`;
  };
  return `${fmt(ini)} → ${fmt(fim)}`;
}

function IconCalendar() {
  return (
    <svg width="13" height="13" viewBox="0 0 14 14" fill="none">
      <rect x="2" y="3" width="10" height="9" rx="1.5" stroke="currentColor" strokeWidth="1.3" />
      <path d="M2 6h10" stroke="currentColor" strokeWidth="1.3" />
      <path d="M5 1.5v3M9 1.5v3" stroke="currentColor" strokeWidth="1.3" strokeLinecap="round" />
    </svg>
  );
}

function IconEye() {
  return (
    <svg width="14" height="14" viewBox="0 0 14 14" fill="none">
      <path
        d="M1 7s2.5-4 6-4 6 4 6 4-2.5 4-6 4-6-4-6-4z"
        stroke="currentColor"
        strokeWidth="1.3"
        strokeLinejoin="round"
      />
      <circle cx="7" cy="7" r="1.6" stroke="currentColor" strokeWidth="1.3" />
    </svg>
  );
}

function IconMore() {
  return (
    <svg width="14" height="14" viewBox="0 0 14 14" fill="none">
      <circle cx="3" cy="7" r="1.2" fill="currentColor" />
      <circle cx="7" cy="7" r="1.2" fill="currentColor" />
      <circle cx="11" cy="7" r="1.2" fill="currentColor" />
    </svg>
  );
}

function IconDownload() {
  return (
    <svg width="11" height="11" viewBox="0 0 14 14" fill="none">
      <path
        d="M7 1v8M7 9L4 6M7 9l3-3M2 11.5h10"
        stroke="currentColor"
        strokeWidth="1.4"
        strokeLinecap="round"
        strokeLinejoin="round"
      />
    </svg>
  );
}

function PeriodPopover({
  dataRef,
  dataRefs,
  dataInicio,
  dataFim,
  onDataRefChange,
  onDataInicioChange,
  onDataFimChange,
}: {
  dataRef: string;
  dataRefs: string[];
  dataInicio: string;
  dataFim: string;
  onDataRefChange: (v: string) => void;
  onDataInicioChange: (v: string) => void;
  onDataFimChange: (v: string) => void;
}) {
  const setRange = (days: number) => {
    onDataInicioChange(dataRef);
    onDataFimChange(addDaysISO(dataRef, days));
  };
  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 10, fontSize: 12 }}>
      <div>
        <Label>Data referência</Label>
        <select value={dataRef} onChange={(e) => onDataRefChange(e.target.value)} style={popInp}>
          {dataRefs.map((v) => (
            <option key={v} value={v}>
              {v}
            </option>
          ))}
        </select>
      </div>
      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 8 }}>
        <div>
          <Label>De</Label>
          <input type="date" value={dataInicio} onChange={(e) => onDataInicioChange(e.target.value)} style={popInp} />
        </div>
        <div>
          <Label>Até</Label>
          <input type="date" value={dataFim} onChange={(e) => onDataFimChange(e.target.value)} style={popInp} />
        </div>
      </div>
      <div>
        <Label>Atalhos</Label>
        <div style={{ display: "flex", gap: 6 }}>
          {[7, 30, 90, 180].map((n) => (
            <button key={n} onClick={() => setRange(n)} style={preset}>
              {n}d
            </button>
          ))}
        </div>
      </div>
    </div>
  );
}

function ViewPopover({
  view,
  activeTab,
  onChange,
}: {
  view: "unidade" | "regiao" | "predio";
  activeTab: string;
  onChange: (v: "unidade" | "regiao" | "predio") => void;
}) {
  const opts: { v: "unidade" | "regiao" | "predio"; label: string; hint: string }[] = [
    { v: "unidade", label: "Por unidade", hint: "linha = apartamento" },
    { v: "predio", label: "Por prédio", hint: "linha = prédio (média)" },
    { v: "regiao", label: "Por região", hint: "linha = região (média)" },
  ];
  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 2 }}>
      {opts.map((o) => {
        const disabled = o.v === "unidade" && AGGREGATED_ONLY_TABS.has(activeTab);
        const active = view === o.v;
        return (
          <button
            key={o.v}
            onClick={() => !disabled && onChange(o.v)}
            disabled={disabled}
            style={{
              display: "flex",
              flexDirection: "column",
              alignItems: "flex-start",
              gap: 1,
              padding: "8px 10px",
              background: active ? "#eef2ff" : "transparent",
              border: 0,
              borderRadius: 6,
              color: disabled ? "#cbd5e1" : active ? "#4338ca" : "#1e293b",
              fontWeight: active ? 600 : 500,
              fontSize: 12,
              fontFamily: "inherit",
              cursor: disabled ? "not-allowed" : "pointer",
              textAlign: "left",
            }}
            title={disabled ? "Esta aba não tem dado em granularidade de unidade" : undefined}
          >
            <span>{o.label}</span>
            <span style={{ fontSize: 10, fontWeight: 400, color: disabled ? "#e2e8f0" : "#94a3b8" }}>
              {o.hint}
            </span>
          </button>
        );
      })}
    </div>
  );
}

function MoreMenu({
  pageSize,
  onPageSizeChange,
  onFakeOcupacao,
  exportContext,
  onExportDone,
}: {
  pageSize: number;
  onPageSizeChange: (n: number) => void;
  onFakeOcupacao: () => void;
  exportContext: {
    table: string;
    view: "unidade" | "regiao" | "predio";
    dataRef: string;
    dataInicio: string;
    dataFim: string;
  };
  onExportDone: () => void;
}) {
  const [fakeState, setFakeState] = useState<"idle" | "running" | "ok" | "error">("idle");
  const [fakeMsg, setFakeMsg] = useState("");
  const [exportState, setExportState] = useState<"idle" | "running" | "error">("idle");
  const [exportErr, setExportErr] = useState("");

  const runFake = async () => {
    setFakeState("running");
    setFakeMsg("");
    try {
      const r = await apiFetch("/api/regras/fake-ocupacao", { method: "POST" });
      const d = await r.json();
      if (!r.ok) throw new Error(d.detail ?? `HTTP ${r.status}`);
      setFakeState("ok");
      setFakeMsg(`feito em ${d.duration_ms} ms`);
      onFakeOcupacao();
    } catch (e) {
      setFakeState("error");
      setFakeMsg(String(e));
    }
  };

  const runExport = async (format: "csv" | "xlsx") => {
    setExportState("running");
    setExportErr("");
    try {
      const qs = new URLSearchParams({
        data_referencia: exportContext.dataRef,
        data_inicio: exportContext.dataInicio,
        data_fim: exportContext.dataFim,
        view: exportContext.view,
        format,
      });
      const r = await fetch(
        `/api/simulador/export/${exportContext.table}?${qs}`,
      );
      if (!r.ok) {
        const errorBody = await r.json().catch(() => ({}));
        throw new Error(errorBody.detail ?? `HTTP ${r.status}`);
      }
      const blob = await r.blob();
      const filename = (() => {
        const dispo = r.headers.get("Content-Disposition") ?? "";
        const m = dispo.match(/filename="([^"]+)"/);
        return m
          ? m[1]
          : `cyclinn_${exportContext.table}_${exportContext.view}.${format}`;
      })();
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = filename;
      document.body.appendChild(a);
      a.click();
      document.body.removeChild(a);
      URL.revokeObjectURL(url);
      setExportState("idle");
      onExportDone();
    } catch (e) {
      setExportState("error");
      setExportErr(String(e));
    }
  };

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 4, fontSize: 12 }}>
      <div style={{ padding: "4px 6px" }}>
        <Label>Linhas por página</Label>
        <select value={pageSize} onChange={(e) => onPageSizeChange(Number(e.target.value))} style={popInp}>
          {[10, 25, 50, 100].map((n) => (
            <option key={n} value={n}>
              {n}
            </option>
          ))}
        </select>
      </div>
      <div style={{ height: 1, background: "#e2e8f0", margin: "4px 0" }} />
      <div style={{ padding: "4px 6px" }}>
        <Label>Exportar matriz</Label>
        <div style={{ display: "flex", gap: 4, marginTop: 2 }}>
          <button
            onClick={() => runExport("csv")}
            disabled={exportState === "running"}
            style={{
              flex: 1,
              padding: "6px 8px",
              background: "#f8fafc",
              border: "1px solid #e2e8f0",
              borderRadius: 5,
              fontSize: 11,
              fontWeight: 600,
              fontFamily: "inherit",
              color: "#475569",
              cursor: exportState === "running" ? "not-allowed" : "pointer",
              display: "flex",
              alignItems: "center",
              justifyContent: "center",
              gap: 4,
            }}
          >
            <IconDownload /> CSV
          </button>
          <button
            onClick={() => runExport("xlsx")}
            disabled={exportState === "running"}
            style={{
              flex: 1,
              padding: "6px 8px",
              background: "#f0fdf4",
              border: "1px solid #bbf7d0",
              borderRadius: 5,
              fontSize: 11,
              fontWeight: 600,
              fontFamily: "inherit",
              color: "#166534",
              cursor: exportState === "running" ? "not-allowed" : "pointer",
              display: "flex",
              alignItems: "center",
              justifyContent: "center",
              gap: 4,
            }}
          >
            <IconDownload /> Excel
          </button>
        </div>
        {exportState === "running" && (
          <span style={{ fontSize: 10, color: "#64748b", display: "block", marginTop: 4 }}>
            preparando…
          </span>
        )}
        {exportState === "error" && (
          <span style={{ fontSize: 10, color: "#dc2626", display: "block", marginTop: 4 }}>
            ✕ {exportErr}
          </span>
        )}
      </div>
      <div style={{ height: 1, background: "#e2e8f0", margin: "4px 0" }} />
      <button
        onClick={runFake}
        disabled={fakeState === "running"}
        style={{
          display: "flex",
          alignItems: "center",
          gap: 8,
          padding: "8px 10px",
          background: "transparent",
          border: 0,
          borderRadius: 6,
          color: "#1e293b",
          fontSize: 12,
          fontWeight: 500,
          fontFamily: "inherit",
          cursor: fakeState === "running" ? "not-allowed" : "pointer",
          textAlign: "left",
        }}
        onMouseEnter={(e) => {
          if (fakeState !== "running") e.currentTarget.style.background = "#f1f5f9";
        }}
        onMouseLeave={(e) => (e.currentTarget.style.background = "transparent")}
      >
        <span>🧪</span>
        <span style={{ flex: 1 }}>
          {fakeState === "running" ? "Gerando…" : "Gerar dados de teste"}
        </span>
      </button>
      {fakeState === "ok" && (
        <span style={{ padding: "0 12px 6px", color: "#15803d", fontSize: 10 }}>✓ {fakeMsg}</span>
      )}
      {fakeState === "error" && (
        <span style={{ padding: "0 12px 6px", color: "#dc2626", fontSize: 10 }}>✕ {fakeMsg}</span>
      )}
    </div>
  );
}

function Label({ children }: { children: React.ReactNode }) {
  return (
    <div style={{ fontSize: 10, fontWeight: 500, color: "#64748b", marginBottom: 4, letterSpacing: 0.2, textTransform: "uppercase" }}>
      {children}
    </div>
  );
}

const popInp: React.CSSProperties = {
  width: "100%",
  padding: "5px 8px",
  border: "1px solid #e2e8f0",
  borderRadius: 5,
  fontSize: 12,
  fontFamily: "inherit",
  color: "#1e293b",
  background: "#ffffff",
};

const preset: React.CSSProperties = {
  flex: 1,
  padding: "4px 0",
  background: "#f8fafc",
  border: "1px solid #e2e8f0",
  borderRadius: 5,
  fontSize: 11,
  fontWeight: 500,
  fontFamily: "inherit",
  color: "#475569",
  cursor: "pointer",
};

const btnFooter: React.CSSProperties = {
  padding: "3px 8px",
  background: "#ffffff",
  border: "1px solid #e2e8f0",
  borderRadius: 4,
  fontSize: 11,
  fontFamily: "inherit",
  color: "#475569",
  cursor: "pointer",
};

function MatrixTable({
  matrix,
  onCellClick,
}: {
  matrix: Matrix;
  onCellClick?: (id: number, data: string) => void;
}) {
  if (matrix.rows.length === 0) {
    return <div style={{ padding: 24, color: "#64748b" }}>Sem dados para exibir.</div>;
  }

  const firstColStyle: React.CSSProperties = {
    position: "sticky",
    left: 0,
    background: "#f8fafc",
    padding: "6px 10px",
    borderRight: "1px solid #e2e8f0",
    borderBottom: "1px solid #f1f5f9",
    fontWeight: 500,
    fontSize: 12,
    color: "#1e293b",
    whiteSpace: "nowrap",
    zIndex: 2,
  };

  return (
    <table style={{ borderCollapse: "collapse", fontSize: 11.5 }}>
      <thead>
        <tr>
          <th style={{ ...firstColStyle, position: "sticky", top: 0, left: 0, zIndex: 3, color: "#4338ca", fontWeight: 600 }}>
            {matrix.row_type === "unidade" ? "Unidade" : matrix.row_type === "predio" ? "Prédio" : "Região"}
          </th>
          {matrix.columns.map((c) => {
            const wknd = isWeekend(c);
            const isRef = c === matrix.data_referencia;
            return (
              <th
                key={c}
                style={{
                  position: "sticky",
                  top: 0,
                  background: isRef ? "#e0e7ff" : wknd ? "#eef2ff" : "#f8fafc",
                  padding: "6px 8px",
                  borderBottom: "1px solid #e2e8f0",
                  borderLeft: isRef ? "2px solid #4f46e5" : undefined,
                  color: "#4338ca",
                  fontWeight: 600,
                  whiteSpace: "nowrap",
                  minWidth: 72,
                  zIndex: 1,
                  fontVariantNumeric: "tabular-nums",
                }}
                title={isRef ? "Data de referência (hoje)" : undefined}
              >
                {formatDateHeader(c)}
                {isRef && (
                  <span
                    style={{
                      marginLeft: 4,
                      fontSize: 9,
                      color: "#4f46e5",
                      verticalAlign: "middle",
                    }}
                  >
                    ●
                  </span>
                )}
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
              const semHeatmap = matrix.table === "pb";
              const isRef = matrix.columns[i] === matrix.data_referencia;
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
              // Em ocupacao_regiao o próprio valor da célula já mostra a real,
              // então o tooltip traz apenas o gap. Nas demais (ex: pi) mantém valor + delta.
              const tooltip = v === null
                ? "sem dado"
                : useColorDriver && matrix.table === "ocupacao_regiao"
                ? `${formatValue(cv, "percent")} ${deltaLabel}`
                : useColorDriver
                ? `${formatValue(v, matrix.format)}  ·  ${formatValue(cv, "percent")} ${deltaLabel}`
                : formatValue(v, matrix.format);
              const clickable = !!onCellClick && v !== null;
              return (
                <td
                  key={i}
                  onClick={clickable ? () => onCellClick!(row.id, matrix.columns[i]) : undefined}
                  style={{
                    background: bg,
                    color: v === null ? "#cbd5e1" : textColor(bg),
                    padding: "4px 8px",
                    borderBottom: "1px solid #f1f5f9",
                    borderRight: "1px solid #f1f5f9",
                    borderLeft: isRef ? "2px solid #4f46e5" : undefined,
                    textAlign: "right",
                    whiteSpace: "nowrap",
                    minWidth: 72,
                    fontVariantNumeric: "tabular-nums",
                    cursor: clickable ? "pointer" : "default",
                  }}
                  title={clickable ? `${tooltip} · clique pra explicar` : tooltip}
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
              title="Soma diária do impacto (Preço Final − Preço Base) em todas as unidades"
            >
              Impacto (R$)
            </th>
            {matrix.day_totals.map((t, i) => {
              const color = t === null || t === 0 ? "#475569" : t > 0 ? "#15803d" : "#b91c1c";
              const isRef = matrix.columns[i] === matrix.data_referencia;
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
                    borderLeft: isRef ? "2px solid #4f46e5" : undefined,
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

// ============================================================
// "Por que esse preço?" — painel lateral de decomposição
// ============================================================

type ExplainRule = {
  regra_id: number;
  ajuste_pct: number;
  label: string;
};

type ExplainFator = {
  tipo: string;
  label: string;
  ajuste_pct: number;
  regras: ExplainRule[];
  link_crud?: string | null;
  nota?: string;
};

type ExplainData = {
  unidade: {
    unidade_id: number;
    codigo_externo: string;
    predio_nome: string;
    regiao_nome: string;
    segmento_nome: string | null;
  };
  data: string;
  data_referencia: string;
  pb: number;
  fatores_priori: ExplainFator[];
  pi: number;
  fatores_posteriori: ExplainFator[];
  d: number;
};

function ExplainPanel({
  unidadeId,
  data,
  dataReferencia,
  onClose,
}: {
  unidadeId: number;
  data: string;
  dataReferencia: string;
  onClose: () => void;
}) {
  const [data_, setData] = useState<ExplainData | null>(null);
  const [loading, setLoading] = useState(true);
  const [err, setErr] = useState<string | null>(null);

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") onClose();
    };
    document.addEventListener("keydown", onKey);
    return () => document.removeEventListener("keydown", onKey);
  }, [onClose]);

  useEffect(() => {
    setLoading(true);
    setErr(null);
    const qs = new URLSearchParams({ data_referencia: dataReferencia });
    fetch(`/api/simulador/explicar/${unidadeId}/${data}?${qs}`)
      .then(async (r) => {
        if (!r.ok) throw new Error((await r.json()).detail ?? `HTTP ${r.status}`);
        return r.json();
      })
      .then((d: ExplainData) => setData(d))
      .catch((e) => setErr(String(e)))
      .finally(() => setLoading(false));
  }, [unidadeId, data, dataReferencia]);

  return (
    <div
      style={{
        position: "fixed",
        top: 44,
        right: 0,
        bottom: 0,
        width: 380,
        maxWidth: "94vw",
        background: "#ffffff",
        borderLeft: "1px solid #e2e8f0",
        boxShadow: "-8px 0 16px -8px rgba(15,23,42,0.10)",
        zIndex: 60,
        display: "flex",
        flexDirection: "column",
        overflow: "hidden",
      }}
    >
      {/* Header */}
      <div
        style={{
          display: "flex",
          alignItems: "center",
          gap: 8,
          padding: "12px 16px",
          borderBottom: "1px solid #f1f5f9",
          flex: "0 0 auto",
        }}
      >
        <span style={{ fontSize: 13, fontWeight: 600, color: "#1e293b" }}>
          Por que esse preço?
        </span>
        <button
          onClick={onClose}
          style={{
            marginLeft: "auto",
            background: "transparent",
            border: 0,
            color: "#94a3b8",
            cursor: "pointer",
            padding: 4,
            fontSize: 16,
            lineHeight: 1,
          }}
          aria-label="fechar"
          title="Esc"
        >
          ×
        </button>
      </div>

      {/* Body */}
      <div style={{ flex: 1, overflow: "auto", padding: "14px 16px" }}>
        {loading && <div style={{ color: "#94a3b8", fontSize: 12 }}>carregando…</div>}
        {err && <div style={{ color: "#dc2626", fontSize: 12 }}>{err}</div>}
        {data_ && (
          <div style={{ display: "flex", flexDirection: "column", gap: 10 }}>
            {/* Identificação */}
            <div style={{ fontSize: 12, color: "#64748b" }}>
              <div>
                <strong style={{ color: "#1e293b" }}>{data_.unidade.codigo_externo}</strong>
                {" · "}
                <span style={{ fontVariantNumeric: "tabular-nums" }}>
                  {formatBrDate(data_.data)}
                </span>
              </div>
              <div style={{ fontSize: 11, marginTop: 2 }}>
                {data_.unidade.predio_nome} · {data_.unidade.regiao_nome}
                {data_.unidade.segmento_nome && ` · ${data_.unidade.segmento_nome}`}
              </div>
            </div>

            {/* Pb */}
            <ExplainRow
              label="Preço Base"
              value={formatBRL(data_.pb)}
              bold
            />

            {/* Fatores a priori */}
            <ExplainGroupTitle>Fatores a priori</ExplainGroupTitle>
            {data_.fatores_priori.map((f) => (
              <ExplainFatorBlock key={f.tipo} fator={f} base={data_.pb} />
            ))}

            {/* Pi */}
            <ExplainRow
              label="Preço Inicial"
              value={formatBRL(data_.pi)}
              hint={`pb ${formatBRLDelta(data_.pi - data_.pb)}`}
              bold
            />

            {/* Fatores a posteriori */}
            <ExplainGroupTitle>Fatores a posteriori</ExplainGroupTitle>
            {data_.fatores_posteriori.map((f) => (
              <ExplainFatorBlock key={f.tipo} fator={f} base={data_.pi} />
            ))}

            {/* d */}
            <ExplainRow
              label="Preço Final"
              value={formatBRL(data_.d)}
              hint={`pi ${formatBRLDelta(data_.d - data_.pi)}`}
              bold
              highlight
            />
          </div>
        )}
      </div>
    </div>
  );
}

function ExplainRow({
  label,
  value,
  hint,
  bold,
  highlight,
}: {
  label: string;
  value: string;
  hint?: string;
  bold?: boolean;
  highlight?: boolean;
}) {
  return (
    <div
      style={{
        display: "flex",
        alignItems: "baseline",
        gap: 8,
        padding: "8px 10px",
        background: highlight ? "#eef2ff" : "#f8fafc",
        border: highlight ? "1px solid #c7d2fe" : "1px solid #e2e8f0",
        borderRadius: 6,
      }}
    >
      <span style={{ fontSize: 12, color: highlight ? "#4338ca" : "#475569", fontWeight: bold ? 600 : 500 }}>
        {label}
      </span>
      {hint && (
        <span style={{ fontSize: 10, color: "#94a3b8", fontVariantNumeric: "tabular-nums" }}>
          {hint}
        </span>
      )}
      <span
        style={{
          marginLeft: "auto",
          fontSize: 13,
          fontWeight: 600,
          color: highlight ? "#4338ca" : "#1e293b",
          fontVariantNumeric: "tabular-nums",
        }}
      >
        {value}
      </span>
    </div>
  );
}

function ExplainGroupTitle({ children }: { children: React.ReactNode }) {
  return (
    <div
      style={{
        fontSize: 10,
        fontWeight: 600,
        color: "#64748b",
        letterSpacing: 0.4,
        textTransform: "uppercase",
        marginTop: 4,
      }}
    >
      {children}
    </div>
  );
}

function ExplainFatorBlock({ fator, base }: { fator: ExplainFator; base: number }) {
  const sign = fator.ajuste_pct > 0 ? "+" : "";
  const color = fator.ajuste_pct === 0 ? "#94a3b8" : fator.ajuste_pct > 0 ? "#15803d" : "#b91c1c";
  const valor_rs = base * fator.ajuste_pct;
  return (
    <div
      style={{
        display: "flex",
        flexDirection: "column",
        gap: 4,
        padding: "8px 10px",
        background: "#ffffff",
        border: "1px solid #f1f5f9",
        borderRadius: 6,
      }}
    >
      <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
        <span style={{ fontSize: 12, color: "#1e293b", fontWeight: 500 }}>{fator.label}</span>
        <span
          style={{
            marginLeft: "auto",
            display: "flex",
            flexDirection: "column",
            alignItems: "flex-end",
            gap: 1,
          }}
        >
          <span
            style={{
              fontSize: 12,
              fontWeight: 600,
              color,
              fontVariantNumeric: "tabular-nums",
              lineHeight: 1.2,
            }}
          >
            {sign}
            {(fator.ajuste_pct * 100).toFixed(1)}%
          </span>
          {fator.ajuste_pct !== 0 && (
            <span
              style={{
                fontSize: 10,
                fontWeight: 500,
                color,
                opacity: 0.75,
                fontVariantNumeric: "tabular-nums",
                lineHeight: 1.2,
              }}
            >
              {formatBRLDelta(valor_rs)}
            </span>
          )}
        </span>
      </div>
      {fator.regras.length > 0 && (
        <div style={{ display: "flex", flexDirection: "column", gap: 2, marginTop: 2 }}>
          {fator.regras.map((r) => (
            <div
              key={r.regra_id}
              style={{
                display: "flex",
                alignItems: "center",
                gap: 6,
                fontSize: 11,
                color: "#64748b",
              }}
            >
              <span style={{ color: "#cbd5e1" }}>↳</span>
              {fator.link_crud ? (
                <a
                  href={fator.link_crud}
                  style={{
                    color: "#4f46e5",
                    textDecoration: "none",
                    flex: 1,
                  }}
                  onMouseEnter={(e) => (e.currentTarget.style.textDecoration = "underline")}
                  onMouseLeave={(e) => (e.currentTarget.style.textDecoration = "none")}
                >
                  {r.label}
                </a>
              ) : (
                <span style={{ flex: 1 }}>{r.label}</span>
              )}
              {fator.regras.length > 1 && (
                <span style={{ fontVariantNumeric: "tabular-nums", color: "#94a3b8" }}>
                  {r.ajuste_pct > 0 ? "+" : ""}
                  {(r.ajuste_pct * 100).toFixed(1)}%
                  {r.ajuste_pct !== 0 && (
                    <span style={{ marginLeft: 4, opacity: 0.7 }}>
                      ({formatBRLDelta(base * r.ajuste_pct)})
                    </span>
                  )}
                </span>
              )}
            </div>
          ))}
        </div>
      )}
      {fator.regras.length === 0 && (
        <div style={{ fontSize: 11, color: "#cbd5e1", fontStyle: "italic" }}>
          {fator.nota ?? "nenhuma regra ativa"}
        </div>
      )}
    </div>
  );
}

function formatBRL(v: number): string {
  return v.toLocaleString("pt-BR", {
    style: "currency",
    currency: "BRL",
    maximumFractionDigits: 2,
  });
}

function formatBRLDelta(v: number): string {
  const sign = v >= 0 ? "+" : "−";
  const abs = Math.abs(v).toLocaleString("pt-BR", {
    style: "currency",
    currency: "BRL",
    maximumFractionDigits: 2,
  });
  return `${sign}${abs}`;
}
