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

        <Chip icon={<IconMore />} value="" width={220}>
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
            />
          )}
        </Chip>
      </div>

      {/* Matriz */}
      <div style={{ flex: 1, overflow: "auto", padding: "8px 0 12px" }}>
        {matrix && <MatrixTable matrix={matrix} />}
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
            window.setTimeout(() => setPublishResult(null), 8000);
          }}
        />
      )}

      {/* Toast de resultado */}
      {publishResult && (
        <PublishToast result={publishResult} onClose={() => setPublishResult(null)} />
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

type PublishResult = {
  ok: boolean;
  modo: string;
  duration_ms: number;
  total_precos: number;
  sucessos: number;
  falhas: number;
  impacto_total: number;
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

function PublishToast({ result, onClose }: { result: PublishResult; onClose: () => void }) {
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
          <span style={{ color: "#b45309" }}>· {result.falhas} falharam</span>
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
}: {
  pageSize: number;
  onPageSizeChange: (n: number) => void;
  onFakeOcupacao: () => void;
}) {
  const [fakeState, setFakeState] = useState<"idle" | "running" | "ok" | "error">("idle");
  const [fakeMsg, setFakeMsg] = useState("");

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

function MatrixTable({ matrix }: { matrix: Matrix }) {
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
              return (
                <td
                  key={i}
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

