"use client";

import { useCallback, useEffect, useState } from "react";
import { apiFetch } from "../lib/api";

type Publicacao = {
  publicacao_id: number;
  timestamp: string;
  usuario: string;
  modo: string;
  tipo: string;
  escopo: string;
  regiao_id: number | null;
  regiao_nome: string | null;
  data_referencia: string;
  periodo_ini: string;
  periodo_fim: string;
  total_precos: number;
  sucessos: number;
  falhas: number;
  impacto_total: number;
  duration_ms: number;
  referencia_id: number | null;
  observacoes: string | null;
};

type DiffStats = {
  n_inalterados: number;
  n_aumentaram: number;
  n_diminuiram: number;
  n_novos: number;
  n_removidos: number;
  impacto_total: number;
};

type DiffTopItem = {
  unidade_id: number;
  unidade_label: string;
  data: string;
  valor_anterior: number | null;
  valor_atual: number | null;
  delta: number | null;
};

type DiffResponse = {
  publicacao_id: number;
  vs: number | null;
  stats: DiffStats;
  top: DiffTopItem[];
  matriz: {
    columns: string[];
    rows: {
      unidade_id: number;
      label: string;
      deltas: (number | null)[];
      valores: (number | null)[];
    }[];
  };
};

const MONTHS_PT = ["jan", "fev", "mar", "abr", "mai", "jun", "jul", "ago", "set", "out", "nov", "dez"];

function fmtDateBR(iso: string): string {
  if (!iso) return "—";
  const [y, m, d] = iso.split("-").map(Number);
  return `${String(d).padStart(2, "0")}/${MONTHS_PT[m - 1]}`;
}

function fmtTimestamp(iso: string): string {
  return new Date(iso).toLocaleString("pt-BR", {
    day: "2-digit",
    month: "2-digit",
    year: "numeric",
    hour: "2-digit",
    minute: "2-digit",
  });
}

function fmtBRL(v: number, withSign = false): string {
  const sign = withSign && v >= 0 ? "+" : v < 0 ? "−" : "";
  const abs = Math.abs(v).toLocaleString("pt-BR", {
    style: "currency",
    currency: "BRL",
    maximumFractionDigits: 0,
  });
  return `${sign}${abs}`;
}

export default function PublicacoesPage() {
  const [items, setItems] = useState<Publicacao[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [selected, setSelected] = useState<number | null>(null);
  const [refreshKey, setRefreshKey] = useState(0);

  useEffect(() => {
    setLoading(true);
    setError(null);
    fetch("/api/publicacoes?page=1&page_size=100")
      .then(async (r) => {
        if (!r.ok) throw new Error(`HTTP ${r.status}`);
        return r.json();
      })
      .then((d) => setItems(d.items ?? []))
      .catch((e) => setError(String(e)))
      .finally(() => setLoading(false));
  }, [refreshKey]);

  return (
    <main style={{ height: "100%", display: "flex", overflow: "hidden" }}>
      {/* Lista */}
      <div
        style={{
          width: 360,
          minWidth: 360,
          background: "#ffffff",
          borderRight: "1px solid #e2e8f0",
          display: "flex",
          flexDirection: "column",
          overflow: "hidden",
        }}
      >
        <div
          style={{
            padding: "12px 16px",
            borderBottom: "1px solid #f1f5f9",
            display: "flex",
            alignItems: "center",
            gap: 8,
          }}
        >
          <span style={{ fontSize: 13, fontWeight: 600, color: "#1e293b", letterSpacing: -0.1 }}>
            Publicações
          </span>
          <span style={{ fontSize: 11, color: "#94a3b8" }}>
            {items.length}
          </span>
        </div>
        <div style={{ flex: 1, overflow: "auto" }}>
          {loading && (
            <div style={{ padding: 16, color: "#94a3b8", fontSize: 12 }}>carregando…</div>
          )}
          {error && (
            <div style={{ padding: 16, color: "#dc2626", fontSize: 12 }}>{error}</div>
          )}
          {!loading && !error && items.length === 0 && (
            <div style={{ padding: 24, color: "#94a3b8", fontSize: 12, textAlign: "center" }}>
              Nenhuma publicação ainda. Vá em <strong>Precificação → Preço Final → Publicar no Guesty</strong> pra criar a primeira.
            </div>
          )}
          {items.map((p) => (
            <PublicacaoCard
              key={p.publicacao_id}
              pub={p}
              selected={selected === p.publicacao_id}
              onClick={() => setSelected(p.publicacao_id)}
            />
          ))}
        </div>
      </div>

      {/* Detalhes */}
      <div style={{ flex: 1, overflow: "auto", padding: "16px 24px" }}>
        {selected === null ? (
          <div style={{ color: "#94a3b8", fontSize: 13, padding: 40, textAlign: "center" }}>
            Selecione uma publicação à esquerda pra ver detalhes e diff.
          </div>
        ) : (
          <DetalhesPublicacao
            publicacaoId={selected}
            onRollback={() => setRefreshKey((k) => k + 1)}
          />
        )}
      </div>
    </main>
  );
}

function PublicacaoCard({
  pub,
  selected,
  onClick,
}: {
  pub: Publicacao;
  selected: boolean;
  onClick: () => void;
}) {
  const isRollback = pub.tipo === "rollback";
  return (
    <button
      onClick={onClick}
      style={{
        width: "100%",
        textAlign: "left",
        padding: "10px 14px",
        background: selected ? "#eef2ff" : "transparent",
        border: 0,
        borderLeft: selected ? "3px solid #4f46e5" : "3px solid transparent",
        borderBottom: "1px solid #f1f5f9",
        cursor: "pointer",
        fontFamily: "inherit",
        display: "flex",
        flexDirection: "column",
        gap: 3,
      }}
      onMouseEnter={(e) => {
        if (!selected) e.currentTarget.style.background = "#f8fafc";
      }}
      onMouseLeave={(e) => {
        if (!selected) e.currentTarget.style.background = "transparent";
      }}
    >
      <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
        <span
          style={{
            fontSize: 11,
            fontWeight: 600,
            color: selected ? "#4338ca" : "#1e293b",
            fontVariantNumeric: "tabular-nums",
          }}
        >
          #{pub.publicacao_id}
        </span>
        {isRollback && (
          <span
            style={{
              fontSize: 9,
              fontWeight: 700,
              color: "#b45309",
              background: "#fef3c7",
              padding: "1px 5px",
              borderRadius: 3,
              textTransform: "uppercase",
              letterSpacing: 0.3,
            }}
          >
            rollback
          </span>
        )}
        <span
          style={{
            fontSize: 9,
            fontWeight: 700,
            color: "#64748b",
            background: "#f1f5f9",
            padding: "1px 5px",
            borderRadius: 3,
            textTransform: "uppercase",
            letterSpacing: 0.3,
          }}
        >
          {pub.modo}
        </span>
        <span style={{ marginLeft: "auto", fontSize: 10, color: "#94a3b8" }}>
          {fmtTimestamp(pub.timestamp)}
        </span>
      </div>
      <div style={{ fontSize: 12, color: "#475569" }}>
        {fmtDateBR(pub.periodo_ini)} → {fmtDateBR(pub.periodo_fim)}
        {pub.regiao_nome && (
          <span style={{ color: "#94a3b8" }}>
            {" · "}
            {pub.regiao_nome}
          </span>
        )}
      </div>
      <div style={{ display: "flex", alignItems: "center", gap: 10, fontSize: 11, color: "#64748b" }}>
        <span>
          {pub.total_precos.toLocaleString("pt-BR")} preços
        </span>
        {pub.falhas > 0 && (
          <span style={{ color: "#b45309" }}>· {pub.falhas} falhas</span>
        )}
        <span style={{ marginLeft: "auto", color: pub.impacto_total >= 0 ? "#15803d" : "#b91c1c", fontWeight: 600 }}>
          {fmtBRL(pub.impacto_total, true)}
        </span>
      </div>
    </button>
  );
}

function DetalhesPublicacao({
  publicacaoId,
  onRollback,
}: {
  publicacaoId: number;
  onRollback: () => void;
}) {
  const [pub, setPub] = useState<(Publicacao & { anterior_id: number | null }) | null>(null);
  const [diff, setDiff] = useState<DiffResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [vsId, setVsId] = useState<number | null>(null);
  const [allPubs, setAllPubs] = useState<Publicacao[]>([]);
  const [confirmRollback, setConfirmRollback] = useState(false);

  const load = useCallback(() => {
    setLoading(true);
    setError(null);
    Promise.all([
      fetch(`/api/publicacoes/${publicacaoId}`).then((r) => r.json()),
      fetch("/api/publicacoes?page=1&page_size=100").then((r) => r.json()),
    ])
      .then(async ([detail, list]) => {
        setPub(detail);
        setAllPubs(list.items ?? []);
        const initialVs = vsId ?? detail.anterior_id;
        const qs = initialVs ? `?vs=${initialVs}` : "";
        const diffData = await fetch(`/api/publicacoes/${publicacaoId}/diff${qs}`).then((r) => r.json());
        setDiff(diffData);
      })
      .catch((e) => setError(String(e)))
      .finally(() => setLoading(false));
  }, [publicacaoId, vsId]);

  useEffect(() => {
    setVsId(null);
    setDiff(null);
    setPub(null);
  }, [publicacaoId]);

  useEffect(() => {
    load();
  }, [load]);

  if (loading || !pub) {
    return <div style={{ color: "#94a3b8", fontSize: 13 }}>carregando…</div>;
  }
  if (error) {
    return <div style={{ color: "#dc2626", fontSize: 13 }}>{error}</div>;
  }

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 16, maxWidth: 1100 }}>
      {/* Header */}
      <div style={{ display: "flex", alignItems: "flex-start", gap: 10 }}>
        <div style={{ flex: 1 }}>
          <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 4 }}>
            <span style={{ fontSize: 18, fontWeight: 600, color: "#1e293b" }}>
              Publicação #{pub.publicacao_id}
            </span>
            {pub.tipo === "rollback" && (
              <span
                style={{
                  fontSize: 10,
                  fontWeight: 700,
                  color: "#b45309",
                  background: "#fef3c7",
                  padding: "2px 7px",
                  borderRadius: 4,
                  textTransform: "uppercase",
                  letterSpacing: 0.3,
                }}
              >
                rollback de #{pub.referencia_id}
              </span>
            )}
            <span
              style={{
                fontSize: 10,
                fontWeight: 700,
                color: "#64748b",
                background: "#f1f5f9",
                padding: "2px 7px",
                borderRadius: 4,
                textTransform: "uppercase",
                letterSpacing: 0.3,
              }}
            >
              {pub.modo}
            </span>
          </div>
          <div style={{ fontSize: 12, color: "#64748b" }}>
            {fmtTimestamp(pub.timestamp)} · por <strong style={{ color: "#1e293b" }}>{pub.usuario}</strong>
            {pub.regiao_nome && <> · {pub.regiao_nome}</>}
            {pub.observacoes && <> · {pub.observacoes}</>}
          </div>
        </div>
        <button
          onClick={() => setConfirmRollback(true)}
          disabled={pub.tipo === "rollback"}
          style={{
            background: pub.tipo === "rollback" ? "#e2e8f0" : "#ffffff",
            color: pub.tipo === "rollback" ? "#94a3b8" : "#b45309",
            border: `1px solid ${pub.tipo === "rollback" ? "#e2e8f0" : "#fcd34d"}`,
            padding: "7px 14px",
            borderRadius: 6,
            fontSize: 12,
            fontWeight: 600,
            fontFamily: "inherit",
            cursor: pub.tipo === "rollback" ? "not-allowed" : "pointer",
          }}
          title={
            pub.tipo === "rollback"
              ? "Esta é uma publicação rollback — sem rollback de rollback"
              : "Reaplica os preços desta publicação"
          }
        >
          ↶ Reverter pra esta versão
        </button>
      </div>

      {/* Stats da publicação */}
      <div
        style={{
          display: "grid",
          gridTemplateColumns: "repeat(auto-fit, minmax(140px, 1fr))",
          gap: 8,
        }}
      >
        <StatCard label="Período" value={`${fmtDateBR(pub.periodo_ini)} → ${fmtDateBR(pub.periodo_fim)}`} />
        <StatCard label="Preços" value={pub.total_precos.toLocaleString("pt-BR")} />
        <StatCard
          label="Sucessos"
          value={pub.sucessos.toLocaleString("pt-BR")}
          color="#15803d"
        />
        <StatCard
          label="Falhas"
          value={pub.falhas.toLocaleString("pt-BR")}
          color={pub.falhas > 0 ? "#b45309" : "#94a3b8"}
        />
        <StatCard
          label="Impacto vs Pb"
          value={fmtBRL(pub.impacto_total, true)}
          color={pub.impacto_total >= 0 ? "#15803d" : "#b91c1c"}
        />
      </div>

      {/* Diff */}
      <div
        style={{
          display: "flex",
          alignItems: "center",
          gap: 10,
          paddingTop: 8,
          borderTop: "1px solid #f1f5f9",
        }}
      >
        <span style={{ fontSize: 13, fontWeight: 600, color: "#1e293b" }}>
          Diff
        </span>
        <span style={{ fontSize: 12, color: "#64748b" }}>
          comparando com
        </span>
        <select
          value={vsId ?? pub.anterior_id ?? ""}
          onChange={(e) => setVsId(e.target.value ? Number(e.target.value) : null)}
          disabled={allPubs.length <= 1}
          style={{
            padding: "4px 8px",
            border: "1px solid #e2e8f0",
            borderRadius: 5,
            fontSize: 12,
            fontFamily: "inherit",
            color: "#1e293b",
            background: "#ffffff",
          }}
        >
          {allPubs
            .filter((p) => p.publicacao_id !== publicacaoId)
            .map((p) => (
              <option key={p.publicacao_id} value={p.publicacao_id}>
                #{p.publicacao_id} · {fmtTimestamp(p.timestamp)}
              </option>
            ))}
        </select>
      </div>

      {diff ? <DiffSection diff={diff} /> : null}

      {confirmRollback && (
        <RollbackModal
          publicacaoId={publicacaoId}
          totalPrecos={pub.total_precos}
          impactoTotal={pub.impacto_total}
          onClose={() => setConfirmRollback(false)}
          onSuccess={() => {
            setConfirmRollback(false);
            onRollback();
            load();
          }}
        />
      )}
    </div>
  );
}

function StatCard({
  label,
  value,
  color,
}: {
  label: string;
  value: string;
  color?: string;
}) {
  return (
    <div
      style={{
        background: "#ffffff",
        border: "1px solid #e2e8f0",
        borderRadius: 6,
        padding: "8px 12px",
        display: "flex",
        flexDirection: "column",
        gap: 2,
      }}
    >
      <span
        style={{
          fontSize: 10,
          fontWeight: 500,
          color: "#64748b",
          letterSpacing: 0.2,
          textTransform: "uppercase",
        }}
      >
        {label}
      </span>
      <span
        style={{
          fontSize: 14,
          fontWeight: 600,
          color: color ?? "#1e293b",
          fontVariantNumeric: "tabular-nums",
        }}
      >
        {value}
      </span>
    </div>
  );
}

function DiffSection({ diff }: { diff: DiffResponse }) {
  if (diff.vs === null) {
    return (
      <div
        style={{
          background: "#f8fafc",
          border: "1px solid #e2e8f0",
          borderRadius: 6,
          padding: 16,
          fontSize: 12,
          color: "#64748b",
        }}
      >
        Esta é a primeira publicação — nada pra comparar.
      </div>
    );
  }

  const s = diff.stats;
  const totalMov = s.n_aumentaram + s.n_diminuiram + s.n_novos + s.n_removidos;

  // Range de delta pra colorir o heatmap
  let dMin = 0,
    dMax = 0;
  diff.matriz.rows.forEach((r) =>
    r.deltas.forEach((d) => {
      if (d !== null) {
        if (d < dMin) dMin = d;
        if (d > dMax) dMax = d;
      }
    })
  );

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 12 }}>
      {/* Stats do diff */}
      <div
        style={{
          display: "grid",
          gridTemplateColumns: "repeat(auto-fit, minmax(120px, 1fr))",
          gap: 8,
        }}
      >
        <StatCard label="Inalteradas" value={s.n_inalterados.toLocaleString("pt-BR")} color="#94a3b8" />
        <StatCard label="↑ subiram" value={s.n_aumentaram.toLocaleString("pt-BR")} color="#15803d" />
        <StatCard label="↓ caíram" value={s.n_diminuiram.toLocaleString("pt-BR")} color="#b91c1c" />
        {s.n_novos > 0 && <StatCard label="+ novos" value={s.n_novos.toLocaleString("pt-BR")} color="#4338ca" />}
        {s.n_removidos > 0 && <StatCard label="− removidos" value={s.n_removidos.toLocaleString("pt-BR")} color="#475569" />}
        <StatCard
          label="Δ impacto"
          value={fmtBRL(s.impacto_total, true)}
          color={s.impacto_total >= 0 ? "#15803d" : "#b91c1c"}
        />
      </div>

      {totalMov === 0 ? (
        <div
          style={{
            background: "#f0fdf4",
            border: "1px solid #bbf7d0",
            color: "#15803d",
            padding: 12,
            borderRadius: 6,
            fontSize: 12,
          }}
        >
          ✓ Nenhum preço mudou entre essas duas publicações.
        </div>
      ) : (
        <>
          {/* Heatmap top 50 unidades */}
          {diff.matriz.rows.length > 0 && (
            <div>
              <div
                style={{
                  fontSize: 10,
                  fontWeight: 500,
                  color: "#64748b",
                  marginBottom: 6,
                  letterSpacing: 0.2,
                  textTransform: "uppercase",
                }}
              >
                Heatmap de delta — top {diff.matriz.rows.length} unidades por |Δ|
              </div>
              <div style={{ overflow: "auto", border: "1px solid #e2e8f0", borderRadius: 6, background: "#ffffff" }}>
                <table style={{ borderCollapse: "collapse", fontSize: 11 }}>
                  <thead>
                    <tr>
                      <th style={diffTh}>Unidade</th>
                      {diff.matriz.columns.map((c) => (
                        <th key={c} style={diffTh}>
                          {fmtDateBR(c)}
                        </th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {diff.matriz.rows.map((row) => (
                      <tr key={row.unidade_id}>
                        <th style={diffFirstTd}>{row.label}</th>
                        {row.deltas.map((d, i) => {
                          const bg =
                            d === null || d === 0
                              ? "#ffffff"
                              : deltaColor(d, dMin, dMax);
                          return (
                            <td
                              key={i}
                              style={{
                                background: bg,
                                color: d === null ? "#cbd5e1" : "#1e293b",
                                padding: "4px 8px",
                                borderBottom: "1px solid #f1f5f9",
                                borderRight: "1px solid #f8fafc",
                                textAlign: "right",
                                whiteSpace: "nowrap",
                                fontVariantNumeric: "tabular-nums",
                                minWidth: 64,
                              }}
                              title={
                                d === null
                                  ? "—"
                                  : `de ${row.valores[i] !== null && d !== null ? fmtBRL((row.valores[i] as number) - d) : "?"} pra ${row.valores[i] !== null ? fmtBRL(row.valores[i] as number) : "?"}`
                              }
                            >
                              {d === null
                                ? "—"
                                : d === 0
                                ? "·"
                                : fmtBRL(d, true)}
                            </td>
                          );
                        })}
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          )}

          {/* Top deltas (lista simples) */}
          {diff.top.length > 0 && (
            <div>
              <div
                style={{
                  fontSize: 10,
                  fontWeight: 500,
                  color: "#64748b",
                  marginBottom: 6,
                  letterSpacing: 0.2,
                  textTransform: "uppercase",
                }}
              >
                Maiores deltas
              </div>
              <div style={{ border: "1px solid #e2e8f0", borderRadius: 6, background: "#ffffff", overflow: "hidden" }}>
                <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 11.5 }}>
                  <thead>
                    <tr>
                      <th style={diffTh}>Unidade</th>
                      <th style={diffTh}>Data</th>
                      <th style={{ ...diffTh, textAlign: "right" }}>Era</th>
                      <th style={{ ...diffTh, textAlign: "right" }}>Virou</th>
                      <th style={{ ...diffTh, textAlign: "right" }}>Δ</th>
                    </tr>
                  </thead>
                  <tbody>
                    {diff.top.slice(0, 10).map((t, i) => (
                      <tr key={i} style={{ borderTop: i === 0 ? 0 : "1px solid #f1f5f9" }}>
                        <td style={{ padding: "5px 10px" }}>
                          <code style={{ fontSize: 11 }}>{t.unidade_label}</code>
                        </td>
                        <td style={{ padding: "5px 10px", fontVariantNumeric: "tabular-nums" }}>
                          {fmtDateBR(t.data)}
                        </td>
                        <td style={{ padding: "5px 10px", textAlign: "right", fontVariantNumeric: "tabular-nums", color: "#64748b" }}>
                          {t.valor_anterior !== null ? fmtBRL(t.valor_anterior) : "—"}
                        </td>
                        <td style={{ padding: "5px 10px", textAlign: "right", fontVariantNumeric: "tabular-nums" }}>
                          {t.valor_atual !== null ? fmtBRL(t.valor_atual) : "—"}
                        </td>
                        <td
                          style={{
                            padding: "5px 10px",
                            textAlign: "right",
                            fontVariantNumeric: "tabular-nums",
                            fontWeight: 600,
                            color: t.delta === null ? "#94a3b8" : t.delta > 0 ? "#15803d" : "#b91c1c",
                          }}
                        >
                          {t.delta !== null ? fmtBRL(t.delta, true) : "—"}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          )}
        </>
      )}
    </div>
  );
}

function deltaColor(d: number, vmin: number, vmax: number): string {
  if (d === 0) return "#ffffff";
  if (d > 0) {
    const t = vmax === 0 ? 0 : Math.min(1, d / vmax);
    const r = Math.round(220 - (220 - 22) * t);
    const g = Math.round(252 - (252 - 163) * t);
    const b = Math.round(231 - (231 - 74) * t);
    return `rgb(${r},${g},${b})`;
  }
  const t = vmin === 0 ? 0 : Math.min(1, d / vmin);
  const r = Math.round(254 - (254 - 220) * t);
  const g = Math.round(226 - (226 - 38) * t);
  const b = Math.round(226 - (226 - 38) * t);
  return `rgb(${r},${g},${b})`;
}

function RollbackModal({
  publicacaoId,
  totalPrecos,
  impactoTotal,
  onClose,
  onSuccess,
}: {
  publicacaoId: number;
  totalPrecos: number;
  impactoTotal: number;
  onClose: () => void;
  onSuccess: () => void;
}) {
  const [submitting, setSubmitting] = useState(false);
  const [err, setErr] = useState<string | null>(null);

  const submit = async () => {
    setSubmitting(true);
    setErr(null);
    try {
      const r = await apiFetch(`/api/publicacoes/${publicacaoId}/rollback`, {
        method: "POST",
      });
      const d = await r.json();
      if (!r.ok) throw new Error(d.detail ?? `HTTP ${r.status}`);
      onSuccess();
    } catch (e) {
      setErr(String(e));
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
          width: 440,
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
          <span style={{ fontSize: 15, fontWeight: 600 }}>
            Reverter pra publicação #{publicacaoId}
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
        </div>
        <div
          style={{
            background: "#fef3c7",
            border: "1px solid #fcd34d",
            borderRadius: 6,
            padding: "10px 12px",
            marginBottom: 14,
            fontSize: 12,
            color: "#78350f",
          }}
        >
          Vai re-publicar os <strong>{totalPrecos.toLocaleString("pt-BR")} preços</strong> desta versão como uma nova publicação tipo <strong>rollback</strong>.
          {" "}A publicação atual fica preservada no histórico.
        </div>
        <div
          style={{
            background: "#f8fafc",
            border: "1px solid #e2e8f0",
            borderRadius: 6,
            padding: 12,
            marginBottom: 14,
            fontSize: 12,
            display: "flex",
            flexDirection: "column",
            gap: 4,
          }}
        >
          <div style={{ display: "flex", justifyContent: "space-between" }}>
            <span style={{ color: "#64748b" }}>Total de preços</span>
            <strong>{totalPrecos.toLocaleString("pt-BR")}</strong>
          </div>
          <div style={{ display: "flex", justifyContent: "space-between" }}>
            <span style={{ color: "#64748b" }}>Impacto original (vs Pb)</span>
            <strong style={{ color: impactoTotal >= 0 ? "#15803d" : "#b91c1c" }}>
              {fmtBRL(impactoTotal, true)}
            </strong>
          </div>
        </div>
        {err && (
          <div style={{ color: "#dc2626", fontSize: 12, marginBottom: 8 }}>{err}</div>
        )}
        <div style={{ display: "flex", justifyContent: "flex-end", gap: 8 }}>
          <button onClick={onClose} disabled={submitting} style={btnSec}>
            Cancelar
          </button>
          <button onClick={submit} disabled={submitting} style={btnPri}>
            {submitting ? "Revertendo…" : "Confirmar rollback"}
          </button>
        </div>
      </div>
    </div>
  );
}

const diffTh: React.CSSProperties = {
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

const diffFirstTd: React.CSSProperties = {
  position: "sticky",
  left: 0,
  background: "#f8fafc",
  padding: "5px 10px",
  borderRight: "1px solid #e2e8f0",
  fontWeight: 500,
  fontSize: 11,
  color: "#1e293b",
  whiteSpace: "nowrap",
  textAlign: "left",
};

const btnPri: React.CSSProperties = {
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

const btnSec: React.CSSProperties = {
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
