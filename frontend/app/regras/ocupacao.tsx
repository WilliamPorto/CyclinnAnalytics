"use client";

import { useCallback, useEffect, useMemo, useState } from "react";

type Banda = {
  ocupacao_min_pct: number;
  ocupacao_max_pct: number;
  ajuste_pct: number;
};

type Bucket = {
  janela_dias: number;
  bandas: Banda[];
  ativo: boolean;
};

function fmtPct(v: number): string {
  if (Math.abs(v) < 1e-4) return "0%";
  const p = v * 100;
  return `${p > 0 ? "+" : ""}${Number.isInteger(p) ? p.toFixed(0) : p.toFixed(1)}%`;
}

function fmtPctSimples(v: number): string {
  const p = v * 100;
  return Number.isInteger(p) ? `${p.toFixed(0)}%` : `${p.toFixed(1)}%`;
}

function labelBucket(j: number): string {
  if (j === 0) return "no dia";
  if (j === 1) return "1 dia";
  return `${j} dias`;
}

// Cor pastel pra valor do ajuste
function corAjuste(v: number): string {
  if (Math.abs(v) < 0.001) return "#f8fafc";
  if (v > 0) {
    const t = Math.min(1, v / 0.5);
    const r = Math.round(240 - (240 - 134) * t);
    const g = Math.round(253 - (253 - 239) * t);
    const b = Math.round(244 - (244 - 172) * t);
    return `rgb(${r},${g},${b})`;
  }
  const t = Math.min(1, -v / 0.3);
  const r = Math.round(254 - (254 - 252) * t);
  const g = Math.round(242 - (242 - 165) * t);
  const b = Math.round(242 - (242 - 165) * t);
  return `rgb(${r},${g},${b})`;
}

// ============================================================
// Utilities: bucket ↔ (limites + ajustes)
// ============================================================

type Shape = {
  janela_dias: number;
  limites: number[]; // entre 0 e 1 (exclusivo), ordenados
  ajustes: number[]; // len = limites.len + 1
};

function bucketToShape(b: Bucket): Shape {
  const bandas = [...b.bandas].sort(
    (a, c) => a.ocupacao_min_pct - c.ocupacao_min_pct
  );
  const limites = bandas.slice(0, -1).map((x) => x.ocupacao_max_pct);
  const ajustes = bandas.map((x) => x.ajuste_pct);
  return { janela_dias: b.janela_dias, limites, ajustes };
}

function shapeToBandas(s: Shape): Banda[] {
  const pontos = [0, ...s.limites, 1];
  const bandas: Banda[] = [];
  for (let i = 0; i < s.ajustes.length; i++) {
    bandas.push({
      ocupacao_min_pct: pontos[i],
      ocupacao_max_pct: pontos[i + 1],
      ajuste_pct: s.ajustes[i],
    });
  }
  return bandas;
}

// Adiciona um limite na posição pos (0-1 exclusivos), mantendo o ajuste igual
// em ambos os lados inicialmente.
function adicionarLimite(s: Shape, pos: number): Shape | null {
  if (pos <= 0 || pos >= 1) return null;
  if (s.limites.some((l) => Math.abs(l - pos) < 1e-4)) return null; // duplicata
  // Encontra em qual banda cai (para duplicar o ajuste atual)
  let idx = 0;
  for (let i = 0; i < s.limites.length; i++) {
    if (pos < s.limites[i]) break;
    idx = i + 1;
  }
  const novosLimites = [...s.limites, pos].sort((a, b) => a - b);
  const novoIdx = novosLimites.indexOf(pos);
  const novosAjustes = [...s.ajustes];
  // Inserir na posição novoIdx+1 um ajuste igual ao da direita (ou esquerda se não houver)
  novosAjustes.splice(novoIdx + 1, 0, s.ajustes[idx]);
  return { ...s, limites: novosLimites, ajustes: novosAjustes };
}

// Remove o limite no índice idx; regra combinada: ajuste da DIREITA vence
function removerLimite(s: Shape, idx: number): Shape {
  if (idx < 0 || idx >= s.limites.length) return s;
  const novosLimites = s.limites.filter((_, i) => i !== idx);
  // Remove o ajuste da esquerda (idx); o da direita (idx+1) permanece
  const novosAjustes = s.ajustes.filter((_, i) => i !== idx);
  return { ...s, limites: novosLimites, ajustes: novosAjustes };
}

function moverLimite(s: Shape, idx: number, novaPos: number): Shape | null {
  if (idx < 0 || idx >= s.limites.length) return null;
  if (novaPos <= 0 || novaPos >= 1) return null;
  const esq = idx > 0 ? s.limites[idx - 1] : 0;
  const dir = idx < s.limites.length - 1 ? s.limites[idx + 1] : 1;
  if (novaPos <= esq || novaPos >= dir) return null;
  const novos = [...s.limites];
  novos[idx] = novaPos;
  return { ...s, limites: novos };
}

function setAjuste(s: Shape, idx: number, v: number): Shape {
  const novos = [...s.ajustes];
  novos[idx] = v;
  return { ...s, ajustes: novos };
}

// ============================================================
// Componente principal
// ============================================================

export default function OcupacaoTab() {
  const [buckets, setBuckets] = useState<Bucket[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [expanded, setExpanded] = useState<number | null>(null);
  const [adding, setAdding] = useState(false);
  const [pendingChanges, setPendingChanges] = useState(false);
  const [rebuildState, setRebuildState] = useState<"idle" | "running" | "ok" | "error">("idle");
  const [rebuildMsg, setRebuildMsg] = useState("");

  const fetchBuckets = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const r = await fetch("/api/regras/ocupacao-portfolio");
      if (!r.ok) throw new Error(`HTTP ${r.status}`);
      const d = await r.json();
      setBuckets(d.buckets ?? []);
    } catch (e) {
      setError(String(e));
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchBuckets();
  }, [fetchBuckets]);

  const saveBucket = useCallback(
    async (janelaOriginal: number | null, shape: Shape) => {
      const isNew = janelaOriginal === null;
      const url = isNew
        ? "/api/regras/ocupacao-portfolio/bucket"
        : `/api/regras/ocupacao-portfolio/bucket/${janelaOriginal}`;
      const res = await fetch(url, {
        method: isNew ? "POST" : "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(shape),
      });
      if (!res.ok) {
        const err = await res.json().catch(() => ({}));
        throw new Error(err.detail ?? `HTTP ${res.status}`);
      }
      setPendingChanges(true);
      setExpanded(null);
      setAdding(false);
      await fetchBuckets();
    },
    [fetchBuckets]
  );

  const toggleAtivo = async (b: Bucket) => {
    const r = await fetch(`/api/regras/ocupacao-portfolio/bucket/${b.janela_dias}/ativo`, {
      method: "PATCH",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ ativo: !b.ativo }),
    });
    if (r.ok) {
      setPendingChanges(true);
      fetchBuckets();
    }
  };

  const deletarBucket = async (b: Bucket) => {
    if (!confirm(`Apagar bucket "${labelBucket(b.janela_dias)}"?`)) return;
    const r = await fetch(`/api/regras/ocupacao-portfolio/bucket/${b.janela_dias}`, {
      method: "DELETE",
    });
    if (r.ok) {
      setPendingChanges(true);
      fetchBuckets();
    }
  };

  const rebuild = async () => {
    setRebuildState("running");
    setRebuildMsg("");
    try {
      const r = await fetch("/api/regras/rebuild-simulador", { method: "POST" });
      const d = await r.json();
      if (!r.ok) throw new Error(d.detail ?? `HTTP ${r.status}`);
      setRebuildState("ok");
      setRebuildMsg(`feito em ${d.duration_ms} ms`);
      setPendingChanges(false);
    } catch (e) {
      setRebuildState("error");
      setRebuildMsg(String(e));
    }
  };

  return (
    <div style={{ height: "100%", display: "flex", flexDirection: "column", overflow: "hidden" }}>
      {/* Toolbar */}
      <div
        style={{
          display: "flex",
          alignItems: "center",
          gap: 10,
          padding: "10px 16px",
          background: "#f8fafc",
          borderBottom: "1px solid #e2e8f0",
          flex: "0 0 auto",
        }}
      >
        <button onClick={() => setAdding(true)} style={btnPrimary}>+ novo bucket</button>
        <div style={{ flex: 1 }} />
        {pendingChanges && (
          <span style={{ color: "#b45309", fontSize: 12, fontWeight: 500 }}>
            ⚠ alterações não aplicadas ao simulador
          </span>
        )}
        <button
          onClick={rebuild}
          disabled={rebuildState === "running"}
          style={{
            ...btnSecondary,
            background: pendingChanges ? "#f59e0b" : "#ffffff",
            color: pendingChanges ? "#ffffff" : "#475569",
            borderColor: pendingChanges ? "#f59e0b" : "#cbd5e1",
            fontWeight: 600,
          }}
        >
          {rebuildState === "running" ? "Reconstruindo…" : "Reconstruir simulador"}
        </button>
        {rebuildState === "ok" && <span style={{ color: "#15803d", fontSize: 12 }}>✓ {rebuildMsg}</span>}
        {rebuildState === "error" && <span style={{ color: "#dc2626", fontSize: 12 }}>✕ {rebuildMsg}</span>}
      </div>

      <div style={{ flex: 1, overflow: "auto", padding: 20 }}>
        <div style={{ maxWidth: 900, margin: "0 auto" }}>
          {loading && <div style={{ color: "#64748b" }}>carregando…</div>}
          {error && <div style={{ color: "#dc2626" }}>{error}</div>}

          <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
            {buckets.map((b) => (
              <BucketCard
                key={b.janela_dias}
                bucket={b}
                expanded={expanded === b.janela_dias}
                onToggleExpand={() =>
                  setExpanded(expanded === b.janela_dias ? null : b.janela_dias)
                }
                onSave={(shape) => saveBucket(b.janela_dias, shape)}
                onToggleAtivo={() => toggleAtivo(b)}
                onDelete={() => deletarBucket(b)}
              />
            ))}

            {adding && (
              <BucketCard
                bucket={{ janela_dias: 0, bandas: [{ ocupacao_min_pct: 0, ocupacao_max_pct: 1, ajuste_pct: 0 }], ativo: true }}
                expanded
                isNew
                onToggleExpand={() => setAdding(false)}
                onSave={(shape) => saveBucket(null, shape)}
                onToggleAtivo={() => {}}
                onDelete={() => setAdding(false)}
              />
            )}
          </div>

          <div style={{ marginTop: 24, fontSize: 11, color: "#64748b", lineHeight: 1.5 }}>
            <strong>Como funciona:</strong> para cada unidade × dia, o motor determina o
            bucket (pela antecedência), pega a ocupação real do portfólio da região, e
            aplica a banda correspondente. Bandas cobrem 0–100% sem buracos; remover um
            limite funde duas bandas mantendo o valor da direita.
          </div>
        </div>
      </div>
    </div>
  );
}

// ============================================================
// BucketCard
// ============================================================

function BucketCard({
  bucket,
  expanded,
  isNew = false,
  onToggleExpand,
  onSave,
  onToggleAtivo,
  onDelete,
}: {
  bucket: Bucket;
  expanded: boolean;
  isNew?: boolean;
  onToggleExpand: () => void;
  onSave: (shape: Shape) => Promise<void>;
  onToggleAtivo: () => void;
  onDelete: () => void;
}) {
  const [shape, setShape] = useState<Shape>(bucketToShape(bucket));
  const [saving, setSaving] = useState(false);
  const [err, setErr] = useState<string | null>(null);
  const [novoLimiteInput, setNovoLimiteInput] = useState("");

  useEffect(() => {
    setShape(bucketToShape(bucket));
    setErr(null);
  }, [bucket, expanded]);

  const submit = async () => {
    setSaving(true);
    setErr(null);
    try {
      await onSave(shape);
    } catch (e) {
      setErr(String(e));
    } finally {
      setSaving(false);
    }
  };

  const addLimite = () => {
    const raw = parseFloat(novoLimiteInput.replace(",", "."));
    if (isNaN(raw)) return;
    const pos = raw / 100;
    const next = adicionarLimite(shape, pos);
    if (!next) {
      setErr("Limite inválido: precisa estar em (0, 100%) e não duplicar");
      return;
    }
    setErr(null);
    setShape(next);
    setNovoLimiteInput("");
  };

  const nBandas = shape.ajustes.length;
  const minAj = Math.min(...shape.ajustes);
  const maxAj = Math.max(...shape.ajustes);
  const resumo = nBandas === 1
    ? `1 banda · ${fmtPct(shape.ajustes[0])}`
    : `${nBandas} bandas · ${fmtPct(minAj)} a ${fmtPct(maxAj)}`;

  return (
    <div
      style={{
        background: "#ffffff",
        border: "1px solid #e2e8f0",
        borderRadius: 6,
        opacity: bucket.ativo ? 1 : 0.55,
      }}
    >
      <div
        style={{
          padding: "10px 14px",
          display: "flex",
          alignItems: "center",
          gap: 10,
          cursor: "pointer",
        }}
        onClick={onToggleExpand}
      >
        <span style={{ fontSize: 12, color: "#64748b", minWidth: 14 }}>
          {expanded ? "▾" : "▸"}
        </span>
        <div style={{ flex: "0 0 140px", fontWeight: 600, fontSize: 14, color: "#0f172a" }}>
          {isNew ? "novo bucket" : labelBucket(bucket.janela_dias)}
        </div>
        <div style={{ flex: 1, fontSize: 12, color: "#475569" }}>
          {isNew ? "preencha abaixo" : resumo}
        </div>
        {/* Mini step function */}
        {!isNew && !expanded && (
          <div style={{ flex: "0 0 220px" }}>
            <StepChart shape={bucketToShape(bucket)} height={24} compact />
          </div>
        )}
        {!isNew && (
          <>
            <button
              onClick={(e) => {
                e.stopPropagation();
                onToggleAtivo();
              }}
              style={{ ...btnSecondary, fontSize: 11, padding: "3px 10px" }}
            >
              {bucket.ativo ? "desativar" : "reativar"}
            </button>
          </>
        )}
      </div>

      {expanded && (
        <div style={{ borderTop: "1px solid #f1f5f9", padding: "14px 18px", background: "#fafafa" }}>
          {/* Janela (editável) */}
          <div style={{ display: "grid", gridTemplateColumns: "180px 1fr", gap: 14, marginBottom: 14 }}>
            <Field label="dias até o check-in">
              <input
                type="number"
                min={0}
                max={365}
                value={shape.janela_dias}
                onChange={(e) =>
                  setShape({ ...shape, janela_dias: Number(e.target.value) })
                }
                style={inp}
              />
            </Field>
            <Field label="pré-visualização">
              <div style={{ background: "#ffffff", border: "1px solid #e2e8f0", borderRadius: 6, padding: 8 }}>
                <StepChart shape={shape} height={60} />
              </div>
            </Field>
          </div>

          {/* Lista de bandas */}
          <div style={{ fontSize: 11, letterSpacing: 0.5, color: "#64748b", fontWeight: 700, textTransform: "uppercase", marginBottom: 6 }}>
            Bandas (0 – 100%)
          </div>
          <div style={{ display: "flex", flexDirection: "column", gap: 4 }}>
            {shape.ajustes.map((adj, i) => {
              const mn = i === 0 ? 0 : shape.limites[i - 1];
              const mx = i === shape.ajustes.length - 1 ? 1 : shape.limites[i];
              return (
                <div key={i} style={{ display: "flex", alignItems: "center", gap: 8, fontSize: 13 }}>
                  <div
                    style={{
                      flex: "0 0 150px",
                      fontVariantNumeric: "tabular-nums",
                      color: "#475569",
                    }}
                  >
                    {fmtPctSimples(mn)} – {fmtPctSimples(mx)}
                  </div>
                  <div style={{ flex: "0 0 110px", display: "flex", alignItems: "center", gap: 6 }}>
                    <input
                      type="number"
                      step={1}
                      value={Math.round(adj * 100)}
                      onChange={(e) => setShape(setAjuste(shape, i, Number(e.target.value) / 100))}
                      style={{
                        ...inp,
                        width: 72,
                        textAlign: "right",
                        background: corAjuste(adj),
                      }}
                    />
                    <span style={{ fontSize: 12, color: "#64748b" }}>%</span>
                  </div>
                  {i < shape.ajustes.length - 1 && (
                    <div style={{ flex: 1, display: "flex", alignItems: "center", gap: 6, color: "#94a3b8", fontSize: 11 }}>
                      <span>→ limite em</span>
                      <input
                        type="number"
                        step={1}
                        min={1}
                        max={99}
                        value={Math.round(shape.limites[i] * 100)}
                        onChange={(e) => {
                          const n = moverLimite(shape, i, Number(e.target.value) / 100);
                          if (n) setShape(n);
                        }}
                        style={{ ...inp, width: 62, textAlign: "right", padding: "4px 6px", fontSize: 12 }}
                      />
                      <span>%</span>
                      <button
                        onClick={() => setShape(removerLimite(shape, i))}
                        title="remover limite (funde com a banda à direita)"
                        style={{
                          background: "transparent",
                          border: "1px solid #fca5a5",
                          color: "#b91c1c",
                          borderRadius: 4,
                          width: 22,
                          height: 22,
                          padding: 0,
                          cursor: "pointer",
                          fontSize: 12,
                        }}
                      >
                        ×
                      </button>
                    </div>
                  )}
                </div>
              );
            })}
          </div>

          {/* Adicionar novo limite */}
          <div style={{ display: "flex", alignItems: "center", gap: 8, marginTop: 14, padding: "8px 10px", background: "#eff6ff", borderRadius: 6, border: "1px dashed #bfdbfe" }}>
            <span style={{ fontSize: 12, color: "#1d4ed8", fontWeight: 500 }}>
              + adicionar limite em
            </span>
            <input
              type="number"
              step={1}
              min={1}
              max={99}
              placeholder="ex: 45"
              value={novoLimiteInput}
              onChange={(e) => setNovoLimiteInput(e.target.value)}
              onKeyDown={(e) => {
                if (e.key === "Enter") addLimite();
              }}
              style={{ ...inp, width: 72, textAlign: "right" }}
            />
            <span style={{ fontSize: 12, color: "#64748b" }}>%</span>
            <button onClick={addLimite} style={{ ...btnPrimary, fontSize: 11, padding: "4px 12px" }}>
              adicionar
            </button>
          </div>

          {err && <div style={{ color: "#dc2626", fontSize: 12, marginTop: 10 }}>{err}</div>}

          <div style={{ display: "flex", gap: 8, justifyContent: "space-between", marginTop: 14 }}>
            <div>
              {!isNew && (
                <button onClick={onDelete} style={{ ...btnSecondary, color: "#b91c1c", borderColor: "#fca5a5" }}>
                  apagar bucket
                </button>
              )}
            </div>
            <div style={{ display: "flex", gap: 8 }}>
              <button onClick={onToggleExpand} style={cancelBtn}>Cancelar</button>
              <button onClick={submit} disabled={saving} style={btnPrimary}>
                {saving ? "Salvando…" : isNew ? "Criar bucket" : "Salvar"}
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

// ============================================================
// Mini step function chart (SVG)
// ============================================================

function StepChart({
  shape,
  height = 60,
  compact = false,
}: {
  shape: Shape;
  height?: number;
  compact?: boolean;
}) {
  const pontos = [0, ...shape.limites, 1];
  const maxAbs = Math.max(0.1, ...shape.ajustes.map((a) => Math.abs(a)));
  const yRange = maxAbs * 1.2;

  const widthPx = 800;
  const yMid = height / 2;
  const yAt = (v: number) => yMid - (v / yRange) * (height / 2 - 4);
  const xAt = (p: number) => p * widthPx;

  return (
    <svg
      viewBox={`0 0 ${widthPx} ${height}`}
      preserveAspectRatio="none"
      style={{ width: "100%", height: height, display: "block" }}
    >
      {/* retângulos coloridos pelas bandas */}
      {shape.ajustes.map((adj, i) => {
        const x = xAt(pontos[i]);
        const w = xAt(pontos[i + 1]) - x;
        return <rect key={`bg-${i}`} x={x} y={0} width={w} height={height} fill={corAjuste(adj)} />;
      })}

      {/* eixo zero */}
      <line x1={0} y1={yMid} x2={widthPx} y2={yMid} stroke="#cbd5e1" strokeWidth={1} strokeDasharray="2 2" />

      {/* linha dos degraus */}
      {shape.ajustes.map((adj, i) => {
        const x1 = xAt(pontos[i]);
        const x2 = xAt(pontos[i + 1]);
        const y = yAt(adj);
        return <line key={`step-${i}`} x1={x1} y1={y} x2={x2} y2={y} stroke="#0f172a" strokeWidth={2} />;
      })}
      {/* conexões verticais entre degraus */}
      {shape.limites.map((lim, i) => {
        const x = xAt(lim);
        const y1 = yAt(shape.ajustes[i]);
        const y2 = yAt(shape.ajustes[i + 1]);
        return <line key={`conn-${i}`} x1={x} y1={y1} x2={x} y2={y2} stroke="#0f172a" strokeWidth={2} />;
      })}

      {/* labels dos ajustes (só em modo não-compact) */}
      {!compact &&
        shape.ajustes.map((adj, i) => {
          const x = (xAt(pontos[i]) + xAt(pontos[i + 1])) / 2;
          const y = yAt(adj);
          return (
            <text
              key={`lbl-${i}`}
              x={x}
              y={y - 5}
              fontSize={11}
              fontWeight={600}
              fill="#0f172a"
              textAnchor="middle"
            >
              {fmtPct(adj)}
            </text>
          );
        })}

      {/* labels dos limites no eixo x (só em modo não-compact) */}
      {!compact &&
        shape.limites.map((lim, i) => {
          const x = xAt(lim);
          return (
            <text
              key={`xlbl-${i}`}
              x={x}
              y={height - 2}
              fontSize={9}
              fill="#64748b"
              textAnchor="middle"
            >
              {Math.round(lim * 100)}%
            </text>
          );
        })}
    </svg>
  );
}

// ============================================================

function Field({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <label style={{ display: "flex", flexDirection: "column", gap: 3 }}>
      <span style={{ fontSize: 10, color: "#64748b", letterSpacing: 0.3, fontWeight: 600, textTransform: "uppercase" }}>
        {label}
      </span>
      {children}
    </label>
  );
}

const btnPrimary: React.CSSProperties = {
  background: "#1d4ed8",
  color: "white",
  border: 0,
  padding: "6px 14px",
  borderRadius: 5,
  fontSize: 13,
  fontWeight: 600,
  cursor: "pointer",
  fontFamily: "inherit",
};
const btnSecondary: React.CSSProperties = {
  background: "#ffffff",
  color: "#475569",
  border: "1px solid #cbd5e1",
  padding: "6px 12px",
  borderRadius: 5,
  fontSize: 12,
  cursor: "pointer",
  fontFamily: "inherit",
};
const cancelBtn: React.CSSProperties = {
  background: "#ffffff",
  color: "#475569",
  border: "1px solid #cbd5e1",
  padding: "5px 14px",
  borderRadius: 5,
  fontSize: 12,
  cursor: "pointer",
  fontFamily: "inherit",
};
const inp: React.CSSProperties = {
  padding: "6px 10px",
  border: "1px solid #cbd5e1",
  borderRadius: 4,
  fontSize: 13,
  fontFamily: "inherit",
  width: "100%",
};
