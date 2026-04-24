"use client";

import { useCallback, useEffect, useMemo, useState } from "react";

type Faixa = {
  lead_min_dias: number;
  lead_max_dias: number;
  por_dow: boolean;
  ajuste_uniforme: number | null;
  ajustes_dow: number[] | null;
  ativo: boolean;
};

type Gap = { lead_min_dias: number; lead_max_dias: number };

const DOWS = ["SEG", "TER", "QUA", "QUI", "SEX", "SÁB", "DOM"];
const HORIZONTE = 365;

function fmtPct(v: number | null): string {
  if (v === null || v === undefined) return "—";
  const p = v * 100;
  if (Math.abs(p) < 0.05) return "0%";
  return `${p > 0 ? "+" : ""}${Number.isInteger(p) ? p.toFixed(0) : p.toFixed(1)}%`;
}

function resumoFaixa(f: Faixa): string {
  if (f.por_dow && f.ajustes_dow) {
    const uniques = Array.from(new Set(f.ajustes_dow.map((v) => v.toFixed(2))));
    if (uniques.length === 1) return `${fmtPct(f.ajustes_dow[0])} em todos os dias`;
    const partes = f.ajustes_dow
      .map((v, i) => (Math.abs(v) < 1e-6 ? null : `${DOWS[i].toLowerCase()} ${fmtPct(v)}`))
      .filter((x): x is string => x !== null);
    return partes.length > 0 ? "varia por dia: " + partes.join(" · ") : "0% em todos os dias";
  }
  return `${fmtPct(f.ajuste_uniforme ?? 0)} (todos os dias)`;
}

// Cor do segmento na timeline — paleta pastel (verde p/ aumento, rosado p/ redução)
function segColor(ajuste: number): string {
  if (Math.abs(ajuste) < 0.001) return "#f8fafc";
  if (ajuste > 0) {
    // green-50 (#f0fdf4) → green-300 (#86efac)
    const t = Math.min(1, ajuste / 0.5);
    const r = Math.round(240 - (240 - 134) * t);
    const g = Math.round(253 - (253 - 239) * t);
    const b = Math.round(244 - (244 - 172) * t);
    return `rgb(${r},${g},${b})`;
  }
  // red-50 (#fef2f2) → red-300 (#fca5a5)
  const t = Math.min(1, -ajuste / 0.3);
  const r = Math.round(254 - (254 - 252) * t);
  const g = Math.round(242 - (242 - 165) * t);
  const b = Math.round(242 - (242 - 165) * t);
  return `rgb(${r},${g},${b})`;
}

// Texto sobre o segmento: sempre escuro, já que a paleta é toda pastel
function textOnSeg(_ajuste: number): string {
  return "#0f172a";
}

export default function AntecedenciaTab() {
  const [faixas, setFaixas] = useState<Faixa[]>([]);
  const [gaps, setGaps] = useState<Gap[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [expanded, setExpanded] = useState<string | null>(null); // key da faixa expandida
  const [adding, setAdding] = useState(false);
  const [pendingChanges, setPendingChanges] = useState(false);
  const [rebuildState, setRebuildState] = useState<"idle" | "running" | "ok" | "error">("idle");
  const [rebuildMsg, setRebuildMsg] = useState("");

  const fetchFaixas = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const r = await fetch("/api/regras/antecedencia");
      if (!r.ok) throw new Error(`HTTP ${r.status}`);
      const d = await r.json();
      setFaixas(d.faixas ?? []);
      setGaps(d.gaps ?? []);
    } catch (e) {
      setError(String(e));
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchFaixas();
  }, [fetchFaixas]);

  const saveFaixa = async (
    originalKey: { lead_min: number; lead_max: number } | null, // null = criar
    body: Omit<Faixa, "ativo">
  ) => {
    const isNew = originalKey === null;
    const url = isNew
      ? "/api/regras/antecedencia/faixa"
      : `/api/regras/antecedencia/faixa/${originalKey.lead_min}/${originalKey.lead_max}`;
    const res = await fetch(url, {
      method: isNew ? "POST" : "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });
    if (!res.ok) {
      const err = await res.json().catch(() => ({}));
      throw new Error(err.detail ?? `HTTP ${res.status}`);
    }
    setPendingChanges(true);
    setExpanded(null);
    setAdding(false);
    await fetchFaixas();
  };

  const toggleAtivo = async (f: Faixa) => {
    const res = await fetch(
      `/api/regras/antecedencia/faixa/${f.lead_min_dias}/${f.lead_max_dias}/ativo`,
      {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ ativo: !f.ativo }),
      }
    );
    if (res.ok) {
      setPendingChanges(true);
      fetchFaixas();
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
        <button
          onClick={() => {
            setAdding(true);
            setExpanded(null);
          }}
          style={btnPrimary}
        >
          + nova faixa
        </button>
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

          {/* Cards de faixas + gaps */}
          <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
            {combinarFaixasEGaps(faixas, gaps).map((item) => {
              if (item.tipo === "gap") {
                const g = item.gap;
                return (
                  <GapCard
                    key={`gap-${g.lead_min_dias}-${g.lead_max_dias}`}
                    gap={g}
                    onCriar={() => {
                      setAdding(true);
                      setExpanded(null);
                    }}
                  />
                );
              }
              const f = item.faixa;
              const key = `${f.lead_min_dias}-${f.lead_max_dias}`;
              return (
                <FaixaCard
                  key={key}
                  faixa={f}
                  expanded={expanded === key}
                  onExpand={() => setExpanded(expanded === key ? null : key)}
                  onSave={(body) =>
                    saveFaixa(
                      { lead_min: f.lead_min_dias, lead_max: f.lead_max_dias },
                      body
                    )
                  }
                  onToggleAtivo={() => toggleAtivo(f)}
                />
              );
            })}

            {adding && (
              <FaixaCard
                faixa={{
                  lead_min_dias: 0,
                  lead_max_dias: 0,
                  por_dow: false,
                  ajuste_uniforme: 0,
                  ajustes_dow: null,
                  ativo: true,
                }}
                expanded
                isNew
                onExpand={() => setAdding(false)}
                onSave={(body) => saveFaixa(null, body)}
                onToggleAtivo={() => {}}
              />
            )}
          </div>

          {/* Timeline visual — abaixo dos cards */}
          <div style={{ marginTop: 32 }}>
            <Timeline faixas={faixas.filter((f) => f.ativo)} gaps={gaps} />
          </div>

          <div style={{ marginTop: 24, fontSize: 11, color: "#64748b", lineHeight: 1.5 }}>
            <strong>Como funciona:</strong> para uma diária com N dias até o check-in, o
            motor aplica a regra da faixa onde N cai. Só uma faixa aplica por vez
            (não cumulativo). Dentro da faixa, a regra específica por dia da semana
            ganha da regra uniforme. Gaps aplicam 0%.
          </div>
        </div>
      </div>
    </div>
  );
}

type Item = { tipo: "faixa"; faixa: Faixa } | { tipo: "gap"; gap: Gap };
function combinarFaixasEGaps(faixas: Faixa[], gaps: Gap[]): Item[] {
  const out: Item[] = [];
  out.push(...faixas.map((f) => ({ tipo: "faixa" as const, faixa: f })));
  out.push(...gaps.map((g) => ({ tipo: "gap" as const, gap: g })));
  out.sort((a, b) => {
    const ka = a.tipo === "faixa" ? a.faixa.lead_min_dias : a.gap.lead_min_dias;
    const kb = b.tipo === "faixa" ? b.faixa.lead_min_dias : b.gap.lead_min_dias;
    return ka - kb;
  });
  return out;
}

function Timeline({ faixas, gaps }: { faixas: Faixa[]; gaps: Gap[] }) {
  const segments = useMemo(() => {
    const segs: Array<{
      mn: number;
      mx: number;
      label: string;
      tone: "faixa" | "gap";
      ajuste?: number;
      porDow?: boolean;
    }> = [];
    for (const f of faixas) {
      const ajuste =
        f.por_dow && f.ajustes_dow
          ? f.ajustes_dow.reduce((a, b) => a + b, 0) / 7
          : f.ajuste_uniforme ?? 0;
      segs.push({
        mn: f.lead_min_dias,
        mx: f.lead_max_dias,
        label: f.por_dow ? "varia" : fmtPct(f.ajuste_uniforme ?? 0),
        tone: "faixa",
        ajuste,
        porDow: f.por_dow,
      });
    }
    for (const g of gaps) {
      segs.push({ mn: g.lead_min_dias, mx: g.lead_max_dias, label: "gap", tone: "gap" });
    }
    segs.sort((a, b) => a.mn - b.mn);
    return segs;
  }, [faixas, gaps]);

  // Marcadores na régua: inclui bordas das faixas/gaps + marcadores principais
  const bordas = useMemo(() => {
    const s = new Set<number>([0, HORIZONTE]);
    for (const seg of segments) {
      s.add(seg.mn);
      s.add(seg.mx);
    }
    return Array.from(s).sort((a, b) => a - b);
  }, [segments]);

  return (
    <div
      style={{
        background: "#ffffff",
        border: "1px solid #e2e8f0",
        borderRadius: 12,
        padding: 20,
        boxShadow: "0 1px 2px rgba(15,23,42,0.04), 0 4px 14px rgba(15,23,42,0.04)",
      }}
    >
      {/* Título e legenda */}
      <div style={{ display: "flex", alignItems: "baseline", justifyContent: "space-between", marginBottom: 14 }}>
        <div>
          <div style={{ fontSize: 11, letterSpacing: 0.5, color: "#64748b", fontWeight: 700, textTransform: "uppercase" }}>
            Linha do tempo
          </div>
          <div style={{ fontSize: 12, color: "#94a3b8", marginTop: 2 }}>
            dias entre a reserva e o check-in
          </div>
        </div>
        <div style={{ display: "flex", gap: 14, fontSize: 10, color: "#64748b", alignItems: "center" }}>
          <span style={{ display: "inline-flex", alignItems: "center", gap: 6 }}>
            <span style={{ display: "inline-block", width: 14, height: 10, borderRadius: 3, background: segColor(0.05), border: "1px solid rgba(0,0,0,0.04)" }} />
            aumento
          </span>
          <span style={{ display: "inline-flex", alignItems: "center", gap: 6 }}>
            <span style={{ display: "inline-block", width: 14, height: 10, borderRadius: 3, background: segColor(0.35), border: "1px solid rgba(0,0,0,0.04)" }} />
            aumento forte
          </span>
          <span style={{ display: "inline-flex", alignItems: "center", gap: 6 }}>
            <span
              style={{
                display: "inline-block",
                width: 14,
                height: 10,
                borderRadius: 3,
                backgroundImage:
                  "repeating-linear-gradient(45deg, #f1f5f9, #f1f5f9 3px, #e2e8f0 3px, #e2e8f0 6px)",
                border: "1px solid #cbd5e1",
              }}
            />
            gap
          </span>
        </div>
      </div>

      {/* Marcadores "hoje" e "longe" */}
      <div style={{ display: "flex", justifyContent: "space-between", marginBottom: 4, fontSize: 10, fontWeight: 600, color: "#475569", letterSpacing: 0.3, textTransform: "uppercase" }}>
        <span>🟢 check-in</span>
        <span>{HORIZONTE} dias antes</span>
      </div>

      {/* Barra principal */}
      <div
        style={{
          position: "relative",
          display: "flex",
          height: 74,
          border: "1px solid #e2e8f0",
          borderRadius: 10,
          overflow: "hidden",
          background: "#f8fafc",
        }}
      >
        {segments.map((s, i) => {
          const width = ((s.mx - s.mn) / HORIZONTE) * 100;
          if (s.tone === "gap") {
            return (
              <div
                key={i}
                title={`${s.mn} – ${s.mx} dias · sem regra`}
                style={{
                  flex: `0 0 ${width}%`,
                  backgroundImage:
                    "repeating-linear-gradient(45deg, #f1f5f9, #f1f5f9 6px, #e2e8f0 6px, #e2e8f0 12px)",
                  borderRight: i < segments.length - 1 ? "1px dashed #cbd5e1" : 0,
                  display: "flex",
                  alignItems: "center",
                  justifyContent: "center",
                  fontSize: 10,
                  fontWeight: 600,
                  color: "#94a3b8",
                  textTransform: "uppercase",
                  letterSpacing: 0.5,
                  minWidth: 0,
                }}
              >
                {width > 6 && "gap"}
              </div>
            );
          }
          const bg = segColor(s.ajuste ?? 0);
          const fg = textOnSeg(s.ajuste ?? 0);
          return (
            <div
              key={i}
              title={`${s.mn} – ${s.mx} dias · ${s.label}`}
              style={{
                flex: `0 0 ${width}%`,
                background: bg,
                borderRight: i < segments.length - 1 ? "1px solid rgba(15,23,42,0.08)" : 0,
                display: "flex",
                flexDirection: "column",
                alignItems: "center",
                justifyContent: "center",
                gap: 3,
                color: fg,
                position: "relative",
                minWidth: 0,
                padding: "0 6px",
              }}
            >
              {width > 4 && (
                <>
                  <div style={{ fontSize: width > 10 ? 18 : 13, fontWeight: 700, letterSpacing: -0.3 }}>
                    {s.label}
                  </div>
                  {width > 7 && (
                    <div style={{ fontSize: 10, fontWeight: 500, opacity: 0.85, textTransform: "uppercase", letterSpacing: 0.3 }}>
                      {s.mn}–{s.mx}d{s.porDow ? " · por dia" : ""}
                    </div>
                  )}
                </>
              )}
            </div>
          );
        })}
      </div>

      {/* Régua de dias — todos os pontos de fronteira */}
      <div style={{ position: "relative", height: 22, marginTop: 4 }}>
        {bordas.map((d) => {
          const pct = (d / HORIZONTE) * 100;
          return (
            <div
              key={d}
              style={{
                position: "absolute",
                left: `${pct}%`,
                transform:
                  d === 0 ? "translateX(-0%)" : d === HORIZONTE ? "translateX(-100%)" : "translateX(-50%)",
                display: "flex",
                flexDirection: "column",
                alignItems: d === 0 ? "flex-start" : d === HORIZONTE ? "flex-end" : "center",
                gap: 2,
              }}
            >
              <span style={{ width: 1, height: 5, background: "#cbd5e1" }} />
              <span
                style={{
                  fontSize: 10,
                  color: "#64748b",
                  fontVariantNumeric: "tabular-nums",
                  fontWeight: 500,
                }}
              >
                {d}d
              </span>
            </div>
          );
        })}
      </div>
    </div>
  );
}

function GapCard({ gap, onCriar }: { gap: Gap; onCriar: () => void }) {
  return (
    <div
      style={{
        background: "#fffbeb",
        border: "1px dashed #fde68a",
        borderRadius: 6,
        padding: "10px 14px",
        display: "flex",
        alignItems: "center",
        gap: 10,
        fontSize: 13,
      }}
    >
      <span style={{ color: "#b45309", fontWeight: 600 }}>⚠</span>
      <span style={{ color: "#0f172a" }}>
        {gap.lead_min_dias} – {gap.lead_max_dias} dias: <strong>sem regra</strong> (0% de ajuste)
      </span>
      <div style={{ flex: 1 }} />
      <button onClick={onCriar} style={{ ...btnSecondary, fontSize: 11 }}>
        + criar faixa
      </button>
    </div>
  );
}

function FaixaCard({
  faixa,
  expanded,
  isNew = false,
  onExpand,
  onSave,
  onToggleAtivo,
}: {
  faixa: Faixa;
  expanded: boolean;
  isNew?: boolean;
  onExpand: () => void;
  onSave: (body: Omit<Faixa, "ativo">) => Promise<void>;
  onToggleAtivo: () => void;
}) {
  const [form, setForm] = useState<Faixa>(faixa);
  const [saving, setSaving] = useState(false);
  const [err, setErr] = useState<string | null>(null);

  useEffect(() => {
    setForm(faixa);
    setErr(null);
  }, [faixa, expanded]);

  const submit = async () => {
    setSaving(true);
    setErr(null);
    try {
      const body = {
        lead_min_dias: form.lead_min_dias,
        lead_max_dias: form.lead_max_dias,
        por_dow: form.por_dow,
        ajuste_uniforme: form.por_dow ? null : form.ajuste_uniforme ?? 0,
        ajustes_dow: form.por_dow ? form.ajustes_dow ?? [0, 0, 0, 0, 0, 0, 0] : null,
      };
      await onSave(body);
    } catch (e) {
      setErr(String(e));
    } finally {
      setSaving(false);
    }
  };

  const cancelar = () => {
    setForm(faixa);
    onExpand();
  };

  const togglePorDow = (checked: boolean) => {
    if (checked) {
      const uniforme = form.ajuste_uniforme ?? 0;
      setForm({ ...form, por_dow: true, ajustes_dow: Array(7).fill(uniforme), ajuste_uniforme: null });
    } else {
      const media = form.ajustes_dow
        ? form.ajustes_dow.reduce((a, b) => a + b, 0) / 7
        : 0;
      setForm({ ...form, por_dow: false, ajuste_uniforme: Number(media.toFixed(4)), ajustes_dow: null });
    }
  };

  const setDow = (idx: number, v: number) => {
    const arr = [...(form.ajustes_dow ?? Array(7).fill(0))];
    arr[idx] = v;
    setForm({ ...form, ajustes_dow: arr });
  };

  return (
    <div
      style={{
        background: "#ffffff",
        border: "1px solid #e2e8f0",
        borderRadius: 6,
        opacity: faixa.ativo ? 1 : 0.55,
      }}
    >
      {/* Header da faixa */}
      <div
        style={{
          padding: "10px 14px",
          display: "flex",
          alignItems: "center",
          gap: 10,
          cursor: "pointer",
        }}
        onClick={onExpand}
      >
        <span style={{ fontSize: 12, color: "#64748b", minWidth: 14 }}>
          {expanded ? "▾" : "▸"}
        </span>
        <div style={{ flex: "0 0 140px", fontWeight: 600, fontSize: 14, color: "#0f172a", fontVariantNumeric: "tabular-nums" }}>
          {isNew ? "nova faixa" : `${faixa.lead_min_dias} – ${faixa.lead_max_dias} dias`}
        </div>
        <div style={{ flex: 1, fontSize: 12, color: "#475569" }}>
          {isNew ? "preencha os campos abaixo" : resumoFaixa(faixa)}
        </div>
        {!isNew && (
          <button
            onClick={(e) => {
              e.stopPropagation();
              onToggleAtivo();
            }}
            style={{ ...btnSecondary, fontSize: 11, padding: "3px 10px" }}
          >
            {faixa.ativo ? "desativar" : "reativar"}
          </button>
        )}
      </div>

      {/* Corpo expandido */}
      {expanded && (
        <div style={{ borderTop: "1px solid #f1f5f9", padding: "14px 18px", background: "#fafafa" }}>
          <div style={{ display: "grid", gridTemplateColumns: "repeat(3, 1fr)", gap: 12, marginBottom: 12 }}>
            <Field label="de (dias)">
              <input
                type="number"
                min={0}
                max={365}
                value={form.lead_min_dias}
                onChange={(e) => setForm({ ...form, lead_min_dias: Number(e.target.value) })}
                style={inp}
              />
            </Field>
            <Field label="até (dias)">
              <input
                type="number"
                min={1}
                max={365}
                value={form.lead_max_dias}
                onChange={(e) => setForm({ ...form, lead_max_dias: Number(e.target.value) })}
                style={inp}
              />
            </Field>
            <Field label="modo">
              <label style={{ display: "flex", alignItems: "center", gap: 6, fontSize: 12, color: "#475569", paddingTop: 6 }}>
                <input
                  type="checkbox"
                  checked={form.por_dow}
                  onChange={(e) => togglePorDow(e.target.checked)}
                />
                granular por dia da semana
              </label>
            </Field>
          </div>

          {!form.por_dow && (
            <Field label={`ajuste (todos os dias): ${fmtPct(form.ajuste_uniforme ?? 0)}`}>
              <input
                type="range"
                min={-0.3}
                max={1.0}
                step={0.01}
                value={form.ajuste_uniforme ?? 0}
                onChange={(e) => setForm({ ...form, ajuste_uniforme: Number(e.target.value) })}
                style={{ width: "100%" }}
              />
            </Field>
          )}

          {form.por_dow && (
            <div>
              <div style={{ fontSize: 11, color: "#64748b", letterSpacing: 0.3, fontWeight: 600, textTransform: "uppercase", marginBottom: 6 }}>
                Ajuste por dia da semana
              </div>
              <div style={{ display: "grid", gridTemplateColumns: "repeat(7, 1fr)", gap: 6 }}>
                {DOWS.map((d, i) => {
                  const v = form.ajustes_dow?.[i] ?? 0;
                  return (
                    <div key={d} style={{ display: "flex", flexDirection: "column", alignItems: "center", gap: 4 }}>
                      <span style={{ fontSize: 10, color: "#64748b", fontWeight: 600, letterSpacing: 0.3 }}>
                        {d}
                      </span>
                      <input
                        type="number"
                        step={1}
                        value={Math.round(v * 100)}
                        onChange={(e) => setDow(i, Number(e.target.value) / 100)}
                        style={{ ...inp, width: "100%", textAlign: "right", padding: "5px 6px" }}
                      />
                      <span style={{ fontSize: 9, color: "#94a3b8" }}>%</span>
                    </div>
                  );
                })}
              </div>
            </div>
          )}

          {err && <div style={{ color: "#dc2626", fontSize: 12, marginTop: 10 }}>{err}</div>}

          <div style={{ display: "flex", gap: 8, justifyContent: "flex-end", marginTop: 14 }}>
            <button onClick={cancelar} style={cancelBtn}>Cancelar</button>
            <button onClick={submit} disabled={saving} style={btnPrimary}>
              {saving ? "Salvando…" : isNew ? "Criar faixa" : "Salvar"}
            </button>
          </div>
        </div>
      )}
    </div>
  );
}

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
