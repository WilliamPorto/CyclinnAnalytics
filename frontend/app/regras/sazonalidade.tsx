"use client";

import { useCallback, useEffect, useMemo, useRef, useState } from "react";

type Regra = {
  regra_id: number;
  escopo: "global" | "regiao" | "predio" | "segmento" | "unidade";
  escopo_id: number | null;
  nome: string;
  data_inicio: string; // ISO date
  data_fim: string;
  ajuste_pct: number;
  recorrente_anual: boolean;
  prioridade: number;
  ativo: boolean;
};

type EscopoOpt = { id: number; nome: string };

const MESES = ["jan", "fev", "mar", "abr", "mai", "jun", "jul", "ago", "set", "out", "nov", "dez"];
const DAYS_PER_MONTH = [31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
const PALETTE = ["#3b82f6", "#10b981", "#f59e0b", "#ef4444", "#8b5cf6", "#ec4899", "#14b8a6", "#f97316"];

function colorForRule(id: number): string {
  return PALETTE[id % PALETTE.length];
}

function fmtDate(iso: string): string {
  const [, m, d] = iso.split("-").map(Number);
  return `${String(d).padStart(2, "0")}/${MESES[m - 1]}`;
}

function fmtAjuste(a: number): string {
  const p = a * 100;
  return `${p > 0 ? "+" : ""}${p.toFixed(0)}%`;
}

function escopoLabel(r: Regra, opts: Record<string, EscopoOpt[]>): string {
  if (r.escopo === "global") return "Global";
  const opt = opts[r.escopo]?.find((o) => o.id === r.escopo_id);
  const tipo = r.escopo[0].toUpperCase() + r.escopo.slice(1);
  return opt ? `${tipo}: ${opt.nome}` : `${tipo}: #${r.escopo_id ?? "?"}`;
}

// Marca cada (mês, dia) coberto por um intervalo de datas (ignora ano; trata cruzamento ano ok)
function diasDoIntervalo(ini: string, fim: string): Set<string> {
  const [yi, mi, di] = ini.split("-").map(Number);
  const [yf, mf, df] = fim.split("-").map(Number);
  const d1 = new Date(Date.UTC(yi, mi - 1, di));
  const d2 = new Date(Date.UTC(yf, mf - 1, df));
  const out = new Set<string>();
  let safety = 400;
  const cur = new Date(d1);
  while (safety-- > 0) {
    out.add(`${cur.getUTCMonth() + 1}-${cur.getUTCDate()}`);
    if (cur.getTime() === d2.getTime()) break;
    cur.setUTCDate(cur.getUTCDate() + 1);
  }
  return out;
}

export default function SazonalidadeTab() {
  const [regras, setRegras] = useState<Regra[]>([]);
  const [escopoOpts, setEscopoOpts] = useState<Record<string, EscopoOpt[]>>({});
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [drawerRegra, setDrawerRegra] = useState<Partial<Regra> | null>(null);
  const [hoveredId, setHoveredId] = useState<number | null>(null);
  const [rebuildState, setRebuildState] = useState<"idle" | "running" | "ok" | "error">("idle");
  const [rebuildMsg, setRebuildMsg] = useState<string>("");
  const [pendingChanges, setPendingChanges] = useState(false);

  const fetchRegras = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const r = await fetch("/api/regras/sazonalidade");
      if (!r.ok) throw new Error(`HTTP ${r.status}`);
      setRegras(await r.json());
    } catch (e) {
      setError(String(e));
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchRegras();
    // Pre-fetch opções de escopo (não-unidade, pra ser leve)
    Promise.all(
      ["regiao", "predio", "segmento"].map((t) =>
        fetch(`/api/regras/escopo/${t}`)
          .then((r) => r.json())
          .then((d) => [t, d] as const)
      )
    ).then((entries) => {
      const obj: Record<string, EscopoOpt[]> = {};
      for (const [t, d] of entries) obj[t] = d;
      setEscopoOpts(obj);
    });
  }, [fetchRegras]);

  const saveRegra = useCallback(
    async (r: Partial<Regra>) => {
      const isNew = !r.regra_id;
      const body = {
        nome: r.nome ?? "",
        data_inicio: r.data_inicio ?? "",
        data_fim: r.data_fim ?? "",
        ajuste_pct: r.ajuste_pct ?? 0,
        escopo: r.escopo ?? "global",
        escopo_id: r.escopo === "global" ? null : r.escopo_id ?? null,
        recorrente_anual: r.recorrente_anual ?? true,
        prioridade: r.prioridade ?? 10,
        ...(isNew ? {} : { ativo: r.ativo ?? true }),
      };
      const res = await fetch(
        isNew
          ? "/api/regras/sazonalidade"
          : `/api/regras/sazonalidade/${r.regra_id}`,
        {
          method: isNew ? "POST" : "PATCH",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(body),
        }
      );
      if (!res.ok) {
        const err = await res.json().catch(() => ({}));
        throw new Error(err.detail ?? `HTTP ${res.status}`);
      }
      setDrawerRegra(null);
      setPendingChanges(true);
      await fetchRegras();
    },
    [fetchRegras]
  );

  const toggleAtivo = useCallback(
    async (r: Regra) => {
      const res = await fetch(`/api/regras/sazonalidade/${r.regra_id}`, {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ ativo: !r.ativo }),
      });
      if (!res.ok) {
        setError(`Falha ao alterar ativo: ${await res.text()}`);
        return;
      }
      setPendingChanges(true);
      await fetchRegras();
    },
    [fetchRegras]
  );

  const rebuild = useCallback(async () => {
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
  }, []);

  const regrasAtivas = useMemo(() => regras.filter((r) => r.ativo), [regras]);

  return (
    <div style={{ height: "100%", display: "flex", flexDirection: "column", overflow: "hidden" }}>
      {/* Toolbar */}
      <div
        style={{
          display: "flex",
          alignItems: "center",
          gap: 12,
          padding: "10px 16px",
          background: "#f8fafc",
          borderBottom: "1px solid #e2e8f0",
        }}
      >
        <button
          onClick={() =>
            setDrawerRegra({
              escopo: "global",
              data_inicio: "",
              data_fim: "",
              ajuste_pct: 0.1,
              recorrente_anual: true,
              prioridade: 10,
            })
          }
          style={{
            background: "#1d4ed8",
            color: "white",
            border: 0,
            padding: "6px 14px",
            borderRadius: 5,
            fontSize: 13,
            fontWeight: 600,
            cursor: "pointer",
          }}
        >
          + Nova regra
        </button>
        <div style={{ flex: 1 }} />
        {pendingChanges && (
          <span style={{ color: "#b45309", fontSize: 12, fontWeight: 500 }}>
            ⚠ alterações ainda não aplicadas ao simulador
          </span>
        )}
        <button
          onClick={rebuild}
          disabled={rebuildState === "running"}
          style={{
            background: pendingChanges ? "#f59e0b" : "#ffffff",
            color: pendingChanges ? "#ffffff" : "#475569",
            border: "1px solid",
            borderColor: pendingChanges ? "#f59e0b" : "#cbd5e1",
            padding: "6px 12px",
            borderRadius: 5,
            fontSize: 12,
            cursor: rebuildState === "running" ? "not-allowed" : "pointer",
            fontWeight: 600,
          }}
        >
          {rebuildState === "running" ? "Reconstruindo…" : "Reconstruir simulador"}
        </button>
        {rebuildState === "ok" && (
          <span style={{ color: "#15803d", fontSize: 12 }}>✓ {rebuildMsg}</span>
        )}
        {rebuildState === "error" && (
          <span style={{ color: "#dc2626", fontSize: 12 }}>✕ {rebuildMsg}</span>
        )}
      </div>

      {/* Conteúdo: lista + calendário */}
      <div style={{ flex: 1, display: "flex", minHeight: 0 }}>
        <div style={{ flex: 1, overflow: "auto", padding: 16 }}>
          {loading && <div style={{ color: "#64748b" }}>carregando…</div>}
          {error && <div style={{ color: "#dc2626" }}>{error}</div>}
          {!loading && regras.length === 0 && (
            <div style={{ color: "#64748b" }}>Nenhuma regra cadastrada.</div>
          )}
          {regras.length > 0 && (
            <ListaRegras
              regras={regras}
              hoveredId={hoveredId}
              onHover={setHoveredId}
              onEdit={(r) => setDrawerRegra(r)}
              onToggleAtivo={toggleAtivo}
              escopoOpts={escopoOpts}
            />
          )}
        </div>

        <aside
          style={{
            flex: "0 0 420px",
            borderLeft: "1px solid #e2e8f0",
            background: "#f8fafc",
            overflow: "auto",
            padding: 16,
          }}
        >
          <div style={{ fontSize: 11, letterSpacing: 0.5, color: "#64748b", fontWeight: 700, textTransform: "uppercase", marginBottom: 12 }}>
            Calendário anual (ano genérico)
          </div>
          <MiniCalendario regras={regrasAtivas} hoveredId={hoveredId} />
        </aside>
      </div>

      {drawerRegra && (
        <Drawer
          regra={drawerRegra}
          escopoOpts={escopoOpts}
          setEscopoOpts={setEscopoOpts}
          onSave={saveRegra}
          onClose={() => setDrawerRegra(null)}
        />
      )}
    </div>
  );
}

function ListaRegras({
  regras,
  hoveredId,
  onHover,
  onEdit,
  onToggleAtivo,
  escopoOpts,
}: {
  regras: Regra[];
  hoveredId: number | null;
  onHover: (id: number | null) => void;
  onEdit: (r: Regra) => void;
  onToggleAtivo: (r: Regra) => void;
  escopoOpts: Record<string, EscopoOpt[]>;
}) {
  return (
    <table style={{ borderCollapse: "collapse", width: "100%", fontSize: 13 }}>
      <thead>
        <tr style={{ background: "#f1f5f9" }}>
          {["", "nome", "período", "escopo", "ajuste", "recorrente", "prioridade", "ações"].map((h) => (
            <th
              key={h}
              style={{
                textAlign: "left",
                padding: "6px 10px",
                borderBottom: "1px solid #cbd5e1",
                color: "#1d4ed8",
                fontWeight: 600,
                whiteSpace: "nowrap",
                fontSize: 12,
              }}
            >
              {h}
            </th>
          ))}
        </tr>
      </thead>
      <tbody>
        {regras.map((r) => {
          const highlight = hoveredId === r.regra_id;
          const faded = hoveredId !== null && !highlight;
          return (
            <tr
              key={r.regra_id}
              onMouseEnter={() => onHover(r.regra_id)}
              onMouseLeave={() => onHover(null)}
              style={{
                opacity: r.ativo ? (faded ? 0.35 : 1) : 0.45,
                background: highlight ? "#eff6ff" : "transparent",
                cursor: "pointer",
              }}
              onClick={() => onEdit(r)}
            >
              <td style={td}>
                <span
                  style={{
                    display: "inline-block",
                    width: 10,
                    height: 10,
                    borderRadius: 2,
                    background: colorForRule(r.regra_id),
                  }}
                />
              </td>
              <td style={{ ...td, fontWeight: 500 }}>{r.nome}</td>
              <td style={td}>{fmtDate(r.data_inicio)} → {fmtDate(r.data_fim)}</td>
              <td style={td}>{escopoLabel(r, escopoOpts)}</td>
              <td style={{ ...td, fontVariantNumeric: "tabular-nums", color: r.ajuste_pct >= 0 ? "#15803d" : "#dc2626" }}>
                {fmtAjuste(r.ajuste_pct)}
              </td>
              <td style={td}>{r.recorrente_anual ? "anual" : "única"}</td>
              <td style={{ ...td, textAlign: "right" }}>{r.prioridade}</td>
              <td style={td} onClick={(e) => e.stopPropagation()}>
                <button
                  onClick={() => onToggleAtivo(r)}
                  style={{
                    background: r.ativo ? "#ffffff" : "#f1f5f9",
                    border: "1px solid #cbd5e1",
                    borderRadius: 4,
                    padding: "3px 10px",
                    fontSize: 11,
                    cursor: "pointer",
                    color: r.ativo ? "#475569" : "#64748b",
                    fontFamily: "inherit",
                  }}
                >
                  {r.ativo ? "desativar" : "reativar"}
                </button>
              </td>
            </tr>
          );
        })}
      </tbody>
    </table>
  );
}

function MiniCalendario({
  regras,
  hoveredId,
}: {
  regras: Regra[];
  hoveredId: number | null;
}) {
  // Para cada (mes, dia), encontra regras que cobrem
  const coverage = useMemo(() => {
    const byKey: Record<string, Regra[]> = {};
    for (const r of regras) {
      const dias = diasDoIntervalo(r.data_inicio, r.data_fim);
      for (const k of dias) {
        if (!byKey[k]) byKey[k] = [];
        byKey[k].push(r);
      }
    }
    return byKey;
  }, [regras]);

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 4 }}>
      {MESES.map((nome, idx) => {
        const m = idx + 1;
        const dias = DAYS_PER_MONTH[idx];
        return (
          <div key={m} style={{ display: "flex", alignItems: "center", gap: 8 }}>
            <div style={{ width: 28, fontSize: 10, color: "#64748b", textAlign: "right", textTransform: "uppercase" }}>
              {nome}
            </div>
            <div
              style={{
                display: "grid",
                gridTemplateColumns: `repeat(31, 1fr)`,
                gap: 1,
                flex: 1,
              }}
            >
              {Array.from({ length: 31 }).map((_, dIdx) => {
                const d = dIdx + 1;
                if (d > dias) {
                  return <div key={d} style={{ height: 14, background: "transparent" }} />;
                }
                const rs = coverage[`${m}-${d}`] ?? [];
                let bg = "#ffffff";
                let border = "1px solid #e2e8f0";
                if (rs.length > 0) {
                  // Se há hover: só mostra a cor da regra hover
                  if (hoveredId !== null) {
                    const match = rs.find((r) => r.regra_id === hoveredId);
                    bg = match ? colorForRule(hoveredId) : "#e2e8f0";
                    border = "1px solid " + (match ? colorForRule(hoveredId) : "#e2e8f0");
                  } else {
                    // Sem hover: pega a de maior prioridade
                    const top = [...rs].sort((a, b) => b.prioridade - a.prioridade)[0];
                    bg = colorForRule(top.regra_id);
                    border = "1px solid " + bg;
                  }
                }
                return (
                  <div
                    key={d}
                    title={rs.length > 0 ? rs.map((r) => r.nome).join(" · ") : `${d}/${nome}`}
                    style={{
                      height: 14,
                      background: bg,
                      border,
                      borderRadius: 2,
                    }}
                  />
                );
              })}
            </div>
          </div>
        );
      })}
    </div>
  );
}

function Drawer({
  regra,
  escopoOpts,
  setEscopoOpts,
  onSave,
  onClose,
}: {
  regra: Partial<Regra>;
  escopoOpts: Record<string, EscopoOpt[]>;
  setEscopoOpts: React.Dispatch<React.SetStateAction<Record<string, EscopoOpt[]>>>;
  onSave: (r: Partial<Regra>) => Promise<void>;
  onClose: () => void;
}) {
  const [form, setForm] = useState<Partial<Regra>>(regra);
  const [saving, setSaving] = useState(false);
  const [err, setErr] = useState<string | null>(null);

  const set = <K extends keyof Regra>(k: K, v: Regra[K]) => setForm({ ...form, [k]: v });

  // Load opções de unidade sob demanda
  useEffect(() => {
    if (form.escopo === "unidade" && !escopoOpts.unidade) {
      fetch("/api/regras/escopo/unidade")
        .then((r) => r.json())
        .then((d) => setEscopoOpts((s) => ({ ...s, unidade: d })));
    }
  }, [form.escopo, escopoOpts.unidade, setEscopoOpts]);

  const submit = async () => {
    setSaving(true);
    setErr(null);
    try {
      await onSave(form);
    } catch (e) {
      setErr(String(e));
    } finally {
      setSaving(false);
    }
  };

  const title = form.regra_id ? `Editar regra #${form.regra_id}` : "Nova regra";

  return (
    <div
      style={{
        position: "fixed",
        top: 0,
        right: 0,
        bottom: 0,
        width: 480,
        background: "#ffffff",
        boxShadow: "-4px 0 24px rgba(0,0,0,0.12)",
        padding: 20,
        overflow: "auto",
        zIndex: 10,
        display: "flex",
        flexDirection: "column",
        gap: 12,
      }}
    >
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
        <h2 style={{ margin: 0, fontSize: 16, color: "#0f172a" }}>{title}</h2>
        <button onClick={onClose} style={{ background: "transparent", border: 0, fontSize: 20, cursor: "pointer", color: "#64748b" }}>×</button>
      </div>

      <Field label="Nome">
        <input value={form.nome ?? ""} onChange={(e) => set("nome", e.target.value)} style={inp} placeholder="Ex: Verão 2027" />
      </Field>

      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 10 }}>
        <Field label="Data início">
          <input type="date" value={form.data_inicio ?? ""} onChange={(e) => set("data_inicio", e.target.value)} style={inp} />
        </Field>
        <Field label="Data fim">
          <input type="date" value={form.data_fim ?? ""} onChange={(e) => set("data_fim", e.target.value)} style={inp} />
        </Field>
      </div>

      <Field label={`Ajuste: ${fmtAjuste(form.ajuste_pct ?? 0)}`}>
        <input
          type="range"
          min={-0.5}
          max={1.0}
          step={0.01}
          value={form.ajuste_pct ?? 0}
          onChange={(e) => set("ajuste_pct", Number(e.target.value))}
          style={{ width: "100%" }}
        />
      </Field>

      <Field label="Escopo">
        <select
          value={form.escopo ?? "global"}
          onChange={(e) => {
            const v = e.target.value as Regra["escopo"];
            setForm({ ...form, escopo: v, escopo_id: v === "global" ? null : form.escopo_id });
          }}
          style={inp}
        >
          <option value="global">Global (todo o portfólio)</option>
          <option value="regiao">Região</option>
          <option value="predio">Prédio</option>
          <option value="segmento">Segmento</option>
          <option value="unidade">Unidade específica</option>
        </select>
      </Field>

      {form.escopo && form.escopo !== "global" && (
        <Field label={`${form.escopo[0].toUpperCase() + form.escopo.slice(1)}`}>
          <select
            value={form.escopo_id ?? ""}
            onChange={(e) => set("escopo_id", Number(e.target.value))}
            style={inp}
          >
            <option value="">— escolha —</option>
            {(escopoOpts[form.escopo] ?? []).map((o) => (
              <option key={o.id} value={o.id}>
                {o.nome}
              </option>
            ))}
          </select>
        </Field>
      )}

      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 10 }}>
        <Field label="Prioridade">
          <input
            type="number"
            value={form.prioridade ?? 10}
            onChange={(e) => set("prioridade", Number(e.target.value))}
            style={inp}
          />
        </Field>
        <Field label="Recorrente anual">
          <label style={{ display: "flex", alignItems: "center", gap: 6, paddingTop: 6 }}>
            <input
              type="checkbox"
              checked={form.recorrente_anual ?? true}
              onChange={(e) => set("recorrente_anual", e.target.checked)}
            />
            <span style={{ fontSize: 12, color: "#475569" }}>repete todo ano</span>
          </label>
        </Field>
      </div>

      {err && <div style={{ color: "#dc2626", fontSize: 12 }}>{err}</div>}

      <div style={{ display: "flex", gap: 8, marginTop: 12 }}>
        <button
          onClick={submit}
          disabled={saving}
          style={{
            background: "#1d4ed8",
            color: "white",
            border: 0,
            padding: "8px 16px",
            borderRadius: 5,
            fontSize: 13,
            fontWeight: 600,
            cursor: saving ? "not-allowed" : "pointer",
          }}
        >
          {saving ? "Salvando…" : form.regra_id ? "Salvar alterações" : "Criar regra"}
        </button>
        <button
          onClick={onClose}
          style={{
            background: "#ffffff",
            color: "#475569",
            border: "1px solid #cbd5e1",
            padding: "8px 16px",
            borderRadius: 5,
            fontSize: 13,
            cursor: "pointer",
          }}
        >
          Cancelar
        </button>
      </div>
    </div>
  );
}

function Field({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <label style={{ display: "flex", flexDirection: "column", gap: 4 }}>
      <span style={{ fontSize: 11, color: "#64748b", letterSpacing: 0.3, fontWeight: 600, textTransform: "uppercase" }}>
        {label}
      </span>
      {children}
    </label>
  );
}

const td: React.CSSProperties = {
  padding: "6px 10px",
  borderBottom: "1px solid #f1f5f9",
  whiteSpace: "nowrap",
};
const inp: React.CSSProperties = {
  padding: "6px 10px",
  border: "1px solid #cbd5e1",
  borderRadius: 4,
  fontSize: 13,
  fontFamily: "inherit",
  width: "100%",
};
