"use client";

import { useCallback, useEffect, useMemo, useState } from "react";

type EscopoTipo = "global" | "regiao" | "predio";
type EscopoRow = {
  escopo: EscopoTipo;
  escopo_id: number | null;
  nome: string;
  ativo: boolean;
  values: number[]; // 7 valores: seg,ter,qua,qui,sex,sab,dom
};

type EscopoOpt = { id: number; nome: string };

const DOWS = ["SEG", "TER", "QUA", "QUI", "SEX", "SÁB", "DOM"];

function fmtPct(v: number): string {
  const p = v * 100;
  if (Math.abs(p) < 0.05) return "0%";
  return `${p > 0 ? "+" : ""}${Number.isInteger(p) ? p.toFixed(0) : p.toFixed(1)}%`;
}

// Heatmap diverging: vermelho ← branco → verde
function cellColor(v: number, vabs: number): string {
  if (Math.abs(v) < 0.001) return "#ffffff";
  const t = vabs === 0 ? 0 : Math.min(1, Math.abs(v) / vabs);
  if (v > 0) {
    const r = Math.round(220 - (220 - 22) * t);
    const g = Math.round(252 - (252 - 163) * t);
    const b = Math.round(231 - (231 - 74) * t);
    return `rgb(${r},${g},${b})`;
  }
  const r = Math.round(254 - (254 - 220) * t);
  const g = Math.round(226 - (226 - 38) * t);
  const b = Math.round(226 - (226 - 38) * t);
  return `rgb(${r},${g},${b})`;
}

export default function DiaSemanaTab() {
  const [escopos, setEscopos] = useState<EscopoRow[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [editing, setEditing] = useState<{ rowIdx: number; dow: number } | null>(null);
  const [inputValue, setInputValue] = useState("");
  const [adding, setAdding] = useState(false);
  const [newTipo, setNewTipo] = useState<"regiao" | "predio">("regiao");
  const [newId, setNewId] = useState<number | null>(null);
  const [opts, setOpts] = useState<Record<string, EscopoOpt[]>>({});
  const [pendingChanges, setPendingChanges] = useState(false);
  const [rebuildState, setRebuildState] = useState<"idle" | "running" | "ok" | "error">("idle");
  const [rebuildMsg, setRebuildMsg] = useState("");

  const fetchMatriz = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const r = await fetch("/api/regras/dia-semana/matriz");
      if (!r.ok) throw new Error(`HTTP ${r.status}`);
      const d = await r.json();
      setEscopos(d.escopos ?? []);
    } catch (e) {
      setError(String(e));
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchMatriz();
    Promise.all(
      ["regiao", "predio"].map((t) =>
        fetch(`/api/regras/escopo/${t}`).then((r) => r.json()).then((d) => [t, d] as const)
      )
    ).then((ents) => {
      const o: Record<string, EscopoOpt[]> = {};
      for (const [t, d] of ents) o[t] = d;
      setOpts(o);
    });
  }, [fetchMatriz]);

  const vabs = useMemo(() => {
    let m = 0;
    for (const e of escopos) for (const v of e.values) m = Math.max(m, Math.abs(v));
    return m || 0.1;
  }, [escopos]);

  const startEdit = (rowIdx: number, dow: number) => {
    const val = escopos[rowIdx].values[dow];
    setInputValue((val * 100).toFixed(0));
    setEditing({ rowIdx, dow });
  };

  const commitEdit = async () => {
    if (!editing) return;
    const { rowIdx, dow } = editing;
    const e = escopos[rowIdx];
    const parsed = parseFloat(inputValue.replace(",", "."));
    if (isNaN(parsed)) {
      setEditing(null);
      return;
    }
    const newVal = parsed / 100;
    // optimistic update
    const newEscopos = escopos.map((x, i) =>
      i === rowIdx
        ? { ...x, values: x.values.map((v, j) => (j === dow ? newVal : v)) }
        : x
    );
    setEscopos(newEscopos);
    setEditing(null);
    try {
      const res = await fetch("/api/regras/dia-semana/celula", {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          escopo: e.escopo,
          escopo_id: e.escopo_id,
          dia_semana: dow,
          ajuste_pct: newVal,
        }),
      });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      setPendingChanges(true);
    } catch (err) {
      setError(String(err));
      fetchMatriz(); // reverte via refresh
    }
  };

  const toggleAtivo = async (e: EscopoRow) => {
    try {
      const res = await fetch("/api/regras/dia-semana/escopo/ativo", {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ escopo: e.escopo, escopo_id: e.escopo_id, ativo: !e.ativo }),
      });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      setPendingChanges(true);
      fetchMatriz();
    } catch (err) {
      setError(String(err));
    }
  };

  const criarEscopo = async () => {
    if (newTipo !== "regiao" && newTipo !== "predio") return;
    if (newId === null) return;
    try {
      const res = await fetch("/api/regras/dia-semana/escopo", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ escopo: newTipo, escopo_id: newId }),
      });
      if (!res.ok) {
        const err = await res.json().catch(() => ({}));
        throw new Error(err.detail ?? `HTTP ${res.status}`);
      }
      setAdding(false);
      setNewId(null);
      setPendingChanges(true);
      fetchMatriz();
    } catch (err) {
      setError(String(err));
    }
  };

  const criarGlobalSeFaltar = async () => {
    try {
      const res = await fetch("/api/regras/dia-semana/escopo", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ escopo: "global", escopo_id: null }),
      });
      if (!res.ok) {
        const err = await res.json().catch(() => ({}));
        throw new Error(err.detail ?? `HTTP ${res.status}`);
      }
      setPendingChanges(true);
      fetchMatriz();
    } catch (err) {
      setError(String(err));
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

  const hasGlobal = escopos.some((e) => e.escopo === "global");

  return (
    <div style={{ height: "100%", display: "flex", flexDirection: "column", overflow: "hidden" }}>
      <div
        style={{
          display: "flex",
          alignItems: "center",
          gap: 10,
          padding: "10px 16px",
          background: "#f8fafc",
          borderBottom: "1px solid #e2e8f0",
          flexWrap: "wrap",
        }}
      >
        <button
          onClick={() => setAdding(true)}
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
          + adicionar escopo
        </button>
        {!hasGlobal && (
          <button
            onClick={criarGlobalSeFaltar}
            style={{
              background: "#fef3c7",
              color: "#b45309",
              border: "1px solid #fde68a",
              padding: "6px 12px",
              borderRadius: 5,
              fontSize: 12,
              cursor: "pointer",
              fontFamily: "inherit",
            }}
            title="Ainda não existe um padrão 'Global' — ao clicar, cria a linha zerada"
          >
            + criar linha Global
          </button>
        )}
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
        {rebuildState === "ok" && <span style={{ color: "#15803d", fontSize: 12 }}>✓ {rebuildMsg}</span>}
        {rebuildState === "error" && <span style={{ color: "#dc2626", fontSize: 12 }}>✕ {rebuildMsg}</span>}
      </div>

      {adding && (
        <div
          style={{
            display: "flex",
            alignItems: "center",
            gap: 10,
            padding: "10px 16px",
            background: "#eff6ff",
            borderBottom: "1px solid #bfdbfe",
          }}
        >
          <span style={{ fontSize: 12, color: "#1d4ed8", fontWeight: 600 }}>Novo escopo:</span>
          <select
            value={newTipo}
            onChange={(e) => {
              setNewTipo(e.target.value as "regiao" | "predio");
              setNewId(null);
            }}
            style={sel}
          >
            <option value="regiao">Região</option>
            <option value="predio">Prédio</option>
          </select>
          <select
            value={newId ?? ""}
            onChange={(e) => setNewId(Number(e.target.value))}
            style={{ ...sel, minWidth: 200 }}
          >
            <option value="">— escolha —</option>
            {(opts[newTipo] ?? [])
              .filter((o) => !escopos.some((e) => e.escopo === newTipo && e.escopo_id === o.id))
              .map((o) => (
                <option key={o.id} value={o.id}>
                  {o.nome}
                </option>
              ))}
          </select>
          <button
            onClick={criarEscopo}
            disabled={newId === null}
            style={{
              background: "#1d4ed8",
              color: "white",
              border: 0,
              padding: "5px 14px",
              borderRadius: 5,
              fontSize: 12,
              fontWeight: 600,
              cursor: newId === null ? "not-allowed" : "pointer",
              opacity: newId === null ? 0.6 : 1,
            }}
          >
            Criar
          </button>
          <button onClick={() => setAdding(false)} style={cancelBtn}>
            Cancelar
          </button>
          <span style={{ color: "#64748b", fontSize: 11, marginLeft: 8 }}>
            novos escopos herdam valores do Global
          </span>
        </div>
      )}

      <div style={{ flex: 1, overflow: "auto", padding: 16 }}>
        {loading && <div style={{ color: "#64748b" }}>carregando…</div>}
        {error && <div style={{ color: "#dc2626" }}>{error}</div>}
        {escopos.length === 0 && !loading && (
          <div style={{ color: "#64748b" }}>Nenhum escopo cadastrado.</div>
        )}
        {escopos.length > 0 && (
          <table style={{ borderCollapse: "separate", borderSpacing: 0, fontSize: 13 }}>
            <thead>
              <tr>
                <th style={hLabel}>escopo</th>
                {DOWS.map((d) => (
                  <th key={d} style={hDow}>
                    {d}
                  </th>
                ))}
                <th style={hAction}></th>
              </tr>
            </thead>
            <tbody>
              {escopos.map((e, rowIdx) => (
                <tr key={`${e.escopo}-${e.escopo_id ?? "null"}`} style={{ opacity: e.ativo ? 1 : 0.45 }}>
                  <td
                    style={{
                      ...cellLabel,
                      fontWeight: e.escopo === "global" ? 700 : 500,
                      color: e.escopo === "global" ? "#1d4ed8" : "#0f172a",
                    }}
                  >
                    {e.nome}
                    <div style={{ fontSize: 10, color: "#94a3b8", textTransform: "uppercase" }}>
                      {e.escopo}
                    </div>
                  </td>
                  {e.values.map((v, dow) => {
                    const isEditing =
                      editing && editing.rowIdx === rowIdx && editing.dow === dow;
                    if (isEditing) {
                      return (
                        <td key={dow} style={{ ...cellVal, padding: 2 }}>
                          <input
                            autoFocus
                            value={inputValue}
                            onChange={(ev) => setInputValue(ev.target.value)}
                            onBlur={commitEdit}
                            onKeyDown={(ev) => {
                              if (ev.key === "Enter") commitEdit();
                              if (ev.key === "Escape") setEditing(null);
                            }}
                            style={{
                              width: 50,
                              padding: "4px 4px",
                              border: "1px solid #1d4ed8",
                              borderRadius: 3,
                              fontSize: 12,
                              textAlign: "right",
                              fontFamily: "inherit",
                            }}
                          />
                          <span style={{ fontSize: 10, color: "#64748b", marginLeft: 2 }}>%</span>
                        </td>
                      );
                    }
                    return (
                      <td
                        key={dow}
                        onClick={() => e.ativo && startEdit(rowIdx, dow)}
                        style={{
                          ...cellVal,
                          background: cellColor(v, vabs),
                          cursor: e.ativo ? "pointer" : "not-allowed",
                        }}
                        title={e.ativo ? "Clique para editar" : "escopo inativo"}
                      >
                        {fmtPct(v)}
                      </td>
                    );
                  })}
                  <td style={cellAction}>
                    <button
                      onClick={() => toggleAtivo(e)}
                      style={{
                        background: e.ativo ? "#ffffff" : "#f1f5f9",
                        border: "1px solid #cbd5e1",
                        borderRadius: 4,
                        padding: "3px 10px",
                        fontSize: 11,
                        cursor: "pointer",
                        color: e.ativo ? "#475569" : "#64748b",
                        fontFamily: "inherit",
                      }}
                    >
                      {e.ativo ? "desativar" : "reativar"}
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
        <div style={{ marginTop: 18, fontSize: 11, color: "#64748b", maxWidth: 600, lineHeight: 1.5 }}>
          <strong>Herança:</strong> ao calcular o preço de uma unidade, o motor usa a
          regra <em>mais específica</em> que casa — Prédio &gt; Região &gt; Global. Se
          nenhuma regra casar, o ajuste é 0%.
        </div>
      </div>
    </div>
  );
}

const hLabel: React.CSSProperties = {
  textAlign: "left",
  padding: "8px 12px",
  background: "#f1f5f9",
  borderBottom: "1px solid #cbd5e1",
  color: "#1d4ed8",
  fontWeight: 600,
  position: "sticky",
  top: 0,
  minWidth: 200,
};
const hDow: React.CSSProperties = {
  padding: "8px 10px",
  background: "#f1f5f9",
  borderBottom: "1px solid #cbd5e1",
  color: "#1d4ed8",
  fontWeight: 600,
  fontSize: 11,
  textAlign: "center",
  minWidth: 70,
};
const hAction: React.CSSProperties = {
  padding: "8px 12px",
  background: "#f1f5f9",
  borderBottom: "1px solid #cbd5e1",
  minWidth: 100,
};
const cellLabel: React.CSSProperties = {
  padding: "8px 12px",
  borderBottom: "1px solid #f1f5f9",
  whiteSpace: "nowrap",
};
const cellVal: React.CSSProperties = {
  padding: "8px 10px",
  borderBottom: "1px solid #f1f5f9",
  textAlign: "right",
  fontVariantNumeric: "tabular-nums",
  whiteSpace: "nowrap",
  transition: "background 0.1s",
};
const cellAction: React.CSSProperties = {
  padding: "8px 12px",
  borderBottom: "1px solid #f1f5f9",
  textAlign: "center",
};
const sel: React.CSSProperties = {
  padding: "4px 8px",
  border: "1px solid #cbd5e1",
  borderRadius: 4,
  fontSize: 12,
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
