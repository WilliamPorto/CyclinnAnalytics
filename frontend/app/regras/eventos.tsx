"use client";

import { useCallback, useEffect, useMemo, useState } from "react";

type EscopoTipo = "global" | "regiao" | "predio" | "unidade";
type Categoria = "esportivo" | "show" | "feriado" | "convencao";

type Impacto = {
  escopo: EscopoTipo;
  escopo_id: number | null;
  ajuste_pct: number;
};

type Evento = {
  evento_id: number;
  nome: string;
  data_inicio: string;
  data_fim: string;
  categoria: Categoria;
  ativo: boolean;
  impactos: Impacto[];
};

type EscopoOpt = { id: number; nome: string };

const CAT_COLORS: Record<Categoria, { bg: string; fg: string; label: string; cell: string }> = {
  // bg/fg = badges (contraste alto pra texto); cell = cor saturada do bloco no calendário
  esportivo: { bg: "#fef3c7", fg: "#b45309", label: "esportivo", cell: "#f59e0b" },
  show: { bg: "#ede9fe", fg: "#6d28d9", label: "show", cell: "#8b5cf6" },
  feriado: { bg: "#dbeafe", fg: "#1d4ed8", label: "feriado", cell: "#3b82f6" },
  convencao: { bg: "#dcfce7", fg: "#15803d", label: "convenção", cell: "#22c55e" },
};

const MESES = ["jan", "fev", "mar", "abr", "mai", "jun", "jul", "ago", "set", "out", "nov", "dez"];
const DAYS_PER_MONTH = [31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];

function fmtDateBr(iso: string): string {
  if (!iso) return "";
  const [, m, d] = iso.split("-").map(Number);
  return `${String(d).padStart(2, "0")}/${MESES[m - 1]}`;
}

function fmtPct(v: number): string {
  if (Math.abs(v) < 1e-4) return "—";
  const p = v * 100;
  return `${p > 0 ? "+" : ""}${Number.isInteger(p) ? p.toFixed(0) : p.toFixed(1)}%`;
}

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

function nomeEscopo(imp: Impacto, opts: Record<string, EscopoOpt[]>): string {
  if (imp.escopo === "global") return "Global";
  const opt = opts[imp.escopo]?.find((o) => o.id === imp.escopo_id);
  return opt ? opt.nome : `${imp.escopo} #${imp.escopo_id ?? "?"}`;
}

export default function EventosTab() {
  const [eventos, setEventos] = useState<Evento[]>([]);
  const [selectedId, setSelectedId] = useState<number | null>(null);
  const [openEditOnSelect, setOpenEditOnSelect] = useState(false);
  const [busca, setBusca] = useState("");
  const [mostrarInativos, setMostrarInativos] = useState(true);
  const [opts, setOpts] = useState<Record<string, EscopoOpt[]>>({});
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [pendingChanges, setPendingChanges] = useState(false);
  const [rebuildState, setRebuildState] = useState<"idle" | "running" | "ok" | "error">("idle");
  const [rebuildMsg, setRebuildMsg] = useState("");

  const fetchMatriz = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const r = await fetch("/api/regras/eventos/matriz");
      if (!r.ok) throw new Error(`HTTP ${r.status}`);
      const d = await r.json();
      setEventos(d.eventos ?? []);
      // Auto-select first active if none selected
      setSelectedId((curr) => {
        if (curr !== null && (d.eventos ?? []).some((e: Evento) => e.evento_id === curr)) {
          return curr;
        }
        const first = (d.eventos ?? []).find((e: Evento) => e.ativo);
        return first?.evento_id ?? null;
      });
    } catch (e) {
      setError(String(e));
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchMatriz();
  }, [fetchMatriz]);

  const loadOptsSe = useCallback(
    async (tipo: string) => {
      if (opts[tipo] || tipo === "global") return;
      const r = await fetch(`/api/regras/escopo/${tipo}`);
      const d = await r.json();
      setOpts((s) => ({ ...s, [tipo]: d }));
    },
    [opts]
  );

  // Pré-carrega regiao e predio (unidade só sob demanda por ser grande)
  useEffect(() => {
    loadOptsSe("regiao");
    loadOptsSe("predio");
  }, [loadOptsSe]);

  const eventosFiltrados = useMemo(() => {
    let arr = eventos;
    if (!mostrarInativos) arr = arr.filter((e) => e.ativo);
    if (busca.trim()) {
      const q = busca.trim().toLowerCase();
      arr = arr.filter(
        (e) => e.nome.toLowerCase().includes(q) || e.categoria.toLowerCase().includes(q)
      );
    }
    return arr;
  }, [eventos, busca, mostrarInativos]);

  const selected = eventos.find((e) => e.evento_id === selectedId) ?? null;

  const saveEvento = async (form: Partial<Evento>): Promise<void> => {
    const isNew = !form.evento_id;
    const body = {
      nome: form.nome ?? "",
      data_inicio: form.data_inicio ?? "",
      data_fim: form.data_fim ?? "",
      categoria: form.categoria ?? "feriado",
      ...(isNew ? {} : { ativo: form.ativo ?? true }),
    };
    const res = await fetch(
      isNew ? "/api/regras/eventos" : `/api/regras/eventos/${form.evento_id}`,
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
    setPendingChanges(true);
    if (isNew) {
      const d = await res.json();
      await fetchMatriz();
      setSelectedId(d.evento_id);
    } else {
      await fetchMatriz();
    }
  };

  const toggleAtivoEvento = async (ev: Evento) => {
    const res = await fetch(`/api/regras/eventos/${ev.evento_id}`, {
      method: "PATCH",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ ativo: !ev.ativo }),
    });
    if (res.ok) {
      setPendingChanges(true);
      fetchMatriz();
    }
  };

  const novoEvento = async () => {
    // Cria um evento rascunho com valores default; abre em modo edição
    const today = new Date().toISOString().slice(0, 10);
    try {
      setOpenEditOnSelect(true);
      await saveEvento({
        nome: "Novo evento",
        data_inicio: today,
        data_fim: today,
        categoria: "feriado",
      });
    } catch (e) {
      setError(String(e));
      setOpenEditOnSelect(false);
    }
  };

  const upsertImpacto = async (
    evento_id: number,
    escopo: EscopoTipo,
    escopo_id: number | null,
    ajuste_pct: number
  ) => {
    const res = await fetch(`/api/regras/eventos/${evento_id}/impacto`, {
      method: "PATCH",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ escopo, escopo_id, ajuste_pct }),
    });
    if (!res.ok) {
      const err = await res.json().catch(() => ({}));
      throw new Error(err.detail ?? `HTTP ${res.status}`);
    }
    setPendingChanges(true);
    await fetchMatriz();
  };

  const removerImpacto = (ev: Evento, imp: Impacto) =>
    upsertImpacto(ev.evento_id, imp.escopo, imp.escopo_id, 0);

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
        <button onClick={novoEvento} style={btnPrimary}>+ novo evento</button>
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

      {/* Master-detail */}
      <div style={{ flex: 1, display: "flex", minHeight: 0, overflow: "hidden" }}>
        {/* Lista de eventos */}
        <aside
          style={{
            flex: "0 0 560px",
            borderRight: "1px solid #e2e8f0",
            background: "#f8fafc",
            display: "flex",
            flexDirection: "column",
          }}
        >
          <div style={{ padding: "10px 12px", borderBottom: "1px solid #e2e8f0", display: "flex", gap: 10, alignItems: "center" }}>
            <input
              placeholder="buscar evento…"
              value={busca}
              onChange={(e) => setBusca(e.target.value)}
              style={{ ...inp, flex: 1 }}
            />
            <label style={{ display: "flex", alignItems: "center", gap: 6, fontSize: 11, color: "#64748b", whiteSpace: "nowrap" }}>
              <input
                type="checkbox"
                checked={mostrarInativos}
                onChange={(e) => setMostrarInativos(e.target.checked)}
              />
              mostrar inativos
            </label>
          </div>

          {/* Cabeçalho das colunas */}
          <div
            style={{
              display: "grid",
              gridTemplateColumns: "1fr 150px 110px 50px",
              gap: 8,
              padding: "6px 14px",
              background: "#f1f5f9",
              borderBottom: "1px solid #e2e8f0",
              fontSize: 10,
              fontWeight: 700,
              color: "#64748b",
              textTransform: "uppercase",
              letterSpacing: 0.3,
            }}
          >
            <div>evento</div>
            <div>período</div>
            <div>categoria</div>
            <div style={{ textAlign: "right" }}>imp.</div>
          </div>

          <div style={{ flex: 1, overflow: "auto" }}>
            {loading && <div style={{ padding: 14, color: "#64748b" }}>carregando…</div>}
            {error && <div style={{ padding: 14, color: "#dc2626" }}>{error}</div>}
            {eventosFiltrados.length === 0 && !loading && (
              <div style={{ padding: 14, color: "#94a3b8", fontSize: 12 }}>
                {eventos.length === 0 ? "Nenhum evento cadastrado." : "Sem resultados."}
              </div>
            )}
            {eventosFiltrados.map((ev) => {
              const c = CAT_COLORS[ev.categoria];
              const isSel = ev.evento_id === selectedId;
              return (
                <button
                  key={ev.evento_id}
                  onClick={() => setSelectedId(ev.evento_id)}
                  style={{
                    display: "grid",
                    gridTemplateColumns: "1fr 150px 110px 50px",
                    gap: 8,
                    alignItems: "center",
                    width: "100%",
                    textAlign: "left",
                    background: isSel ? "#eff6ff" : "#ffffff",
                    border: 0,
                    borderLeft: isSel ? "3px solid #1d4ed8" : "3px solid transparent",
                    borderBottom: "1px solid #f1f5f9",
                    padding: "10px 11px",
                    cursor: "pointer",
                    fontFamily: "inherit",
                    opacity: ev.ativo ? 1 : 0.5,
                  }}
                >
                  <div style={{ display: "flex", alignItems: "center", gap: 6, minWidth: 0 }}>
                    <span
                      style={{
                        width: 8,
                        height: 8,
                        borderRadius: 2,
                        background: c.fg,
                        flex: "0 0 auto",
                      }}
                    />
                    <span style={{ fontWeight: 500, fontSize: 13, color: "#0f172a", overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
                      {ev.nome}
                    </span>
                  </div>
                  <div style={{ fontSize: 11, color: "#475569", fontVariantNumeric: "tabular-nums" }}>
                    {fmtDateBr(ev.data_inicio)}
                    {ev.data_inicio !== ev.data_fim && ` → ${fmtDateBr(ev.data_fim)}`}
                  </div>
                  <div>
                    <span
                      style={{
                        background: c.bg,
                        color: c.fg,
                        padding: "2px 8px",
                        borderRadius: 8,
                        fontSize: 10,
                        fontWeight: 600,
                        textTransform: "uppercase",
                        letterSpacing: 0.3,
                      }}
                    >
                      {c.label}
                    </span>
                  </div>
                  <div style={{ fontSize: 11, color: "#64748b", textAlign: "right" }}>
                    {ev.impactos.length}
                  </div>
                </button>
              );
            })}
          </div>
          <div style={{ padding: "8px 12px", borderTop: "1px solid #e2e8f0", fontSize: 11, color: "#64748b" }}>
            {eventosFiltrados.length} evento{eventosFiltrados.length === 1 ? "" : "s"}
          </div>
        </aside>

        {/* Detalhe do evento selecionado */}
        <section style={{ flex: 1, display: "flex", flexDirection: "column", overflow: "hidden" }}>
          {selected ? (
            <EventoDetalhe
              evento={selected}
              opts={opts}
              loadOptsSe={loadOptsSe}
              openEditOnMount={openEditOnSelect}
              onEditOpened={() => setOpenEditOnSelect(false)}
              onSave={saveEvento}
              onToggleAtivo={toggleAtivoEvento}
              onUpsertImpacto={upsertImpacto}
              onRemoverImpacto={removerImpacto}
            />
          ) : (
            <div style={{ flex: 1, display: "flex", alignItems: "center", justifyContent: "center", color: "#94a3b8" }}>
              Selecione um evento à esquerda.
            </div>
          )}
        </section>
      </div>

      {/* Mini-calendário */}
      <div
        style={{
          borderTop: "1px solid #e2e8f0",
          background: "#f8fafc",
          padding: 16,
          flex: "0 0 auto",
        }}
      >
        <div
          style={{
            fontSize: 11,
            letterSpacing: 0.5,
            color: "#64748b",
            fontWeight: 700,
            textTransform: "uppercase",
            marginBottom: 8,
          }}
        >
          Calendário anual — eventos ativos (por categoria)
        </div>
        <MiniCalendario
          eventos={eventos.filter((e) => e.ativo)}
          selectedId={selectedId}
        />
      </div>
    </div>
  );
}

function EventoDetalhe({
  evento,
  opts,
  loadOptsSe,
  openEditOnMount,
  onEditOpened,
  onSave,
  onToggleAtivo,
  onUpsertImpacto,
  onRemoverImpacto,
}: {
  evento: Evento;
  opts: Record<string, EscopoOpt[]>;
  loadOptsSe: (tipo: string) => Promise<void>;
  openEditOnMount?: boolean;
  onEditOpened?: () => void;
  onSave: (e: Partial<Evento>) => Promise<void>;
  onToggleAtivo: (e: Evento) => void;
  onUpsertImpacto: (evId: number, esc: EscopoTipo, escId: number | null, v: number) => Promise<void>;
  onRemoverImpacto: (e: Evento, imp: Impacto) => Promise<void>;
}) {
  const [editMeta, setEditMeta] = useState(false);
  const [form, setForm] = useState<Partial<Evento>>(evento);
  const [savingMeta, setSavingMeta] = useState(false);
  const [saveErr, setSaveErr] = useState<string | null>(null);
  const [addingImpacto, setAddingImpacto] = useState(false);
  const [editingImpacto, setEditingImpacto] = useState<{ esc: EscopoTipo; escId: number | null } | null>(null);
  const [impactoInputVal, setImpactoInputVal] = useState("");

  // Reseta estados locais apenas quando mudar de evento (não em cada re-fetch)
  useEffect(() => {
    setForm(evento);
    setAddingImpacto(false);
    setEditingImpacto(null);
    if (openEditOnMount) {
      setEditMeta(true);
      onEditOpened?.();
    } else {
      setEditMeta(false);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [evento.evento_id]);

  // Sincroniza form com evento quando o próprio evento for atualizado (sem abrir edit)
  useEffect(() => {
    if (!editMeta) setForm(evento);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [evento]);

  const submitMeta = async () => {
    setSavingMeta(true);
    setSaveErr(null);
    try {
      await onSave({ ...form, evento_id: evento.evento_id });
      setEditMeta(false);
    } catch (e) {
      setSaveErr(String(e));
    } finally {
      setSavingMeta(false);
    }
  };

  const startEditImpacto = (imp: Impacto) => {
    setImpactoInputVal((imp.ajuste_pct * 100).toFixed(0));
    setEditingImpacto({ esc: imp.escopo, escId: imp.escopo_id });
  };

  const commitEditImpacto = async () => {
    if (!editingImpacto) return;
    const trimmed = impactoInputVal.trim();
    const val = trimmed === "" ? 0 : parseFloat(trimmed.replace(",", ".")) / 100;
    if (isNaN(val)) {
      setEditingImpacto(null);
      return;
    }
    const prev = editingImpacto;
    setEditingImpacto(null);
    try {
      await onUpsertImpacto(evento.evento_id, prev.esc, prev.escId, val);
    } catch (e) {
      setSaveErr(String(e));
    }
  };

  const c = CAT_COLORS[evento.categoria];

  return (
    <div style={{ flex: 1, display: "flex", flexDirection: "column", overflow: "auto" }}>
      {/* Metadata header */}
      <div style={{ padding: "18px 22px", borderBottom: "1px solid #e2e8f0" }}>
        <div style={{ display: "flex", alignItems: "center", gap: 10, marginBottom: 4 }}>
          {!evento.ativo && (
            <span
              style={{
                background: "#fee2e2",
                color: "#991b1b",
                padding: "1px 8px",
                borderRadius: 10,
                fontSize: 10,
                fontWeight: 700,
                textTransform: "uppercase",
              }}
            >
              inativo
            </span>
          )}
          <h2
            onClick={() => !editMeta && setEditMeta(true)}
            title={!editMeta ? "Clique para editar" : undefined}
            style={{
              margin: 0,
              fontSize: 18,
              color: "#0f172a",
              flex: 1,
              cursor: editMeta ? "default" : "pointer",
            }}
          >
            {evento.nome}
          </h2>
          <button
            onClick={() => onToggleAtivo(evento)}
            style={{ ...btnSecondary, fontSize: 11, padding: "4px 10px" }}
          >
            {evento.ativo ? "desativar" : "reativar"}
          </button>
          {!editMeta && (
            <button
              onClick={() => setEditMeta(true)}
              style={{ ...btnSecondary, fontSize: 11, padding: "4px 10px" }}
            >
              editar
            </button>
          )}
        </div>

        {!editMeta ? (
          <div style={{ display: "flex", alignItems: "center", gap: 14, fontSize: 13, color: "#475569" }}>
            <span>
              <strong>{fmtDateBr(evento.data_inicio)}</strong>
              {evento.data_inicio !== evento.data_fim && (
                <> → <strong>{fmtDateBr(evento.data_fim)}</strong></>
              )}
            </span>
            <span
              style={{
                background: c.bg,
                color: c.fg,
                padding: "2px 10px",
                borderRadius: 10,
                fontSize: 11,
                fontWeight: 600,
                textTransform: "uppercase",
                letterSpacing: 0.3,
              }}
            >
              {c.label}
            </span>
          </div>
        ) : (
          <div style={{ display: "grid", gridTemplateColumns: "2fr 1fr 1fr 1fr", gap: 10, marginTop: 8 }}>
            <Field label="nome">
              <input
                value={form.nome ?? ""}
                onChange={(e) => setForm({ ...form, nome: e.target.value })}
                style={inp}
              />
            </Field>
            <Field label="data início">
              <input
                type="date"
                value={form.data_inicio ?? ""}
                onChange={(e) => setForm({ ...form, data_inicio: e.target.value })}
                style={inp}
              />
            </Field>
            <Field label="data fim">
              <input
                type="date"
                value={form.data_fim ?? ""}
                onChange={(e) => setForm({ ...form, data_fim: e.target.value })}
                style={inp}
              />
            </Field>
            <Field label="categoria">
              <select
                value={form.categoria ?? "feriado"}
                onChange={(e) => setForm({ ...form, categoria: e.target.value as Categoria })}
                style={inp}
              >
                <option value="esportivo">esportivo</option>
                <option value="show">show</option>
                <option value="feriado">feriado</option>
                <option value="convencao">convenção</option>
              </select>
            </Field>
            <div style={{ gridColumn: "1 / -1", display: "flex", gap: 8, justifyContent: "flex-end", marginTop: 4 }}>
              {saveErr && <span style={{ color: "#dc2626", fontSize: 11, flex: 1 }}>{saveErr}</span>}
              <button
                onClick={() => {
                  setEditMeta(false);
                  setForm(evento);
                  setSaveErr(null);
                }}
                style={cancelBtn}
              >
                Cancelar
              </button>
              <button onClick={submitMeta} disabled={savingMeta} style={btnPrimary}>
                {savingMeta ? "Salvando…" : "Salvar"}
              </button>
            </div>
          </div>
        )}
      </div>

      {/* Impactos */}
      <div style={{ padding: "14px 22px", flex: 1, maxWidth: 760 }}>
        <div style={{ display: "flex", alignItems: "baseline", marginBottom: 10, gap: 10 }}>
          <div style={{ fontSize: 11, letterSpacing: 0.5, color: "#64748b", fontWeight: 700, textTransform: "uppercase" }}>
            Impactos ({evento.impactos.length})
          </div>
          <div style={{ flex: 1 }} />
          {!addingImpacto && evento.ativo && (
            <button onClick={() => setAddingImpacto(true)} style={{ ...btnPrimary, padding: "4px 12px", fontSize: 12 }}>
              + adicionar impacto
            </button>
          )}
        </div>

        {evento.impactos.length === 0 && !addingImpacto && (
          <div style={{ color: "#94a3b8", fontSize: 13, padding: 20, textAlign: "center", border: "1px dashed #e2e8f0", borderRadius: 6 }}>
            Nenhum impacto cadastrado. Adicione ao menos 1 pra esse evento afetar o preço.
          </div>
        )}

        {(evento.impactos.length > 0 || addingImpacto) && (
          <table style={{ borderCollapse: "collapse", width: "100%", fontSize: 13 }}>
            <thead>
              <tr style={{ background: "#f1f5f9" }}>
                <th style={{ ...thCell, width: 120 }}>tipo</th>
                <th style={thCell}>escopo</th>
                <th style={{ ...thCell, width: 140, textAlign: "right" }}>ajuste</th>
                <th style={{ ...thCell, width: 80 }}></th>
              </tr>
            </thead>
            <tbody>
              {evento.impactos.map((imp) => {
                const isEditing =
                  editingImpacto &&
                  editingImpacto.esc === imp.escopo &&
                  editingImpacto.escId === imp.escopo_id;
                return (
                  <tr key={`${imp.escopo}:${imp.escopo_id}`}>
                    <td style={tdCell}>
                      <span
                        style={{
                          fontSize: 10,
                          color: "#475569",
                          background: "#f1f5f9",
                          padding: "2px 8px",
                          borderRadius: 4,
                          textTransform: "uppercase",
                          fontWeight: 600,
                          letterSpacing: 0.3,
                        }}
                      >
                        {imp.escopo}
                      </span>
                    </td>
                    <td style={tdCell}>{nomeEscopo(imp, opts)}</td>
                    <td
                      style={{ ...tdCell, textAlign: "right", fontVariantNumeric: "tabular-nums", cursor: evento.ativo ? "pointer" : "default" }}
                      onClick={() => evento.ativo && !isEditing && startEditImpacto(imp)}
                    >
                      {isEditing ? (
                        <input
                          autoFocus
                          value={impactoInputVal}
                          onChange={(e) => setImpactoInputVal(e.target.value)}
                          onBlur={commitEditImpacto}
                          onKeyDown={(e) => {
                            if (e.key === "Enter") commitEditImpacto();
                            if (e.key === "Escape") setEditingImpacto(null);
                          }}
                          placeholder="% (vazio = remover)"
                          style={{
                            width: 110,
                            padding: "4px 6px",
                            border: "1px solid #1d4ed8",
                            borderRadius: 3,
                            fontSize: 12,
                            textAlign: "right",
                            fontFamily: "inherit",
                          }}
                        />
                      ) : (
                        <span style={{ color: imp.ajuste_pct >= 0 ? "#15803d" : "#dc2626", fontWeight: 600 }}>
                          {fmtPct(imp.ajuste_pct)}
                        </span>
                      )}
                    </td>
                    <td style={{ ...tdCell, textAlign: "center" }}>
                      <button
                        onClick={() => onRemoverImpacto(evento, imp)}
                        title="Remover"
                        style={{
                          background: "transparent",
                          border: "1px solid #e2e8f0",
                          color: "#dc2626",
                          padding: "3px 8px",
                          borderRadius: 4,
                          fontSize: 11,
                          cursor: "pointer",
                          fontFamily: "inherit",
                        }}
                      >
                        remover
                      </button>
                    </td>
                  </tr>
                );
              })}
              {addingImpacto && (
                <NovoImpactoRow
                  evento={evento}
                  opts={opts}
                  loadOptsSe={loadOptsSe}
                  onCancel={() => setAddingImpacto(false)}
                  onSave={async (esc, escId, val) => {
                    await onUpsertImpacto(evento.evento_id, esc, escId, val);
                    setAddingImpacto(false);
                  }}
                />
              )}
            </tbody>
          </table>
        )}

        <div style={{ marginTop: 18, fontSize: 11, color: "#64748b", maxWidth: 700, lineHeight: 1.5 }}>
          <strong>Herança:</strong> o motor escolhe o impacto mais específico — Unidade
          &gt; Prédio &gt; Região &gt; Global. Entre eventos diferentes, os ajustes
          somam.
        </div>
      </div>
    </div>
  );
}

function NovoImpactoRow({
  evento,
  opts,
  loadOptsSe,
  onSave,
  onCancel,
}: {
  evento: Evento;
  opts: Record<string, EscopoOpt[]>;
  loadOptsSe: (tipo: string) => Promise<void>;
  onSave: (esc: EscopoTipo, escId: number | null, val: number) => Promise<void>;
  onCancel: () => void;
}) {
  const [tipo, setTipo] = useState<EscopoTipo>("regiao");
  const [escId, setEscId] = useState<number | null>(null);
  const [val, setVal] = useState("");
  const [err, setErr] = useState<string | null>(null);
  const [saving, setSaving] = useState(false);

  useEffect(() => {
    loadOptsSe(tipo);
  }, [tipo, loadOptsSe]);

  const jaExiste = useMemo(() => {
    return evento.impactos.some(
      (i) => i.escopo === tipo && i.escopo_id === escId
    );
  }, [evento.impactos, tipo, escId]);

  const submit = async () => {
    const n = parseFloat(val.replace(",", "."));
    if (isNaN(n)) {
      setErr("Valor inválido");
      return;
    }
    if (tipo !== "global" && escId === null) {
      setErr("Selecione o escopo");
      return;
    }
    if (jaExiste) {
      setErr("Esse escopo já tem impacto — edite o existente");
      return;
    }
    setSaving(true);
    setErr(null);
    try {
      await onSave(tipo, tipo === "global" ? null : escId, n / 100);
    } catch (e) {
      setErr(String(e));
    } finally {
      setSaving(false);
    }
  };

  return (
    <tr style={{ background: "#eff6ff" }}>
      <td style={tdCell}>
        <select
          value={tipo}
          onChange={(e) => {
            setTipo(e.target.value as EscopoTipo);
            setEscId(null);
          }}
          style={{ ...inp, padding: "4px 6px", fontSize: 12 }}
        >
          <option value="global">global</option>
          <option value="regiao">regiao</option>
          <option value="predio">predio</option>
          <option value="unidade">unidade</option>
        </select>
      </td>
      <td style={tdCell}>
        {tipo === "global" ? (
          <span style={{ color: "#64748b", fontSize: 12 }}>Global</span>
        ) : (
          <select
            value={escId ?? ""}
            onChange={(e) => setEscId(e.target.value ? Number(e.target.value) : null)}
            style={{ ...inp, padding: "4px 6px", fontSize: 12, width: "100%" }}
          >
            <option value="">— escolha —</option>
            {(opts[tipo] ?? []).map((o) => (
              <option key={o.id} value={o.id}>
                {o.nome}
              </option>
            ))}
          </select>
        )}
      </td>
      <td style={{ ...tdCell, textAlign: "right" }}>
        <input
          autoFocus
          value={val}
          onChange={(e) => setVal(e.target.value)}
          onKeyDown={(e) => {
            if (e.key === "Enter") submit();
            if (e.key === "Escape") onCancel();
          }}
          placeholder="ex: 80 (%)"
          style={{
            width: 110,
            padding: "4px 6px",
            border: "1px solid #1d4ed8",
            borderRadius: 3,
            fontSize: 12,
            textAlign: "right",
            fontFamily: "inherit",
          }}
        />
      </td>
      <td style={{ ...tdCell, textAlign: "center" }}>
        <div style={{ display: "flex", gap: 4, justifyContent: "center" }}>
          <button onClick={submit} disabled={saving} style={{ ...btnPrimary, padding: "3px 10px", fontSize: 11 }}>
            {saving ? "…" : "salvar"}
          </button>
          <button onClick={onCancel} style={{ ...cancelBtn, padding: "3px 8px", fontSize: 11 }}>
            ✕
          </button>
        </div>
        {err && <div style={{ color: "#dc2626", fontSize: 10, marginTop: 4 }}>{err}</div>}
      </td>
    </tr>
  );
}

function MiniCalendario({
  eventos,
  selectedId,
}: {
  eventos: Evento[];
  selectedId: number | null;
}) {
  const coverage = useMemo(() => {
    const m: Record<string, Evento[]> = {};
    for (const e of eventos) {
      const dias = diasDoIntervalo(e.data_inicio, e.data_fim);
      for (const k of dias) {
        if (!m[k]) m[k] = [];
        m[k].push(e);
      }
    }
    return m;
  }, [eventos]);

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 2 }}>
      {/* Régua de dias */}
      <div style={{ display: "flex", alignItems: "center", gap: 6, marginBottom: 2 }}>
        <div style={{ width: 28 }} />
        <div style={{ display: "grid", gridTemplateColumns: "repeat(31, 1fr)", gap: 1, flex: 1 }}>
          {Array.from({ length: 31 }).map((_, dIdx) => {
            const d = dIdx + 1;
            const destaque = d % 5 === 0 || d === 1;
            return (
              <div
                key={d}
                style={{
                  fontSize: 9,
                  color: destaque ? "#334155" : "#64748b",
                  fontWeight: destaque ? 600 : 400,
                  textAlign: "center",
                  fontVariantNumeric: "tabular-nums",
                  lineHeight: 1,
                }}
              >
                {d}
              </div>
            );
          })}
        </div>
      </div>

      {MESES.map((nome, idx) => {
        const m = idx + 1;
        const dias = DAYS_PER_MONTH[idx];
        return (
          <div key={m} style={{ display: "flex", alignItems: "center", gap: 6 }}>
            <div style={{ width: 28, fontSize: 10, color: "#64748b", textAlign: "right", textTransform: "uppercase", fontWeight: 600 }}>
              {nome}
            </div>
            <div style={{ display: "grid", gridTemplateColumns: "repeat(31, 1fr)", gap: 1, flex: 1 }}>
              {Array.from({ length: 31 }).map((_, dIdx) => {
                const d = dIdx + 1;
                if (d > dias) return <div key={d} style={{ height: 12 }} />;
                const evs = coverage[`${m}-${d}`] ?? [];
                let bg = "#f1f5f9";
                let border = "1px solid #e2e8f0";
                let title = `${d}/${nome}`;
                if (evs.length > 0) {
                  const match =
                    selectedId !== null ? evs.find((e) => e.evento_id === selectedId) : null;
                  if (selectedId !== null) {
                    bg = match ? CAT_COLORS[match.categoria].cell : "#cbd5e1";
                    border = "1px solid " + (match ? CAT_COLORS[match.categoria].fg : "#94a3b8");
                  } else {
                    const top = evs[0];
                    bg = CAT_COLORS[top.categoria].cell;
                    border = "1px solid " + CAT_COLORS[top.categoria].fg;
                  }
                  title = evs.map((e) => e.nome).join(" · ");
                }
                return (
                  <div key={d} title={title} style={{ height: 12, background: bg, border, borderRadius: 2 }} />
                );
              })}
            </div>
          </div>
        );
      })}

      <div style={{ display: "flex", gap: 14, marginTop: 10, fontSize: 10, color: "#64748b" }}>
        {(Object.entries(CAT_COLORS) as [Categoria, typeof CAT_COLORS.esportivo][]).map(([k, c]) => (
          <span key={k} style={{ display: "inline-flex", alignItems: "center", gap: 4 }}>
            <span style={{ width: 12, height: 12, background: c.cell, border: "1px solid " + c.fg, borderRadius: 2 }} />
            {c.label}
          </span>
        ))}
        <span style={{ display: "inline-flex", alignItems: "center", gap: 4, marginLeft: 8 }}>
          <span style={{ width: 12, height: 12, background: "#f1f5f9", border: "1px solid #e2e8f0", borderRadius: 2 }} />
          sem eventos
        </span>
      </div>
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

const thCell: React.CSSProperties = {
  textAlign: "left",
  padding: "8px 10px",
  borderBottom: "1px solid #cbd5e1",
  color: "#1d4ed8",
  fontWeight: 600,
  fontSize: 11,
  whiteSpace: "nowrap",
};
const tdCell: React.CSSProperties = {
  padding: "8px 10px",
  borderBottom: "1px solid #f1f5f9",
  whiteSpace: "nowrap",
};
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
