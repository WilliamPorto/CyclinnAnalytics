"use client";

import { useCallback, useEffect, useMemo, useState } from "react";

type Item = {
  log_id: number;
  timestamp: string; // ISO UTC
  usuario: string;
  operacao: string;
  recurso: string;
  recurso_id: string | null;
  detalhes: string | null;
};

const OPERACOES_KNOWN: Record<string, { label: string; bg: string; fg: string }> = {
  create: { label: "criar", bg: "#dcfce7", fg: "#15803d" },
  update: { label: "atualizar", bg: "#dbeafe", fg: "#1d4ed8" },
  delete: { label: "excluir", bg: "#fee2e2", fg: "#b91c1c" },
  rebuild_simulador: { label: "rebuild simulador", bg: "#ede9fe", fg: "#6d28d9" },
  rebuild_simulador_erro: { label: "rebuild (erro)", bg: "#fef3c7", fg: "#b45309" },
};

function opInfo(op: string) {
  return (
    OPERACOES_KNOWN[op] ?? { label: op, bg: "#f1f5f9", fg: "#475569" }
  );
}

function fmtTimestamp(iso: string): string {
  const d = new Date(iso);
  return d.toLocaleString("pt-BR", {
    day: "2-digit",
    month: "2-digit",
    year: "numeric",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  });
}

export default function AuditoriaPage() {
  const [items, setItems] = useState<Item[]>([]);
  const [total, setTotal] = useState(0);
  const [page, setPage] = useState(1);
  const [pageSize, setPageSize] = useState(50);
  const [recurso, setRecurso] = useState("");
  const [operacao, setOperacao] = useState("");
  const [usuario, setUsuario] = useState("");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const fetchLog = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const qs = new URLSearchParams({
        page: String(page),
        page_size: String(pageSize),
      });
      if (recurso) qs.set("recurso", recurso);
      if (operacao) qs.set("operacao", operacao);
      if (usuario) qs.set("usuario", usuario);
      const r = await fetch(`/api/auditoria/operacoes?${qs}`);
      if (!r.ok) throw new Error(`HTTP ${r.status}`);
      const d = await r.json();
      setItems(d.items ?? []);
      setTotal(d.total ?? 0);
    } catch (e) {
      setError(String(e));
    } finally {
      setLoading(false);
    }
  }, [page, pageSize, recurso, operacao, usuario]);

  useEffect(() => {
    fetchLog();
  }, [fetchLog]);

  useEffect(() => {
    setPage(1);
  }, [recurso, operacao, usuario, pageSize]);

  const totalPages = Math.max(1, Math.ceil(total / pageSize));

  const resourcesVisible = useMemo(() => {
    return Array.from(new Set(items.map((i) => i.recurso))).sort();
  }, [items]);

  return (
    <main style={{ height: "100%", display: "flex", flexDirection: "column", overflow: "hidden" }}>
      {/* Toolbar de filtros */}
      <div
        style={{
          display: "flex",
          gap: 14,
          alignItems: "flex-end",
          padding: "12px 18px",
          background: "#f8fafc",
          borderBottom: "1px solid #e2e8f0",
          flex: "0 0 auto",
          flexWrap: "wrap",
        }}
      >
        <Field label="recurso">
          <input
            placeholder="ex: regras.eventos"
            value={recurso}
            onChange={(e) => setRecurso(e.target.value)}
            style={{ ...inp, width: 200 }}
          />
        </Field>
        <Field label="operação">
          <select value={operacao} onChange={(e) => setOperacao(e.target.value)} style={{ ...inp, width: 160 }}>
            <option value="">todas</option>
            <option value="create">criar</option>
            <option value="update">atualizar</option>
            <option value="delete">excluir</option>
            <option value="rebuild_simulador">rebuild simulador</option>
          </select>
        </Field>
        <Field label="usuário">
          <input
            placeholder="ex: admin"
            value={usuario}
            onChange={(e) => setUsuario(e.target.value)}
            style={{ ...inp, width: 140 }}
          />
        </Field>
        <Field label="linhas por página">
          <select value={pageSize} onChange={(e) => setPageSize(Number(e.target.value))} style={{ ...inp, width: 90 }}>
            {[25, 50, 100, 200].map((n) => (
              <option key={n} value={n}>
                {n}
              </option>
            ))}
          </select>
        </Field>
        <div style={{ flex: 1 }} />
        <button onClick={fetchLog} style={btnSecondary}>
          atualizar
        </button>
      </div>

      {/* Info + paginação */}
      <div
        style={{
          display: "flex",
          alignItems: "center",
          gap: 10,
          padding: "8px 18px",
          background: "#f1f5f9",
          borderBottom: "1px solid #e2e8f0",
          fontSize: 12,
          color: "#475569",
          flex: "0 0 auto",
        }}
      >
        <span>
          <strong>{total}</strong> registro{total === 1 ? "" : "s"} · página <strong>{page}</strong> de <strong>{totalPages}</strong>
        </span>
        <div style={{ flex: 1 }} />
        <button onClick={() => setPage(1)} disabled={page === 1} style={pageBtn}>
          « primeira
        </button>
        <button onClick={() => setPage((p) => Math.max(1, p - 1))} disabled={page === 1} style={pageBtn}>
          ‹ anterior
        </button>
        <button onClick={() => setPage((p) => Math.min(totalPages, p + 1))} disabled={page >= totalPages} style={pageBtn}>
          próxima ›
        </button>
        <button onClick={() => setPage(totalPages)} disabled={page >= totalPages} style={pageBtn}>
          última »
        </button>
      </div>

      {/* Conteúdo */}
      <div style={{ flex: 1, overflow: "auto", padding: "16px 18px" }}>
        {error && <div style={{ color: "#dc2626" }}>{error}</div>}
        {loading && !items.length && <div style={{ color: "#64748b" }}>carregando…</div>}
        {!loading && items.length === 0 && !error && (
          <div style={{ color: "#94a3b8", textAlign: "center", padding: 40, fontSize: 13 }}>
            Nenhuma operação registrada ainda.
          </div>
        )}

        {items.length > 0 && (
          <table style={{ borderCollapse: "collapse", width: "100%", fontSize: 12.5 }}>
            <thead>
              <tr>
                <th style={thCell}>data/hora</th>
                <th style={thCell}>usuário</th>
                <th style={thCell}>operação</th>
                <th style={thCell}>recurso</th>
                <th style={thCell}>id</th>
                <th style={thCell}>detalhes</th>
              </tr>
            </thead>
            <tbody>
              {items.map((it) => {
                const op = opInfo(it.operacao);
                return (
                  <tr key={it.log_id} style={{ borderBottom: "1px solid #f1f5f9" }}>
                    <td style={{ ...tdCell, fontVariantNumeric: "tabular-nums", whiteSpace: "nowrap" }}>
                      {fmtTimestamp(it.timestamp)}
                    </td>
                    <td style={tdCell}>{it.usuario}</td>
                    <td style={tdCell}>
                      <span
                        style={{
                          background: op.bg,
                          color: op.fg,
                          padding: "2px 8px",
                          borderRadius: 10,
                          fontSize: 10,
                          fontWeight: 600,
                          textTransform: "uppercase",
                          letterSpacing: 0.3,
                        }}
                      >
                        {op.label}
                      </span>
                    </td>
                    <td style={tdCell}>
                      <code style={{ fontSize: 11, color: "#0f172a" }}>{it.recurso}</code>
                    </td>
                    <td style={tdCell}>
                      {it.recurso_id && (
                        <code style={{ fontSize: 11, color: "#64748b" }}>{it.recurso_id}</code>
                      )}
                    </td>
                    <td style={{ ...tdCell, color: "#64748b", fontSize: 11 }}>
                      {it.detalhes && <span title={it.detalhes}>{it.detalhes}</span>}
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        )}
      </div>
    </main>
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
  background: "#f1f5f9",
  borderBottom: "1px solid #cbd5e1",
  color: "#1d4ed8",
  fontWeight: 600,
  fontSize: 11,
  whiteSpace: "nowrap",
  position: "sticky",
  top: 0,
};
const tdCell: React.CSSProperties = {
  padding: "6px 10px",
  verticalAlign: "top",
};
const inp: React.CSSProperties = {
  padding: "6px 10px",
  border: "1px solid #cbd5e1",
  borderRadius: 4,
  fontSize: 13,
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
const pageBtn: React.CSSProperties = {
  background: "#ffffff",
  color: "#475569",
  border: "1px solid #cbd5e1",
  borderRadius: 4,
  padding: "3px 10px",
  fontSize: 11,
  cursor: "pointer",
  fontFamily: "inherit",
};
