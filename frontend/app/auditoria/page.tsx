"use client";

import { useCallback, useEffect, useState } from "react";
import { Chip } from "../components/Chip";

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

  const opLabel = operacao
    ? OPERACOES_KNOWN[operacao]?.label ?? operacao
    : "todas";

  const filtersActive = [recurso, operacao, usuario].filter(Boolean).length;

  return (
    <main style={{ height: "100%", display: "flex", flexDirection: "column", overflow: "hidden" }}>
      {/* Toolbar única: título + chips + ações */}
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
        <span style={{ fontSize: 13, fontWeight: 600, color: "#1e293b", letterSpacing: -0.1 }}>
          Log de operações
        </span>
        {filtersActive > 0 && (
          <span
            style={{
              fontSize: 10,
              fontWeight: 600,
              color: "#4338ca",
              background: "#eef2ff",
              padding: "2px 7px",
              borderRadius: 10,
            }}
          >
            {filtersActive} filtro{filtersActive > 1 ? "s" : ""}
          </span>
        )}

        <div style={{ flex: 1 }} />

        {loading && (
          <span style={{ color: "#94a3b8", fontSize: 11, fontWeight: 500 }}>carregando…</span>
        )}
        {error && (
          <span style={{ color: "#dc2626", fontSize: 11, fontWeight: 500 }} title={error}>
            erro
          </span>
        )}

        <Chip icon={<IconFilter />} value={`Operação: ${opLabel}`} width={200}>
          {(close) => (
            <div style={{ display: "flex", flexDirection: "column", gap: 2 }}>
              {[
                { v: "", label: "Todas" },
                { v: "create", label: "Criar" },
                { v: "update", label: "Atualizar" },
                { v: "delete", label: "Excluir" },
                { v: "rebuild_simulador", label: "Rebuild simulador" },
              ].map((o) => {
                const active = operacao === o.v;
                return (
                  <button
                    key={o.v}
                    onClick={() => {
                      setOperacao(o.v);
                      close();
                    }}
                    style={menuItem(active)}
                  >
                    {o.label}
                  </button>
                );
              })}
            </div>
          )}
        </Chip>

        <Chip
          icon={<IconSearch />}
          value={recurso || usuario ? `${recurso || "—"} · ${usuario || "—"}` : "Recurso e usuário"}
          width={260}
        >
          {() => (
            <div style={{ display: "flex", flexDirection: "column", gap: 10 }}>
              <div>
                <Label>Recurso</Label>
                <input
                  placeholder="ex: regras.eventos"
                  value={recurso}
                  onChange={(e) => setRecurso(e.target.value)}
                  style={popInp}
                  autoFocus
                />
              </div>
              <div>
                <Label>Usuário</Label>
                <input
                  placeholder="ex: admin"
                  value={usuario}
                  onChange={(e) => setUsuario(e.target.value)}
                  style={popInp}
                />
              </div>
              {(recurso || usuario) && (
                <button
                  onClick={() => {
                    setRecurso("");
                    setUsuario("");
                  }}
                  style={{
                    padding: "5px 8px",
                    background: "#f8fafc",
                    border: "1px solid #e2e8f0",
                    borderRadius: 5,
                    fontSize: 11,
                    fontWeight: 500,
                    fontFamily: "inherit",
                    color: "#64748b",
                    cursor: "pointer",
                  }}
                >
                  Limpar
                </button>
              )}
            </div>
          )}
        </Chip>

        <Chip icon={<IconMore />} value="" width={200}>
          {(close) => (
            <div style={{ display: "flex", flexDirection: "column", gap: 4, fontSize: 12 }}>
              <div style={{ padding: "4px 6px" }}>
                <Label>Linhas por página</Label>
                <select
                  value={pageSize}
                  onChange={(e) => {
                    setPageSize(Number(e.target.value));
                    close();
                  }}
                  style={popInp}
                >
                  {[25, 50, 100, 200].map((n) => (
                    <option key={n} value={n}>
                      {n}
                    </option>
                  ))}
                </select>
              </div>
              <div style={{ height: 1, background: "#e2e8f0", margin: "4px 0" }} />
              <button
                onClick={() => {
                  fetchLog();
                  close();
                }}
                style={menuItem(false)}
              >
                ↻ Atualizar agora
              </button>
            </div>
          )}
        </Chip>
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
                <th style={thCell}>Data/hora</th>
                <th style={thCell}>Usuário</th>
                <th style={thCell}>Operação</th>
                <th style={thCell}>Recurso</th>
                <th style={thCell}>ID</th>
                <th style={thCell}>Detalhes</th>
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

      {/* Footer: paginação + total */}
      {items.length > 0 && (
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
            <strong style={{ color: "#1e293b" }}>{total}</strong> registro{total === 1 ? "" : "s"}
          </span>
        </div>
      )}
    </main>
  );
}

function IconFilter() {
  return (
    <svg width="13" height="13" viewBox="0 0 14 14" fill="none">
      <path
        d="M2 3h10l-3.5 4.5v4l-3 1v-5L2 3z"
        stroke="currentColor"
        strokeWidth="1.3"
        strokeLinejoin="round"
      />
    </svg>
  );
}

function IconSearch() {
  return (
    <svg width="13" height="13" viewBox="0 0 14 14" fill="none">
      <circle cx="6" cy="6" r="4" stroke="currentColor" strokeWidth="1.3" />
      <path d="M9 9l3 3" stroke="currentColor" strokeWidth="1.3" strokeLinecap="round" />
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

function Label({ children }: { children: React.ReactNode }) {
  return (
    <div
      style={{
        fontSize: 10,
        fontWeight: 500,
        color: "#64748b",
        marginBottom: 4,
        letterSpacing: 0.2,
        textTransform: "uppercase",
      }}
    >
      {children}
    </div>
  );
}

function menuItem(active: boolean): React.CSSProperties {
  return {
    display: "block",
    width: "100%",
    padding: "6px 10px",
    background: active ? "#eef2ff" : "transparent",
    border: 0,
    borderRadius: 5,
    color: active ? "#4338ca" : "#1e293b",
    fontWeight: active ? 600 : 500,
    fontSize: 12,
    fontFamily: "inherit",
    cursor: "pointer",
    textAlign: "left",
  };
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

const thCell: React.CSSProperties = {
  textAlign: "left",
  padding: "8px 10px",
  background: "#f8fafc",
  borderBottom: "1px solid #e2e8f0",
  color: "#4338ca",
  fontWeight: 600,
  fontSize: 11,
  whiteSpace: "nowrap",
  position: "sticky",
  top: 0,
  letterSpacing: -0.1,
};

const tdCell: React.CSSProperties = {
  padding: "6px 10px",
  verticalAlign: "top",
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
