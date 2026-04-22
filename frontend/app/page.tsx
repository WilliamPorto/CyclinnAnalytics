"use client";

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import CodeMirror from "@uiw/react-codemirror";
import { sql, PostgreSQL } from "@codemirror/lang-sql";
import type { EditorView } from "@codemirror/view";

type TableEntry = { table: string; row_count: number };
type SchemaMap = Record<string, TableEntry[]>;

type QueryResult = {
  columns: string[];
  rows: unknown[][];
  row_count: number;
  duration_ms: number;
  truncated: boolean;
};

const PAGE_SIZE_OPTIONS = [50, 100, 250, 500, 1000];

const DEFAULT_SQL = `-- exemplos:
-- SELECT * FROM cadastro.unidades LIMIT 10;
-- SELECT p.nome predio, COUNT(*) unidades FROM cadastro.unidades u JOIN cadastro.predios p USING(predio_id) GROUP BY p.nome ORDER BY unidades DESC;

SELECT table_schema, table_name
FROM information_schema.tables
WHERE table_schema NOT IN ('main','information_schema','pg_catalog')
ORDER BY 1, 2;`;

export default function Page() {
  const [schemas, setSchemas] = useState<SchemaMap>({});
  const [sqlText, setSqlText] = useState(DEFAULT_SQL);
  const [result, setResult] = useState<QueryResult | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const [collapsed, setCollapsed] = useState<Record<string, boolean>>({});
  const [hasSelection, setHasSelection] = useState(false);
  const [page, setPage] = useState(1);
  const [pageSize, setPageSize] = useState(100);
  const viewRef = useRef<EditorView | null>(null);

  // Reset para a página 1 quando o result ou o page_size mudam
  useEffect(() => {
    setPage(1);
  }, [result, pageSize]);

  const totalPages = result ? Math.max(1, Math.ceil(result.row_count / pageSize)) : 1;
  const pageStart = (page - 1) * pageSize;
  const pageEnd = Math.min(pageStart + pageSize, result?.row_count ?? 0);
  const pagedRows = useMemo(
    () => (result ? result.rows.slice(pageStart, pageEnd) : []),
    [result, pageStart, pageEnd]
  );

  useEffect(() => {
    fetch("/api/schemas")
      .then((r) => r.json())
      .then(setSchemas)
      .catch((e) => setError(String(e)));
  }, []);

  const runQuery = useCallback(async () => {
    const view = viewRef.current;
    let toRun = sqlText;
    if (view) {
      const { from, to } = view.state.selection.main;
      if (from !== to) {
        toRun = view.state.sliceDoc(from, to);
      }
    }
    if (!toRun.trim()) {
      setError("Nada para executar.");
      return;
    }

    setLoading(true);
    setError(null);
    try {
      const res = await fetch("/api/query", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ sql: toRun }),
      });
      const data = await res.json();
      if (!res.ok) {
        setError(data?.detail ?? `HTTP ${res.status}`);
        setResult(null);
      } else {
        setResult(data);
      }
    } catch (e) {
      setError(String(e));
      setResult(null);
    } finally {
      setLoading(false);
    }
  }, [sqlText]);

  const onKeyDown = useCallback(
    (e: React.KeyboardEvent) => {
      if ((e.ctrlKey || e.metaKey) && e.key === "Enter") {
        e.preventDefault();
        runQuery();
      }
    },
    [runQuery]
  );

  const insertAtCursor = useCallback((text: string) => {
    setSqlText((curr) => (curr.endsWith("\n") || curr.length === 0 ? curr + text : curr + "\n" + text));
  }, []);

  const extensions = useMemo(() => [sql({ dialect: PostgreSQL, upperCaseKeywords: true })], []);

  return (
    <main onKeyDown={onKeyDown} style={{ display: "grid", gridTemplateColumns: "280px 1fr", height: "100vh" }}>
      <aside
        style={{
          borderRight: "1px solid #e2e8f0",
          background: "#f8fafc",
          overflow: "auto",
          padding: "16px 12px",
        }}
      >
        <h1 style={{ fontSize: 14, letterSpacing: 0.3, margin: "0 0 12px 4px", color: "#1d4ed8" }}>
          cyclinn_pricing
        </h1>
        {Object.keys(schemas).length === 0 && (
          <div style={{ color: "#64748b", padding: 8 }}>carregando…</div>
        )}
        {Object.entries(schemas).map(([schema, tables]) => {
          const isCollapsed = collapsed[schema] ?? false;
          return (
            <div key={schema} style={{ marginBottom: 10 }}>
              <button
                onClick={() => setCollapsed((s) => ({ ...s, [schema]: !isCollapsed }))}
                style={{
                  width: "100%",
                  textAlign: "left",
                  background: "transparent",
                  border: 0,
                  color: "#b45309",
                  fontWeight: 600,
                  padding: "4px 6px",
                  fontSize: 13,
                }}
              >
                {isCollapsed ? "▸" : "▾"} {schema}
              </button>
              {!isCollapsed && (
                <ul style={{ listStyle: "none", margin: 0, padding: "2px 0 6px 18px" }}>
                  {tables.map((t) => (
                    <li key={t.table}>
                      <button
                        onClick={() => insertAtCursor(`SELECT * FROM ${schema}.${t.table} LIMIT 100;`)}
                        title={`${t.row_count} linhas — clicar insere SELECT no editor`}
                        style={{
                          display: "flex",
                          justifyContent: "space-between",
                          width: "100%",
                          background: "transparent",
                          border: 0,
                          color: "#1e293b",
                          padding: "3px 4px",
                          fontSize: 12.5,
                          fontFamily: "inherit",
                        }}
                      >
                        <span>{t.table}</span>
                        <span style={{ color: "#94a3b8", fontSize: 11 }}>{t.row_count}</span>
                      </button>
                    </li>
                  ))}
                </ul>
              )}
            </div>
          );
        })}
      </aside>

      <section style={{ display: "flex", flexDirection: "column", overflow: "hidden" }}>
        <div style={{ borderBottom: "1px solid #e2e8f0", height: "38vh", minHeight: 180 }}>
          <CodeMirror
            value={sqlText}
            height="100%"
            theme="light"
            extensions={extensions}
            onChange={setSqlText}
            onCreateEditor={(view) => (viewRef.current = view)}
            onUpdate={(upd) => {
              if (upd.selectionSet || upd.docChanged) {
                const { from, to } = upd.state.selection.main;
                setHasSelection(from !== to);
              }
            }}
            basicSetup={{ lineNumbers: true, highlightActiveLine: true, foldGutter: true }}
            style={{ height: "100%", fontSize: 13 }}
          />
        </div>

        <div
          style={{
            display: "flex",
            alignItems: "center",
            gap: 12,
            padding: "8px 14px",
            background: "#f8fafc",
            borderBottom: "1px solid #e2e8f0",
          }}
        >
          <button
            onClick={runQuery}
            disabled={loading}
            style={{
              background: "#2563eb",
              color: "white",
              border: 0,
              padding: "7px 16px",
              borderRadius: 5,
              fontWeight: 600,
            }}
          >
            {loading
              ? "Executando…"
              : hasSelection
              ? "▶ Run selection (Ctrl+Enter)"
              : "▶ Run (Ctrl+Enter)"}
          </button>
          {result && (
            <span style={{ color: "#64748b", fontSize: 12 }}>
              {result.row_count} linha{result.row_count === 1 ? "" : "s"} · {result.duration_ms} ms
              {result.truncated && (
                <span style={{ color: "#d97706", marginLeft: 8 }}>(truncado em 5000)</span>
              )}
            </span>
          )}
          {error && <span style={{ color: "#dc2626", fontSize: 12 }}>{error}</span>}
        </div>

        {result && result.row_count > 0 && (
          <Paginator
            page={page}
            totalPages={totalPages}
            pageStart={pageStart}
            pageEnd={pageEnd}
            rowCount={result.row_count}
            pageSize={pageSize}
            onChangePage={setPage}
            onChangePageSize={setPageSize}
          />
        )}

        <div style={{ flex: 1, overflow: "auto", padding: 0 }}>
          {result && <ResultTable result={result} rows={pagedRows} startIndex={pageStart} />}
        </div>
      </section>
    </main>
  );
}

type PaginatorProps = {
  page: number;
  totalPages: number;
  pageStart: number;
  pageEnd: number;
  rowCount: number;
  pageSize: number;
  onChangePage: (p: number) => void;
  onChangePageSize: (n: number) => void;
};

function Paginator(p: PaginatorProps) {
  const btn = (label: string, onClick: () => void, disabled: boolean) => (
    <button
      onClick={onClick}
      disabled={disabled}
      style={{
        background: "#ffffff",
        color: "#1e293b",
        border: "1px solid #cbd5e1",
        borderRadius: 4,
        padding: "4px 10px",
        fontSize: 12,
      }}
    >
      {label}
    </button>
  );
  const [input, setInput] = useState(String(p.page));
  useEffect(() => setInput(String(p.page)), [p.page]);
  const commit = () => {
    const n = Number(input);
    if (Number.isFinite(n) && n >= 1 && n <= p.totalPages) p.onChangePage(Math.floor(n));
    else setInput(String(p.page));
  };

  return (
    <div
      style={{
        display: "flex",
        alignItems: "center",
        gap: 8,
        padding: "6px 14px",
        background: "#f1f5f9",
        borderBottom: "1px solid #e2e8f0",
        fontSize: 12,
        color: "#475569",
      }}
    >
      {btn("« Primeira", () => p.onChangePage(1), p.page === 1)}
      {btn("‹ Anterior", () => p.onChangePage(Math.max(1, p.page - 1)), p.page === 1)}
      <span style={{ margin: "0 4px" }}>
        Página{" "}
        <input
          value={input}
          onChange={(e) => setInput(e.target.value)}
          onBlur={commit}
          onKeyDown={(e) => {
            if (e.key === "Enter") (e.target as HTMLInputElement).blur();
          }}
          style={{
            width: 48,
            padding: "2px 6px",
            textAlign: "right",
            border: "1px solid #cbd5e1",
            borderRadius: 4,
            fontSize: 12,
          }}
        />{" "}
        / {p.totalPages}
      </span>
      {btn("Próxima ›", () => p.onChangePage(Math.min(p.totalPages, p.page + 1)), p.page === p.totalPages)}
      {btn("Última »", () => p.onChangePage(p.totalPages), p.page === p.totalPages)}

      <span style={{ marginLeft: 16 }}>
        Mostrando <strong>{p.pageStart + 1}</strong>–<strong>{p.pageEnd}</strong> de{" "}
        <strong>{p.rowCount.toLocaleString("pt-BR")}</strong>
      </span>

      <span style={{ marginLeft: "auto" }}>
        Por página:{" "}
        <select
          value={p.pageSize}
          onChange={(e) => p.onChangePageSize(Number(e.target.value))}
          style={{ padding: "2px 6px", border: "1px solid #cbd5e1", borderRadius: 4, fontSize: 12 }}
        >
          {PAGE_SIZE_OPTIONS.map((n) => (
            <option key={n} value={n}>
              {n}
            </option>
          ))}
        </select>
      </span>
    </div>
  );
}

function ResultTable({
  result,
  rows,
  startIndex,
}: {
  result: QueryResult;
  rows: unknown[][];
  startIndex: number;
}) {
  if (result.columns.length === 0) {
    return <div style={{ padding: 16, color: "#64748b" }}>Query executada (sem colunas).</div>;
  }
  return (
    <table style={{ borderCollapse: "collapse", width: "100%", fontSize: 12.5 }}>
      <thead>
        <tr style={{ position: "sticky", top: 0, background: "#f1f5f9", zIndex: 1 }}>
          <th
            style={{
              textAlign: "right",
              padding: "8px 10px",
              borderBottom: "1px solid #cbd5e1",
              color: "#94a3b8",
              fontWeight: 500,
              whiteSpace: "nowrap",
              fontSize: 11,
            }}
          >
            #
          </th>
          {result.columns.map((c) => (
            <th
              key={c}
              style={{
                textAlign: "left",
                padding: "8px 12px",
                borderBottom: "1px solid #cbd5e1",
                color: "#1d4ed8",
                fontWeight: 600,
                whiteSpace: "nowrap",
              }}
            >
              {c}
            </th>
          ))}
        </tr>
      </thead>
      <tbody>
        {rows.map((row, i) => (
          <tr key={startIndex + i} style={{ background: i % 2 ? "#ffffff" : "#f8fafc" }}>
            <td
              style={{
                padding: "6px 10px",
                borderBottom: "1px solid #e2e8f0",
                color: "#94a3b8",
                textAlign: "right",
                fontSize: 11,
                whiteSpace: "nowrap",
              }}
            >
              {startIndex + i + 1}
            </td>
            {row.map((v, j) => (
              <td
                key={j}
                style={{
                  padding: "6px 12px",
                  borderBottom: "1px solid #e2e8f0",
                  whiteSpace: "nowrap",
                  color: v === null ? "#94a3b8" : "#0f172a",
                  fontFamily: typeof v === "number" ? "inherit" : "inherit",
                  textAlign: typeof v === "number" ? "right" : "left",
                }}
              >
                {v === null ? "NULL" : typeof v === "object" ? JSON.stringify(v) : String(v)}
              </td>
            ))}
          </tr>
        ))}
      </tbody>
    </table>
  );
}
