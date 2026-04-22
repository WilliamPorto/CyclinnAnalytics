"""
Backend FastAPI para consulta da base parquet via DuckDB.

Cada parquet em data/<schema>/<tabela>/<tabela>.parquet é exposto como view
DuckDB no schema correspondente. Usuário pode consultar com SQL livre via
POST /query.

Execução:
  .venv/bin/uvicorn backend.main:app --reload --port 8000
"""

from __future__ import annotations

import time
from pathlib import Path
from typing import Any

import duckdb
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

ROOT = Path(__file__).resolve().parent.parent
DATA_ROOT = ROOT / "data"
MAX_ROWS = 5000
MAX_QUERY_MS = 15_000

app = FastAPI(title="Cyclinn Pricing DB", version="0.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


def build_duckdb() -> duckdb.DuckDBPyConnection:
    """Cria uma conexão DuckDB e registra cada parquet como view no schema."""
    con = duckdb.connect()
    if not DATA_ROOT.exists():
        raise RuntimeError(f"DATA_ROOT não existe: {DATA_ROOT}")

    for schema_dir in sorted(DATA_ROOT.iterdir()):
        if not schema_dir.is_dir():
            continue
        schema = schema_dir.name
        con.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
        for table_dir in sorted(schema_dir.iterdir()):
            if not table_dir.is_dir():
                continue
            table = table_dir.name
            parquet_files = list(table_dir.glob("*.parquet"))
            if not parquet_files:
                continue
            glob = str(table_dir / "*.parquet").replace("'", "''")
            con.execute(
                f"""CREATE OR REPLACE VIEW "{schema}"."{table}" AS
                SELECT * FROM read_parquet('{glob}')"""
            )
    return con


CON = build_duckdb()


class QueryRequest(BaseModel):
    sql: str = Field(..., min_length=1, max_length=50_000)


class QueryResponse(BaseModel):
    columns: list[str]
    rows: list[list[Any]]
    row_count: int
    duration_ms: int
    truncated: bool


class TableInfo(BaseModel):
    schema_name: str = Field(..., alias="schema")
    table: str
    row_count: int
    columns: list[dict[str, str]]

    class Config:
        populate_by_name = True


@app.get("/health")
def health() -> dict:
    return {"ok": True}


@app.get("/schemas")
def list_schemas() -> dict:
    """Lista schemas + tabelas + contagem de linhas."""
    result: dict[str, list[dict[str, Any]]] = {}
    rows = CON.execute(
        """
        SELECT table_schema, table_name
        FROM information_schema.tables
        WHERE table_type IN ('VIEW', 'BASE TABLE')
          AND table_schema NOT IN ('main', 'information_schema', 'pg_catalog')
        ORDER BY table_schema, table_name
        """
    ).fetchall()
    for schema, table in rows:
        count = CON.execute(f'SELECT COUNT(*) FROM "{schema}"."{table}"').fetchone()[0]
        result.setdefault(schema, []).append({"table": table, "row_count": count})
    return result


@app.get("/schemas/{schema}/tables/{table}")
def describe_table(schema: str, table: str) -> TableInfo:
    try:
        cols = CON.execute(
            """
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = ? AND table_name = ?
            ORDER BY ordinal_position
            """,
            [schema, table],
        ).fetchall()
        if not cols:
            raise HTTPException(status_code=404, detail=f"{schema}.{table} não encontrado")
        count = CON.execute(f'SELECT COUNT(*) FROM "{schema}"."{table}"').fetchone()[0]
    except duckdb.Error as e:
        raise HTTPException(status_code=400, detail=str(e))
    return TableInfo(
        schema=schema,
        table=table,
        row_count=count,
        columns=[{"name": c[0], "type": c[1]} for c in cols],
    )


@app.post("/query", response_model=QueryResponse)
def run_query(req: QueryRequest) -> QueryResponse:
    sql = req.sql.strip().rstrip(";")
    if not sql:
        raise HTTPException(status_code=400, detail="SQL vazio")

    # Bloqueia DDL/DML que alterariam estado
    lowered = sql.lower()
    forbidden = ("insert ", "update ", "delete ", "drop ", "alter ", "attach ", "copy ", "pragma ")
    if any(lowered.startswith(k) or f";{k}" in lowered for k in forbidden):
        raise HTTPException(status_code=400, detail="Apenas SELECT/WITH é permitido.")

    # Envolve em subquery para aplicar LIMIT de forma segura
    wrapped = f"SELECT * FROM ({sql}) AS q LIMIT {MAX_ROWS + 1}"
    start = time.perf_counter()
    try:
        cur = CON.execute(wrapped)
    except duckdb.Error as e:
        raise HTTPException(status_code=400, detail=f"Erro SQL: {e}")

    columns = [d[0] for d in cur.description]
    rows = cur.fetchall()
    duration_ms = int((time.perf_counter() - start) * 1000)

    truncated = len(rows) > MAX_ROWS
    if truncated:
        rows = rows[:MAX_ROWS]

    # Coagir tipos para JSON-serializável
    out_rows: list[list[Any]] = []
    for r in rows:
        out_row = []
        for v in r:
            if v is None:
                out_row.append(None)
            elif hasattr(v, "isoformat"):
                out_row.append(v.isoformat())
            elif isinstance(v, (bytes, bytearray)):
                out_row.append(v.hex())
            else:
                out_row.append(v)
        out_rows.append(out_row)

    return QueryResponse(
        columns=columns,
        rows=out_rows,
        row_count=len(out_rows),
        duration_ms=duration_ms,
        truncated=truncated,
    )


@app.post("/reload")
def reload_views() -> dict:
    """Recarrega as views (útil após regenerar parquets)."""
    global CON
    CON.close()
    CON = build_duckdb()
    return {"ok": True, "reloaded_at": time.time()}
