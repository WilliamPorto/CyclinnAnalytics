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
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import duckdb
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

ROOT = Path(__file__).resolve().parent.parent
DATA_ROOT = ROOT / "data"
SIMULADOR_DB = ROOT / "simulador" / "backend" / "simulador.duckdb"
SIMULADOR_ALIAS = "simulador"  # catálogo anexado
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
    """Cria uma conexão DuckDB, registra parquets como views e anexa o simulador.duckdb."""
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

    # Anexa o banco do simulador (se existir) como catálogo read-only
    if SIMULADOR_DB.exists():
        sim_path = str(SIMULADOR_DB).replace("'", "''")
        con.execute(
            f"ATTACH '{sim_path}' AS {SIMULADOR_ALIAS} (READ_ONLY)"
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
    """Lista schemas + tabelas + contagem de linhas (default catalog + simulador anexado)."""
    result: dict[str, list[dict[str, Any]]] = {}

    # Views do catálogo default (parquets em /data/)
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

    # Tabelas do banco simulador anexado (se presente)
    attached = CON.execute(
        """
        SELECT table_name
        FROM duckdb_tables()
        WHERE database_name = ? AND NOT internal
        ORDER BY table_name
        """,
        [SIMULADOR_ALIAS],
    ).fetchall()
    for (table,) in attached:
        count = CON.execute(
            f'SELECT COUNT(*) FROM {SIMULADOR_ALIAS}.main."{table}"'
        ).fetchone()[0]
        result.setdefault(SIMULADOR_ALIAS, []).append(
            {"table": table, "row_count": count}
        )

    return result


@app.get("/schemas/{schema}/tables/{table}")
def describe_table(schema: str, table: str) -> TableInfo:
    try:
        if schema == SIMULADOR_ALIAS:
            cols = CON.execute(
                """
                SELECT column_name, data_type
                FROM duckdb_columns()
                WHERE database_name = ? AND table_name = ?
                ORDER BY column_index
                """,
                [SIMULADOR_ALIAS, table],
            ).fetchall()
            if not cols:
                raise HTTPException(
                    status_code=404, detail=f"{schema}.{table} não encontrado"
                )
            count = CON.execute(
                f'SELECT COUNT(*) FROM {SIMULADOR_ALIAS}.main."{table}"'
            ).fetchone()[0]
        else:
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
                raise HTTPException(
                    status_code=404, detail=f"{schema}.{table} não encontrado"
                )
            count = CON.execute(
                f'SELECT COUNT(*) FROM "{schema}"."{table}"'
            ).fetchone()[0]
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


# ============================================================
# Endpoints do simulador (dashboards)
# ============================================================

# (value_col, format, row_type)
SIMULADOR_TABLES: dict[str, tuple[str, str, str]] = {
    "pb":                    ("valor",                  "currency", "unidade"),
    "fat_sazonalidade":      ("ajuste_pct",             "percent",  "unidade"),
    "fat_dia_semana":        ("ajuste_pct",             "percent",  "unidade"),
    "fat_eventos":           ("ajuste_pct",             "percent",  "unidade"),
    "fat_antecedencia":      ("ajuste_pct",             "percent",  "unidade"),
    "fat_ajuste_portfolio":  ("ajuste_pct",             "percent",  "unidade"),
    "fat_ajuste_individual": ("ajuste_pct",             "percent",  "unidade"),
    "pi":                    ("valor",                  "currency", "unidade"),
    "d":                     ("valor",                  "currency", "unidade"),
    "ocupacao_portfolio":    ("ocupacao_pct",           "percent",  "portfolio"),
    "expectativa_portfolio": ("ocupacao_esperada_pct",  "percent",  "portfolio"),
}


@app.get("/simulador/tables")
def simulador_tables() -> dict:
    """Lista configuração das 11 tabelas-matriz para o dashboard."""
    return {
        name: {"value_col": v, "format": fmt, "row_type": rt}
        for name, (v, fmt, rt) in SIMULADOR_TABLES.items()
    }


@app.get("/simulador/data-referencias")
def simulador_data_referencias() -> dict:
    rows = CON.execute(
        f"""
        SELECT DISTINCT data_referencia
        FROM {SIMULADOR_ALIAS}.main.simulador_meta
        ORDER BY data_referencia DESC
        """
    ).fetchall()
    return {"values": [r[0].isoformat() for r in rows]}


@app.get("/simulador/matrix/{table}")
def simulador_matrix(
    table: str,
    data_referencia: str,
    data_inicio: str,
    data_fim: str,
    page: int = 1,
    page_size: int = 25,
) -> dict:
    if table not in SIMULADOR_TABLES:
        raise HTTPException(status_code=404, detail=f"Tabela '{table}' não suportada")

    value_col, fmt, row_type = SIMULADOR_TABLES[table]

    try:
        d_ref = date.fromisoformat(data_referencia)
        d_ini = date.fromisoformat(data_inicio)
        d_fim = date.fromisoformat(data_fim)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"Data inválida: {e}")

    if d_fim < d_ini:
        raise HTTPException(status_code=400, detail="data_fim anterior a data_inicio")

    page = max(1, int(page))
    page_size = max(1, min(200, int(page_size)))
    offset = (page - 1) * page_size

    # Linhas da página + total (unidades ou regiões)
    if row_type == "unidade":
        total = CON.execute("SELECT COUNT(*) FROM cadastro.unidades").fetchone()[0]
        label_rows = CON.execute(
            f"""
            SELECT unidade_id, codigo_externo
            FROM cadastro.unidades
            ORDER BY codigo_externo
            LIMIT {page_size} OFFSET {offset}
            """
        ).fetchall()
        key_col = "unidade_id"
    else:
        total = CON.execute("SELECT COUNT(*) FROM cadastro.regioes").fetchone()[0]
        label_rows = CON.execute(
            f"""
            SELECT regiao_id, nome
            FROM cadastro.regioes
            ORDER BY nome
            LIMIT {page_size} OFFSET {offset}
            """
        ).fetchall()
        key_col = "portfolio_id"

    ids = [r[0] for r in label_rows]
    labels = {r[0]: r[1] for r in label_rows}

    # Colunas de data
    date_cols: list[str] = []
    cur = d_ini
    while cur <= d_fim:
        date_cols.append(cur.isoformat())
        cur += timedelta(days=1)

    # Valores (apenas linhas da página)
    matrix_rows: list[dict] = []
    if ids:
        ids_list = ",".join(str(i) for i in ids)
        values_rows = CON.execute(
            f"""
            SELECT {key_col} AS id, data, {value_col} AS v
            FROM {SIMULADOR_ALIAS}.main.{table}
            WHERE data_referencia = DATE '{d_ref.isoformat()}'
              AND data BETWEEN DATE '{d_ini.isoformat()}' AND DATE '{d_fim.isoformat()}'
              AND {key_col} IN ({ids_list})
            """
        ).fetchall()
        lookup: dict[tuple[int, str], Any] = {}
        for rid, dt, v in values_rows:
            lookup[(rid, dt.isoformat())] = v
        for rid in ids:
            vals = [lookup.get((rid, d)) for d in date_cols]
            matrix_rows.append({"id": rid, "label": labels[rid], "values": vals})

    # Min/max globais (considerando TODAS as linhas no período — não só a página)
    stats = CON.execute(
        f"""
        SELECT MIN({value_col}), MAX({value_col})
        FROM {SIMULADOR_ALIAS}.main.{table}
        WHERE data_referencia = DATE '{d_ref.isoformat()}'
          AND data BETWEEN DATE '{d_ini.isoformat()}' AND DATE '{d_fim.isoformat()}'
        """
    ).fetchone()
    vmin = float(stats[0]) if stats[0] is not None else 0.0
    vmax = float(stats[1]) if stats[1] is not None else 0.0

    return {
        "table": table,
        "data_referencia": data_referencia,
        "data_inicio": data_inicio,
        "data_fim": data_fim,
        "format": fmt,
        "row_type": row_type,
        "columns": date_cols,
        "rows": matrix_rows,
        "total_rows": total,
        "page": page,
        "page_size": page_size,
        "min": vmin,
        "max": vmax,
    }


@app.post("/reload")
def reload_views() -> dict:
    """Recarrega as views (útil após regenerar parquets)."""
    global CON
    CON.close()
    CON = build_duckdb()
    return {"ok": True, "reloaded_at": time.time()}
