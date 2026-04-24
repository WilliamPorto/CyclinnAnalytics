"""
Backend FastAPI para consulta da base parquet via DuckDB.

Cada parquet em data/<schema>/<tabela>/<tabela>.parquet é exposto como view
DuckDB no schema correspondente. Usuário pode consultar com SQL livre via
POST /query.

Execução:
  .venv/bin/uvicorn backend.main:app --reload --port 8000
"""

from __future__ import annotations

import json
import subprocess
import sys
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional

import duckdb
import pandas as pd
from fastapi import FastAPI, Header, HTTPException
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
    view: str = "unidade",  # "unidade" | "portfolio"
) -> dict:
    if table not in SIMULADOR_TABLES:
        raise HTTPException(status_code=404, detail=f"Tabela '{table}' não suportada")
    if view not in ("unidade", "portfolio"):
        raise HTTPException(status_code=400, detail=f"view inválido: {view}")

    value_col, fmt, native_row_type = SIMULADOR_TABLES[table]
    # Para tabelas que já são nativamente por portfólio, ignoramos "view".
    effective_row_type = native_row_type if native_row_type == "portfolio" else view

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

    # Labels (unidades × codigo_externo) ou (regioes × nome)
    if effective_row_type == "unidade":
        total = CON.execute("SELECT COUNT(*) FROM cadastro.unidades").fetchone()[0]
        label_rows = CON.execute(
            f"""
            SELECT unidade_id, codigo_externo
            FROM cadastro.unidades
            ORDER BY codigo_externo
            LIMIT {page_size} OFFSET {offset}
            """
        ).fetchall()
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

    ids = [r[0] for r in label_rows]
    labels = {r[0]: r[1] for r in label_rows}

    date_cols: list[str] = []
    cur = d_ini
    while cur <= d_fim:
        date_cols.append(cur.isoformat())
        cur += timedelta(days=1)

    d_ref_s = d_ref.isoformat()
    d_ini_s = d_ini.isoformat()
    d_fim_s = d_fim.isoformat()

    needs_aggregation = effective_row_type == "portfolio" and native_row_type == "unidade"

    # Define se a tabela injeta uma coluna color_pct (usada pra heatmap por
    # diferença em vez de pelo valor absoluto).
    #   pi                  → color_pct = (pi − pb) / pb
    #   ocupacao_portfolio  → color_pct = real − esperada
    has_color_pct = table in ("pi", "ocupacao_portfolio")

    def build_values_sql(ids_filter: str) -> str:
        # Ocupação real: v = real, color_pct = (real − esperada) → heatmap pelo gap
        if table == "ocupacao_portfolio":
            return f"""
                SELECT o.portfolio_id AS id, o.data,
                       o.ocupacao_pct AS v,
                       (o.ocupacao_pct - e.ocupacao_esperada_pct) AS color_pct
                FROM {SIMULADOR_ALIAS}.main.ocupacao_portfolio o
                JOIN {SIMULADOR_ALIAS}.main.expectativa_portfolio e
                  USING(data_referencia, portfolio_id, data)
                WHERE o.data_referencia = DATE '{d_ref_s}'
                  AND o.data BETWEEN DATE '{d_ini_s}' AND DATE '{d_fim_s}'
                  {ids_filter.replace('portfolio_id', 'o.portfolio_id')}
            """
        # pi: retorna também o % de diferença vs Pb (= soma dos fatores a priori)
        if table == "pi":
            if not needs_aggregation:
                return f"""
                    SELECT pi.unidade_id AS id, pi.data, pi.valor AS v,
                           (pi.valor - pb.valor) / pb.valor AS color_pct
                    FROM {SIMULADOR_ALIAS}.main.pi pi
                    JOIN {SIMULADOR_ALIAS}.main.pb pb
                      USING(data_referencia, unidade_id, data)
                    WHERE pi.data_referencia = DATE '{d_ref_s}'
                      AND pi.data BETWEEN DATE '{d_ini_s}' AND DATE '{d_fim_s}'
                      {ids_filter.replace('unidade_id', 'pi.unidade_id')}
                """
            return f"""
                SELECT p.regiao_id AS id, pi.data,
                       AVG(pi.valor) AS v,
                       AVG((pi.valor - pb.valor) / pb.valor) AS color_pct
                FROM {SIMULADOR_ALIAS}.main.pi pi
                JOIN {SIMULADOR_ALIAS}.main.pb pb
                  USING(data_referencia, unidade_id, data)
                JOIN cadastro.unidades u ON u.unidade_id = pi.unidade_id
                JOIN cadastro.predios p USING(predio_id)
                WHERE pi.data_referencia = DATE '{d_ref_s}'
                  AND pi.data BETWEEN DATE '{d_ini_s}' AND DATE '{d_fim_s}'
                  {ids_filter}
                GROUP BY p.regiao_id, pi.data
            """
        if not needs_aggregation:
            key_col = "unidade_id" if native_row_type == "unidade" else "portfolio_id"
            return f"""
                SELECT {key_col} AS id, data, {value_col} AS v, NULL::DOUBLE AS color_pct
                FROM {SIMULADOR_ALIAS}.main.{table}
                WHERE data_referencia = DATE '{d_ref_s}'
                  AND data BETWEEN DATE '{d_ini_s}' AND DATE '{d_fim_s}'
                  {ids_filter}
            """
        # Agrega por região (média simples)
        return f"""
            SELECT p.regiao_id AS id, t.data, AVG(t.{value_col}) AS v, NULL::DOUBLE AS color_pct
            FROM {SIMULADOR_ALIAS}.main.{table} t
            JOIN cadastro.unidades u ON u.unidade_id = t.unidade_id
            JOIN cadastro.predios p USING(predio_id)
            WHERE t.data_referencia = DATE '{d_ref_s}'
              AND t.data BETWEEN DATE '{d_ini_s}' AND DATE '{d_fim_s}'
              {ids_filter}
            GROUP BY p.regiao_id, t.data
        """

    matrix_rows: list[dict] = []
    if ids:
        ids_list = ",".join(str(i) for i in ids)
        if needs_aggregation:
            ids_filter = f"AND p.regiao_id IN ({ids_list})"
        else:
            key_col = "unidade_id" if native_row_type == "unidade" else "portfolio_id"
            ids_filter = f"AND {key_col} IN ({ids_list})"
        values_rows = CON.execute(build_values_sql(ids_filter)).fetchall()
        lookup: dict[tuple[int, str], Any] = {}
        color_lookup: dict[tuple[int, str], Any] = {}
        for row in values_rows:
            rid, dt, v = row[0], row[1], row[2]
            lookup[(rid, dt.isoformat())] = v
            if len(row) >= 4 and row[3] is not None:
                color_lookup[(rid, dt.isoformat())] = float(row[3])
        for rid in ids:
            vals = [lookup.get((rid, d)) for d in date_cols]
            item: dict[str, Any] = {"id": rid, "label": labels[rid], "values": vals}
            if has_color_pct:
                item["color_values"] = [color_lookup.get((rid, d)) for d in date_cols]
            matrix_rows.append(item)

    # Min/max global no período (para heatmap consistente entre páginas)
    stats = CON.execute(
        f"SELECT MIN(v), MAX(v), MIN(color_pct), MAX(color_pct) FROM ({build_values_sql('')}) q"
    ).fetchone()
    vmin = float(stats[0]) if stats[0] is not None else 0.0
    vmax = float(stats[1]) if stats[1] is not None else 0.0
    color_min = float(stats[2]) if has_color_pct and stats[2] is not None else None
    color_max = float(stats[3]) if has_color_pct and stats[3] is not None else None

    return {
        "table": table,
        "data_referencia": data_referencia,
        "data_inicio": data_inicio,
        "data_fim": data_fim,
        "format": fmt,
        "row_type": effective_row_type,
        "view": view,
        "aggregated": needs_aggregation,
        "columns": date_cols,
        "rows": matrix_rows,
        "total_rows": total,
        "page": page,
        "page_size": page_size,
        "min": vmin,
        "max": vmax,
        "color_min": color_min,
        "color_max": color_max,
        "color_format": "percent" if has_color_pct else None,
    }


# ============================================================
# Auditoria (log de operações mutáveis)
# ============================================================

AUDITORIA_PARQUET = (
    DATA_ROOT / "auditoria" / "log_operacoes" / "log_operacoes.parquet"
)
USUARIO_PADRAO = "admin"


def _get_usuario(x_usuario: Optional[str]) -> str:
    """Lê o header X-Usuario ou cai no padrão 'admin'."""
    if x_usuario and x_usuario.strip():
        return x_usuario.strip()[:120]
    return USUARIO_PADRAO


def _read_auditoria_df() -> pd.DataFrame:
    if not AUDITORIA_PARQUET.exists():
        return pd.DataFrame(
            columns=["log_id", "timestamp", "usuario", "operacao", "recurso", "recurso_id", "detalhes"]
        )
    return pd.read_parquet(AUDITORIA_PARQUET)


def _write_auditoria_df(df: pd.DataFrame) -> None:
    AUDITORIA_PARQUET.parent.mkdir(parents=True, exist_ok=True)
    if not df.empty:
        df = df.copy()
        df["log_id"] = df["log_id"].astype("int64")
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        for col in ("usuario", "operacao", "recurso"):
            df[col] = df[col].astype(str)
    df.to_parquet(AUDITORIA_PARQUET, index=False)


def log_operacao(
    usuario: str,
    operacao: str,
    recurso: str,
    recurso_id: Optional[str] = None,
    detalhes: Optional[dict[str, Any]] = None,
) -> None:
    """Adiciona uma linha no log de auditoria."""
    df = _read_auditoria_df()
    next_id = int(df["log_id"].max()) + 1 if not df.empty else 1
    nova = {
        "log_id": next_id,
        "timestamp": datetime.now(timezone.utc),
        "usuario": usuario,
        "operacao": operacao,
        "recurso": recurso,
        "recurso_id": str(recurso_id) if recurso_id is not None else None,
        "detalhes": json.dumps(detalhes, default=str, ensure_ascii=False) if detalhes else None,
    }
    df = pd.concat([df, pd.DataFrame([nova])], ignore_index=True)
    _write_auditoria_df(df)


@app.get("/auditoria/operacoes")
def listar_auditoria(
    page: int = 1,
    page_size: int = 50,
    recurso: Optional[str] = None,
    operacao: Optional[str] = None,
    usuario: Optional[str] = None,
) -> dict:
    df = _read_auditoria_df()
    if recurso:
        df = df[df["recurso"].str.startswith(recurso)]
    if operacao:
        df = df[df["operacao"] == operacao]
    if usuario:
        df = df[df["usuario"] == usuario]

    total = len(df)
    df = df.sort_values("timestamp", ascending=False)
    page = max(1, int(page))
    page_size = max(1, min(500, int(page_size)))
    offset = (page - 1) * page_size
    slice_df = df.iloc[offset : offset + page_size]

    items = []
    for _, r in slice_df.iterrows():
        ts = r["timestamp"]
        items.append({
            "log_id": int(r["log_id"]),
            "timestamp": ts.isoformat() if hasattr(ts, "isoformat") else str(ts),
            "usuario": r["usuario"],
            "operacao": r["operacao"],
            "recurso": r["recurso"],
            "recurso_id": None if pd.isna(r["recurso_id"]) else r["recurso_id"],
            "detalhes": None if pd.isna(r["detalhes"]) else r["detalhes"],
        })
    return {
        "total": total,
        "page": page,
        "page_size": page_size,
        "items": items,
    }


# ============================================================
# Regras — CRUD de sazonalidade (grava no parquet direto)
# ============================================================

SAZONALIDADE_PARQUET = (
    DATA_ROOT / "regras_priori" / "regras_sazonalidade" / "regras_sazonalidade.parquet"
)
SIMULADOR_BUILD_SCRIPT = ROOT / "simulador" / "backend" / "build_simulator_db.py"


def _read_sazonalidade_df() -> pd.DataFrame:
    if not SAZONALIDADE_PARQUET.exists():
        return pd.DataFrame(
            columns=[
                "regra_id", "escopo", "escopo_id", "nome",
                "data_inicio", "data_fim", "ajuste_pct",
                "recorrente_anual", "prioridade",
            ]
        )
    df = pd.read_parquet(SAZONALIDADE_PARQUET)
    if "ativo" in df.columns:
        df = df.drop(columns=["ativo"])
    return df


def _write_sazonalidade_df(df: pd.DataFrame) -> None:
    SAZONALIDADE_PARQUET.parent.mkdir(parents=True, exist_ok=True)
    if not df.empty:
        df = df.copy()
        df["data_inicio"] = pd.to_datetime(df["data_inicio"]).dt.date
        df["data_fim"] = pd.to_datetime(df["data_fim"]).dt.date
        df["escopo_id"] = df["escopo_id"].astype("Int64")
        df["regra_id"] = df["regra_id"].astype("int64")
        df["ajuste_pct"] = df["ajuste_pct"].astype(float)
        df["prioridade"] = df["prioridade"].astype("int64")
        df["recorrente_anual"] = df["recorrente_anual"].astype(bool)
    df.to_parquet(SAZONALIDADE_PARQUET, index=False)


def _serialize_regra(row: pd.Series) -> dict:
    def iso(v: Any) -> Optional[str]:
        if pd.isna(v):
            return None
        if hasattr(v, "isoformat"):
            return v.isoformat()
        return str(v)

    return {
        "regra_id": int(row["regra_id"]),
        "escopo": row["escopo"],
        "escopo_id": (None if pd.isna(row["escopo_id"]) else int(row["escopo_id"])),
        "nome": row["nome"],
        "data_inicio": iso(row["data_inicio"]),
        "data_fim": iso(row["data_fim"]),
        "ajuste_pct": float(row["ajuste_pct"]),
        "recorrente_anual": bool(row["recorrente_anual"]),
        "prioridade": int(row["prioridade"]),
    }


class SazonalidadeIn(BaseModel):
    nome: str = Field(..., min_length=1, max_length=120)
    data_inicio: str
    data_fim: str
    ajuste_pct: float = Field(..., ge=-1.0, le=3.0)
    escopo: str = Field(..., pattern="^(global|regiao|predio|segmento|unidade)$")
    escopo_id: Optional[int] = None
    recorrente_anual: bool = True
    prioridade: int = 10


class SazonalidadePatch(BaseModel):
    nome: Optional[str] = None
    data_inicio: Optional[str] = None
    data_fim: Optional[str] = None
    ajuste_pct: Optional[float] = None
    escopo: Optional[str] = None
    escopo_id: Optional[int] = None
    recorrente_anual: Optional[bool] = None
    prioridade: Optional[int] = None


def _validar_datas(d_ini_s: str, d_fim_s: str) -> tuple[date, date]:
    try:
        d_ini = date.fromisoformat(d_ini_s)
        d_fim = date.fromisoformat(d_fim_s)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"Data inválida: {e}")
    if d_fim < d_ini:
        raise HTTPException(status_code=400, detail="data_fim anterior a data_inicio")
    return d_ini, d_fim


def _validar_escopo(escopo: str, escopo_id: Optional[int]) -> None:
    if escopo == "global":
        if escopo_id is not None:
            raise HTTPException(status_code=400, detail="escopo global não deve ter escopo_id")
        return
    if escopo_id is None:
        raise HTTPException(
            status_code=400, detail=f"escopo '{escopo}' requer escopo_id"
        )
    # Valida se o escopo_id existe no catálogo
    table_map = {
        "regiao": "cadastro.regioes",
        "predio": "cadastro.predios",
        "segmento": "cadastro.segmentos",
        "unidade": "cadastro.unidades",
    }
    col_map = {
        "regiao": "regiao_id",
        "predio": "predio_id",
        "segmento": "segmento_id",
        "unidade": "unidade_id",
    }
    t = table_map[escopo]
    c = col_map[escopo]
    exists = CON.execute(f"SELECT COUNT(*) FROM {t} WHERE {c} = ?", [escopo_id]).fetchone()[0]
    if not exists:
        raise HTTPException(
            status_code=400, detail=f"{escopo}_id={escopo_id} não encontrado"
        )


@app.get("/regras/sazonalidade")
def listar_sazonalidade() -> list[dict]:
    df = _read_sazonalidade_df()
    df = df.sort_values("data_inicio", ascending=True)
    return [_serialize_regra(r) for _, r in df.iterrows()]


@app.post("/regras/sazonalidade", status_code=201)
def criar_sazonalidade(
    body: SazonalidadeIn,
    x_usuario: Optional[str] = Header(default=None, alias="X-Usuario"),
) -> dict:
    d_ini, d_fim = _validar_datas(body.data_inicio, body.data_fim)
    _validar_escopo(body.escopo, body.escopo_id)

    df = _read_sazonalidade_df()
    next_id = int(df["regra_id"].max()) + 1 if not df.empty else 1

    nova = {
        "regra_id": next_id,
        "escopo": body.escopo,
        "escopo_id": body.escopo_id,
        "nome": body.nome,
        "data_inicio": d_ini,
        "data_fim": d_fim,
        "ajuste_pct": body.ajuste_pct,
        "recorrente_anual": body.recorrente_anual,
        "prioridade": body.prioridade,
    }
    df = pd.concat([df, pd.DataFrame([nova])], ignore_index=True)
    _write_sazonalidade_df(df)
    log_operacao(
        _get_usuario(x_usuario), "create",
        "regras.sazonalidade", str(next_id),
        {"nome": body.nome, "ajuste_pct": body.ajuste_pct, "escopo": body.escopo},
    )
    return _serialize_regra(pd.Series(nova))


@app.patch("/regras/sazonalidade/{regra_id}")
def editar_sazonalidade(
    regra_id: int, body: SazonalidadePatch,
    x_usuario: Optional[str] = Header(default=None, alias="X-Usuario"),
) -> dict:
    df = _read_sazonalidade_df()
    mask = df["regra_id"] == regra_id
    if not mask.any():
        raise HTTPException(status_code=404, detail=f"regra {regra_id} não encontrada")

    patch_dict = body.model_dump(exclude_none=True)

    if "data_inicio" in patch_dict or "data_fim" in patch_dict:
        row = df[mask].iloc[0]
        d_ini_s = patch_dict.get("data_inicio") or str(row["data_inicio"])
        d_fim_s = patch_dict.get("data_fim") or str(row["data_fim"])
        d_ini, d_fim = _validar_datas(d_ini_s, d_fim_s)
        patch_dict["data_inicio"] = d_ini
        patch_dict["data_fim"] = d_fim

    if "escopo" in patch_dict or "escopo_id" in patch_dict:
        row = df[mask].iloc[0]
        escopo = patch_dict.get("escopo") or row["escopo"]
        escopo_id = patch_dict.get("escopo_id")
        if "escopo_id" not in patch_dict:
            escopo_id = None if pd.isna(row["escopo_id"]) else int(row["escopo_id"])
        _validar_escopo(escopo, escopo_id)
        patch_dict["escopo"] = escopo
        patch_dict["escopo_id"] = escopo_id

    for col, val in patch_dict.items():
        df.loc[mask, col] = val

    _write_sazonalidade_df(df)
    log_operacao(
        _get_usuario(x_usuario), "update",
        "regras.sazonalidade", str(regra_id),
        {k: (v.isoformat() if hasattr(v, "isoformat") else v) for k, v in patch_dict.items()},
    )
    return _serialize_regra(df[mask].iloc[0])


@app.delete("/regras/sazonalidade/{regra_id}")
def deletar_sazonalidade(
    regra_id: int,
    x_usuario: Optional[str] = Header(default=None, alias="X-Usuario"),
) -> dict:
    df = _read_sazonalidade_df()
    mask = df["regra_id"] == regra_id
    if not mask.any():
        raise HTTPException(status_code=404, detail=f"regra {regra_id} não encontrada")
    removida = df[mask].iloc[0]
    nome = str(removida["nome"])
    df = df[~mask].copy()
    _write_sazonalidade_df(df)
    log_operacao(
        _get_usuario(x_usuario), "delete",
        "regras.sazonalidade", str(regra_id),
        {"nome": nome},
    )
    return {"ok": True}


@app.get("/regras/escopo/{tipo}")
def listar_opcoes_escopo(tipo: str) -> list[dict]:
    queries = {
        "regiao": "SELECT regiao_id AS id, nome FROM cadastro.regioes ORDER BY nome",
        "predio": "SELECT predio_id AS id, nome FROM cadastro.predios ORDER BY nome",
        "segmento": "SELECT segmento_id AS id, nome FROM cadastro.segmentos ORDER BY nome",
        "unidade": (
            "SELECT unidade_id AS id, codigo_externo AS nome "
            "FROM cadastro.unidades ORDER BY codigo_externo"
        ),
    }
    if tipo not in queries:
        raise HTTPException(status_code=404, detail=f"tipo '{tipo}' não suportado")
    rows = CON.execute(queries[tipo]).fetchall()
    return [{"id": r[0], "nome": r[1]} for r in rows]


# ─── Dia da semana (matriz escopo × DOW) ───────────────────────

DIA_SEMANA_PARQUET = (
    DATA_ROOT / "regras_priori" / "regras_dia_semana" / "regras_dia_semana.parquet"
)


def _read_dia_semana_df() -> pd.DataFrame:
    if not DIA_SEMANA_PARQUET.exists():
        return pd.DataFrame(
            columns=["regra_id", "escopo", "escopo_id", "dia_semana", "ajuste_pct"]
        )
    df = pd.read_parquet(DIA_SEMANA_PARQUET)
    if "ativo" in df.columns:
        df = df.drop(columns=["ativo"])
    return df


def _write_dia_semana_df(df: pd.DataFrame) -> None:
    DIA_SEMANA_PARQUET.parent.mkdir(parents=True, exist_ok=True)
    if not df.empty:
        df = df.copy()
        df["regra_id"] = df["regra_id"].astype("int64")
        df["escopo_id"] = df["escopo_id"].astype("Int64")
        df["dia_semana"] = df["dia_semana"].astype("int64")
        df["ajuste_pct"] = df["ajuste_pct"].astype(float)
    df.to_parquet(DIA_SEMANA_PARQUET, index=False)


@app.get("/regras/dia-semana/matriz")
def matriz_dia_semana() -> dict:
    """Retorna a matriz (escopo × DOW).

    Estrutura:
      {
        escopos: [{escopo, escopo_id, nome, ativo, values: [seg,ter,qua,qui,sex,sab,dom]}],
      }
    Somente escopos global, regiao e predio. Inclui inativos (ativo=false).
    """
    df = _read_dia_semana_df()
    df = df[df["escopo"].isin(["global", "regiao", "predio"])]

    # Agrupa por (escopo, escopo_id): 7 valores por DOW + ativo da linha
    grupos: list[dict] = []
    if not df.empty:
        for (escopo, esc_id), g in df.groupby(["escopo", "escopo_id"], dropna=False):
            values = [0.0] * 7
            for _, row in g.iterrows():
                dow = int(row["dia_semana"])
                if 0 <= dow < 7:
                    values[dow] = float(row["ajuste_pct"])
            grupos.append(
                {
                    "escopo": escopo,
                    "escopo_id": None if pd.isna(esc_id) else int(esc_id),
                    "values": values,
                }
            )

    # Decora com nome do escopo
    def nome_escopo(escopo: str, esc_id: Optional[int]) -> str:
        if escopo == "global":
            return "Global (default)"
        tables = {"regiao": "cadastro.regioes", "predio": "cadastro.predios"}
        cols = {"regiao": "regiao_id", "predio": "predio_id"}
        if escopo not in tables or esc_id is None:
            return f"{escopo} #{esc_id}"
        r = CON.execute(
            f"SELECT nome FROM {tables[escopo]} WHERE {cols[escopo]} = ?", [esc_id]
        ).fetchone()
        return r[0] if r else f"{escopo} #{esc_id}"

    ordem = {"global": 0, "regiao": 1, "predio": 2}
    for g in grupos:
        g["nome"] = nome_escopo(g["escopo"], g["escopo_id"])
    grupos.sort(key=lambda g: (ordem.get(g["escopo"], 9), g["nome"]))

    return {"escopos": grupos}


class NovoEscopoDiaSemana(BaseModel):
    escopo: str = Field(..., pattern="^(global|regiao|predio)$")
    escopo_id: Optional[int] = None


@app.post("/regras/dia-semana/escopo", status_code=201)
def criar_escopo_dia_semana(
    body: NovoEscopoDiaSemana,
    x_usuario: Optional[str] = Header(default=None, alias="X-Usuario"),
) -> dict:
    _validar_escopo(body.escopo, body.escopo_id)

    df = _read_dia_semana_df()
    if body.escopo == "global":
        mask = df["escopo"] == "global"
    else:
        mask = (df["escopo"] == body.escopo) & (df["escopo_id"] == body.escopo_id)
    if mask.any():
        raise HTTPException(
            status_code=409, detail="Esse escopo já tem regras cadastradas"
        )

    global_rows = df[df["escopo"] == "global"]
    defaults = [0.0] * 7
    if not global_rows.empty:
        for _, r in global_rows.iterrows():
            dow = int(r["dia_semana"])
            if 0 <= dow < 7:
                defaults[dow] = float(r["ajuste_pct"])

    next_id = int(df["regra_id"].max()) + 1 if not df.empty else 1
    novas_rows = []
    for dow in range(7):
        novas_rows.append({
            "regra_id": next_id,
            "escopo": body.escopo,
            "escopo_id": body.escopo_id,
            "dia_semana": dow,
            "ajuste_pct": defaults[dow],
        })
        next_id += 1
    df = pd.concat([df, pd.DataFrame(novas_rows)], ignore_index=True)
    _write_dia_semana_df(df)
    log_operacao(
        _get_usuario(x_usuario), "create",
        "regras.dia_semana", f"{body.escopo}:{body.escopo_id or 'null'}",
        {"values": defaults},
    )
    return {
        "escopo": body.escopo,
        "escopo_id": body.escopo_id,
        "values": defaults,
    }


class CelulaDiaSemanaPatch(BaseModel):
    escopo: str = Field(..., pattern="^(global|regiao|predio)$")
    escopo_id: Optional[int] = None
    dia_semana: int = Field(..., ge=0, le=6)
    ajuste_pct: float = Field(..., ge=-1.0, le=3.0)


@app.patch("/regras/dia-semana/celula")
def patch_celula_dia_semana(
    body: CelulaDiaSemanaPatch,
    x_usuario: Optional[str] = Header(default=None, alias="X-Usuario"),
) -> dict:
    df = _read_dia_semana_df()
    if body.escopo == "global":
        mask = (df["escopo"] == "global") & (df["dia_semana"] == body.dia_semana)
    else:
        mask = (
            (df["escopo"] == body.escopo)
            & (df["escopo_id"] == body.escopo_id)
            & (df["dia_semana"] == body.dia_semana)
        )
    if not mask.any():
        next_id = int(df["regra_id"].max()) + 1 if not df.empty else 1
        df = pd.concat(
            [
                df,
                pd.DataFrame([{
                    "regra_id": next_id,
                    "escopo": body.escopo,
                    "escopo_id": body.escopo_id,
                    "dia_semana": body.dia_semana,
                    "ajuste_pct": body.ajuste_pct,
                }]),
            ],
            ignore_index=True,
        )
    else:
        df.loc[mask, "ajuste_pct"] = body.ajuste_pct
    _write_dia_semana_df(df)
    log_operacao(
        _get_usuario(x_usuario), "update",
        "regras.dia_semana",
        f"{body.escopo}:{body.escopo_id or 'null'}:dow={body.dia_semana}",
        {"ajuste_pct": body.ajuste_pct},
    )
    return {"ok": True}


@app.delete("/regras/dia-semana/escopo/{escopo}/{escopo_id}")
def deletar_escopo_dia_semana(
    escopo: str, escopo_id: str,
    x_usuario: Optional[str] = Header(default=None, alias="X-Usuario"),
) -> dict:
    df = _read_dia_semana_df()
    if escopo == "global":
        mask = df["escopo"] == "global"
    else:
        try:
            eid = int(escopo_id)
        except ValueError:
            raise HTTPException(status_code=400, detail="escopo_id inválido")
        mask = (df["escopo"] == escopo) & (df["escopo_id"] == eid)
    if not mask.any():
        raise HTTPException(status_code=404, detail="Escopo não encontrado")
    linhas_afetadas = int(mask.sum())
    df = df[~mask].copy()
    _write_dia_semana_df(df)
    log_operacao(
        _get_usuario(x_usuario), "delete",
        "regras.dia_semana", f"{escopo}:{escopo_id}",
        {"linhas_removidas": linhas_afetadas},
    )
    return {"ok": True, "linhas_removidas": linhas_afetadas}


# ─── Eventos (matriz evento × escopo) ──────────────────────────

EVENTOS_PARQUET = DATA_ROOT / "regras_priori" / "eventos" / "eventos.parquet"
IMPACTOS_PARQUET = DATA_ROOT / "regras_priori" / "evento_impactos" / "evento_impactos.parquet"

CATEGORIAS_EVENTO = {"esportivo", "show", "feriado", "convencao"}


def _read_eventos_df() -> pd.DataFrame:
    if not EVENTOS_PARQUET.exists():
        return pd.DataFrame(
            columns=["evento_id", "nome", "data_inicio", "data_fim", "categoria"]
        )
    df = pd.read_parquet(EVENTOS_PARQUET)
    if "ativo" in df.columns:
        df = df.drop(columns=["ativo"])
    return df


def _write_eventos_df(df: pd.DataFrame) -> None:
    EVENTOS_PARQUET.parent.mkdir(parents=True, exist_ok=True)
    if not df.empty:
        df = df.copy()
        df["evento_id"] = df["evento_id"].astype("int64")
        df["data_inicio"] = pd.to_datetime(df["data_inicio"]).dt.date
        df["data_fim"] = pd.to_datetime(df["data_fim"]).dt.date
    df.to_parquet(EVENTOS_PARQUET, index=False)


def _read_impactos_df() -> pd.DataFrame:
    if not IMPACTOS_PARQUET.exists():
        return pd.DataFrame(
            columns=["impacto_id", "evento_id", "escopo", "escopo_id", "ajuste_pct"]
        )
    df = pd.read_parquet(IMPACTOS_PARQUET)
    if "ativo" in df.columns:
        df = df.drop(columns=["ativo"])
    return df


def _write_impactos_df(df: pd.DataFrame) -> None:
    IMPACTOS_PARQUET.parent.mkdir(parents=True, exist_ok=True)
    if not df.empty:
        df = df.copy()
        df["impacto_id"] = df["impacto_id"].astype("int64")
        df["evento_id"] = df["evento_id"].astype("int64")
        df["escopo_id"] = df["escopo_id"].astype("Int64")
        df["ajuste_pct"] = df["ajuste_pct"].astype(float)
    df.to_parquet(IMPACTOS_PARQUET, index=False)


def _nome_escopo_lookup(escopo: str, escopo_id: Optional[int]) -> str:
    if escopo == "global":
        return "Global"
    table = {
        "regiao": ("cadastro.regioes", "regiao_id"),
        "predio": ("cadastro.predios", "predio_id"),
        "unidade": ("cadastro.unidades", "unidade_id"),
    }.get(escopo)
    if not table or escopo_id is None:
        return f"{escopo} #{escopo_id}"
    t, c = table
    if escopo == "unidade":
        sql = f"SELECT codigo_externo FROM {t} WHERE {c} = ?"
    else:
        sql = f"SELECT nome FROM {t} WHERE {c} = ?"
    r = CON.execute(sql, [escopo_id]).fetchone()
    return r[0] if r else f"{escopo} #{escopo_id}"


@app.get("/regras/eventos/matriz")
def matriz_eventos() -> dict:
    eventos = _read_eventos_df()
    impactos = _read_impactos_df()

    impactos_por_evento: dict[int, list[dict]] = {}
    for _, imp in impactos.iterrows():
        eid = int(imp["evento_id"])
        impactos_por_evento.setdefault(eid, []).append(
            {
                "escopo": imp["escopo"],
                "escopo_id": None if pd.isna(imp["escopo_id"]) else int(imp["escopo_id"]),
                "ajuste_pct": float(imp["ajuste_pct"]),
            }
        )

    eventos_out = []
    for _, ev in eventos.sort_values("data_inicio").iterrows():
        eventos_out.append(
            {
                "evento_id": int(ev["evento_id"]),
                "nome": str(ev["nome"]),
                "data_inicio": ev["data_inicio"].isoformat() if hasattr(ev["data_inicio"], "isoformat") else str(ev["data_inicio"]),
                "data_fim": ev["data_fim"].isoformat() if hasattr(ev["data_fim"], "isoformat") else str(ev["data_fim"]),
                "categoria": str(ev["categoria"]),
                "impactos": impactos_por_evento.get(int(ev["evento_id"]), []),
            }
        )

    # Escopos "usados" (únicos, com algum impacto ativo)
    usados_keys: set[tuple[str, Optional[int]]] = set()
    for imps in impactos_por_evento.values():
        for imp in imps:
            usados_keys.add((imp["escopo"], imp["escopo_id"]))
    ordem = {"global": 0, "regiao": 1, "predio": 2, "unidade": 3}
    escopos_usados = [
        {"escopo": esc, "escopo_id": eid, "nome": _nome_escopo_lookup(esc, eid)}
        for esc, eid in usados_keys
    ]
    escopos_usados.sort(key=lambda e: (ordem.get(e["escopo"], 9), e["nome"]))

    return {"eventos": eventos_out, "escopos_usados": escopos_usados}


class EventoIn(BaseModel):
    nome: str = Field(..., min_length=1, max_length=120)
    data_inicio: str
    data_fim: str
    categoria: str


class EventoPatch(BaseModel):
    nome: Optional[str] = None
    data_inicio: Optional[str] = None
    data_fim: Optional[str] = None
    categoria: Optional[str] = None


def _validar_categoria(cat: str) -> None:
    if cat not in CATEGORIAS_EVENTO:
        raise HTTPException(
            status_code=400,
            detail=f"categoria inválida. Use: {sorted(CATEGORIAS_EVENTO)}",
        )


@app.post("/regras/eventos", status_code=201)
def criar_evento(
    body: EventoIn,
    x_usuario: Optional[str] = Header(default=None, alias="X-Usuario"),
) -> dict:
    d_ini, d_fim = _validar_datas(body.data_inicio, body.data_fim)
    _validar_categoria(body.categoria)

    df = _read_eventos_df()
    next_id = int(df["evento_id"].max()) + 1 if not df.empty else 1
    novo = {
        "evento_id": next_id,
        "nome": body.nome,
        "data_inicio": d_ini,
        "data_fim": d_fim,
        "categoria": body.categoria,
    }
    df = pd.concat([df, pd.DataFrame([novo])], ignore_index=True)
    _write_eventos_df(df)
    log_operacao(
        _get_usuario(x_usuario), "create",
        "regras.eventos", str(next_id),
        {"nome": body.nome, "categoria": body.categoria},
    )
    return {
        "evento_id": next_id,
        "nome": body.nome,
        "data_inicio": d_ini.isoformat(),
        "data_fim": d_fim.isoformat(),
        "categoria": body.categoria,
        "impactos": [],
    }


@app.patch("/regras/eventos/{evento_id}")
def editar_evento(
    evento_id: int, body: EventoPatch,
    x_usuario: Optional[str] = Header(default=None, alias="X-Usuario"),
) -> dict:
    df = _read_eventos_df()
    mask = df["evento_id"] == evento_id
    if not mask.any():
        raise HTTPException(status_code=404, detail=f"evento {evento_id} não encontrado")

    patch = body.model_dump(exclude_none=True)

    if "data_inicio" in patch or "data_fim" in patch:
        row = df[mask].iloc[0]
        d_ini_s = patch.get("data_inicio") or str(row["data_inicio"])
        d_fim_s = patch.get("data_fim") or str(row["data_fim"])
        d_ini, d_fim = _validar_datas(d_ini_s, d_fim_s)
        patch["data_inicio"] = d_ini
        patch["data_fim"] = d_fim

    if "categoria" in patch:
        _validar_categoria(patch["categoria"])

    for col, val in patch.items():
        df.loc[mask, col] = val

    _write_eventos_df(df)
    log_operacao(
        _get_usuario(x_usuario), "update",
        "regras.eventos", str(evento_id),
        {k: (v.isoformat() if hasattr(v, "isoformat") else v) for k, v in patch.items()},
    )
    return {"ok": True}


@app.delete("/regras/eventos/{evento_id}")
def deletar_evento(
    evento_id: int,
    x_usuario: Optional[str] = Header(default=None, alias="X-Usuario"),
) -> dict:
    df_ev = _read_eventos_df()
    mask = df_ev["evento_id"] == evento_id
    if not mask.any():
        raise HTTPException(status_code=404, detail=f"evento {evento_id} não encontrado")
    nome = str(df_ev[mask].iloc[0]["nome"])
    df_ev = df_ev[~mask].copy()
    _write_eventos_df(df_ev)
    # Remove também todos os impactos desse evento
    df_imp = _read_impactos_df()
    imp_mask = df_imp["evento_id"] == evento_id
    impactos_removidos = int(imp_mask.sum())
    if imp_mask.any():
        df_imp = df_imp[~imp_mask].copy()
        _write_impactos_df(df_imp)
    log_operacao(
        _get_usuario(x_usuario), "delete",
        "regras.eventos", str(evento_id),
        {"nome": nome, "impactos_removidos": impactos_removidos},
    )
    return {"ok": True, "impactos_removidos": impactos_removidos}


class ImpactoPatch(BaseModel):
    escopo: str = Field(..., pattern="^(global|regiao|predio|unidade)$")
    escopo_id: Optional[int] = None
    ajuste_pct: float = Field(..., ge=-1.0, le=3.0)


@app.patch("/regras/eventos/{evento_id}/impacto")
def upsert_impacto(
    evento_id: int, body: ImpactoPatch,
    x_usuario: Optional[str] = Header(default=None, alias="X-Usuario"),
) -> dict:
    ev_df = _read_eventos_df()
    if not (ev_df["evento_id"] == evento_id).any():
        raise HTTPException(status_code=404, detail=f"evento {evento_id} não encontrado")

    _validar_escopo(body.escopo, body.escopo_id)

    imp_df = _read_impactos_df()
    mask = (
        (imp_df["evento_id"] == evento_id)
        & (imp_df["escopo"] == body.escopo)
        & (
            (imp_df["escopo_id"].isna() & (body.escopo_id is None))
            | (imp_df["escopo_id"] == body.escopo_id)
        )
    )

    recurso_id = f"{evento_id}:{body.escopo}:{body.escopo_id or 'null'}"

    # Ajuste zero → remove o impacto
    if abs(body.ajuste_pct) < 1e-6:
        if mask.any():
            imp_df = imp_df[~mask].copy()
            _write_impactos_df(imp_df)
        log_operacao(
            _get_usuario(x_usuario), "delete",
            "regras.eventos.impacto", recurso_id, None,
        )
        return {"ok": True, "acao": "removido"}

    if mask.any():
        imp_df.loc[mask, "ajuste_pct"] = body.ajuste_pct
        acao = "update"
    else:
        next_id = int(imp_df["impacto_id"].max()) + 1 if not imp_df.empty else 1
        novo = {
            "impacto_id": next_id,
            "evento_id": evento_id,
            "escopo": body.escopo,
            "escopo_id": body.escopo_id,
            "ajuste_pct": body.ajuste_pct,
        }
        imp_df = pd.concat([imp_df, pd.DataFrame([novo])], ignore_index=True)
        acao = "create"

    _write_impactos_df(imp_df)
    log_operacao(
        _get_usuario(x_usuario), acao,
        "regras.eventos.impacto", recurso_id,
        {"ajuste_pct": body.ajuste_pct},
    )
    return {"ok": True, "acao": acao}


# ─── Antecedência (faixas de lead_time) ───────────────────────

ANTECEDENCIA_PARQUET = (
    DATA_ROOT / "regras_priori" / "regras_antecedencia" / "regras_antecedencia.parquet"
)


def _read_antecedencia_df() -> pd.DataFrame:
    if not ANTECEDENCIA_PARQUET.exists():
        return pd.DataFrame(
            columns=[
                "regra_id", "escopo", "escopo_id",
                "lead_min_dias", "lead_max_dias", "dia_semana",
                "ajuste_pct",
            ]
        )
    df = pd.read_parquet(ANTECEDENCIA_PARQUET)
    if "ativo" in df.columns:
        df = df.drop(columns=["ativo"])
    return df


def _write_antecedencia_df(df: pd.DataFrame) -> None:
    ANTECEDENCIA_PARQUET.parent.mkdir(parents=True, exist_ok=True)
    if not df.empty:
        df = df.copy()
        df["regra_id"] = df["regra_id"].astype("int64")
        df["lead_min_dias"] = df["lead_min_dias"].astype("int64")
        df["lead_max_dias"] = df["lead_max_dias"].astype("int64")
        df["dia_semana"] = df["dia_semana"].astype("Int64")
        df["ajuste_pct"] = df["ajuste_pct"].astype(float)
    df.to_parquet(ANTECEDENCIA_PARQUET, index=False)


def _serializar_faixas(df: pd.DataFrame) -> list[dict]:
    """Agrupa linhas em faixas (lead_min, lead_max). Determina se é uniforme ou por DOW."""
    out = []
    for (mn, mx), g in df.groupby(["lead_min_dias", "lead_max_dias"]):
        dow_rows = g[g["dia_semana"].notna()]
        uniform_rows = g[g["dia_semana"].isna()]
        if len(dow_rows) > 0:
            por_dow = True
            dow_values = [0.0] * 7
            for _, r in dow_rows.iterrows():
                idx = int(r["dia_semana"])
                if 0 <= idx < 7:
                    dow_values[idx] = float(r["ajuste_pct"])
            ajuste_uniforme = None
            ajustes_dow = dow_values
        else:
            por_dow = False
            ajuste_uniforme = float(uniform_rows["ajuste_pct"].iloc[0]) if len(uniform_rows) else 0.0
            ajustes_dow = None
        out.append({
            "lead_min_dias": int(mn),
            "lead_max_dias": int(mx),
            "por_dow": por_dow,
            "ajuste_uniforme": ajuste_uniforme,
            "ajustes_dow": ajustes_dow,
        })
    out.sort(key=lambda f: f["lead_min_dias"])
    return out


def _calcular_gaps(faixas_ativas: list[dict], limite: int = 365) -> list[dict]:
    intervals = sorted(
        (f["lead_min_dias"], f["lead_max_dias"]) for f in faixas_ativas
    )
    gaps = []
    prev_end = 0
    for mn, mx in intervals:
        if mn > prev_end:
            gaps.append({"lead_min_dias": prev_end, "lead_max_dias": mn})
        prev_end = max(prev_end, mx)
    if prev_end < limite:
        gaps.append({"lead_min_dias": prev_end, "lead_max_dias": limite})
    return gaps


@app.get("/regras/antecedencia")
def listar_antecedencia() -> dict:
    df = _read_antecedencia_df()
    if df.empty:
        return {"faixas": [], "gaps": [{"lead_min_dias": 0, "lead_max_dias": 365}]}
    faixas = _serializar_faixas(df)
    gaps = _calcular_gaps(faixas)
    return {"faixas": faixas, "gaps": gaps}


class FaixaAntecedenciaIn(BaseModel):
    lead_min_dias: int = Field(..., ge=0, le=365)
    lead_max_dias: int = Field(..., gt=0, le=365)
    por_dow: bool
    ajuste_uniforme: Optional[float] = None
    ajustes_dow: Optional[list[float]] = None


def _checar_sobreposicao(
    df: pd.DataFrame,
    mn: int,
    mx: int,
    ignorar_faixa: Optional[tuple[int, int]] = None,
) -> None:
    if mn >= mx:
        raise HTTPException(status_code=400, detail="lead_min_dias deve ser < lead_max_dias")
    faixas_existentes: set[tuple[int, int]] = set(
        (int(a), int(b)) for a, b in zip(df["lead_min_dias"], df["lead_max_dias"])
    )
    if ignorar_faixa is not None:
        faixas_existentes.discard(ignorar_faixa)
    for a, b in faixas_existentes:
        if mx > a and mn < b:
            raise HTTPException(
                status_code=409,
                detail=f"Nova faixa sobrepõe com {a}-{b} dias",
            )


def _construir_linhas_faixa(
    df: pd.DataFrame, body: FaixaAntecedenciaIn
) -> list[dict]:
    next_id = int(df["regra_id"].max()) + 1 if not df.empty else 1
    linhas = []
    if body.por_dow:
        vals = body.ajustes_dow or [0.0] * 7
        if len(vals) != 7:
            raise HTTPException(status_code=400, detail="ajustes_dow precisa ter 7 valores")
        for i in range(7):
            linhas.append({
                "regra_id": next_id,
                "escopo": "global",
                "escopo_id": None,
                "lead_min_dias": body.lead_min_dias,
                "lead_max_dias": body.lead_max_dias,
                "dia_semana": i,
                "ajuste_pct": float(vals[i]),
            })
            next_id += 1
    else:
        linhas.append({
            "regra_id": next_id,
            "escopo": "global",
            "escopo_id": None,
            "lead_min_dias": body.lead_min_dias,
            "lead_max_dias": body.lead_max_dias,
            "dia_semana": None,
            "ajuste_pct": float(body.ajuste_uniforme or 0),
        })
    return linhas


@app.post("/regras/antecedencia/faixa", status_code=201)
def criar_faixa_antecedencia(
    body: FaixaAntecedenciaIn,
    x_usuario: Optional[str] = Header(default=None, alias="X-Usuario"),
) -> dict:
    df = _read_antecedencia_df()
    _checar_sobreposicao(df, body.lead_min_dias, body.lead_max_dias)
    linhas = _construir_linhas_faixa(df, body)
    df = pd.concat([df, pd.DataFrame(linhas)], ignore_index=True)
    _write_antecedencia_df(df)
    log_operacao(
        _get_usuario(x_usuario), "create",
        "regras.antecedencia",
        f"{body.lead_min_dias}-{body.lead_max_dias}",
        {"por_dow": body.por_dow, "ajuste_uniforme": body.ajuste_uniforme},
    )
    return {"ok": True}


@app.put("/regras/antecedencia/faixa/{lead_min}/{lead_max}")
def atualizar_faixa_antecedencia(
    lead_min: int, lead_max: int, body: FaixaAntecedenciaIn,
    x_usuario: Optional[str] = Header(default=None, alias="X-Usuario"),
) -> dict:
    df = _read_antecedencia_df()
    mask = (df["lead_min_dias"] == lead_min) & (df["lead_max_dias"] == lead_max)
    if not mask.any():
        raise HTTPException(status_code=404, detail="Faixa não encontrada")
    _checar_sobreposicao(
        df, body.lead_min_dias, body.lead_max_dias,
        ignorar_faixa=(lead_min, lead_max),
    )
    df = df[~mask].copy()
    linhas = _construir_linhas_faixa(df, body)
    df = pd.concat([df, pd.DataFrame(linhas)], ignore_index=True)
    _write_antecedencia_df(df)
    log_operacao(
        _get_usuario(x_usuario), "update",
        "regras.antecedencia",
        f"{lead_min}-{lead_max}",
        {"nova_faixa": f"{body.lead_min_dias}-{body.lead_max_dias}", "por_dow": body.por_dow},
    )
    return {"ok": True}


@app.delete("/regras/antecedencia/faixa/{lead_min}/{lead_max}")
def deletar_faixa_antecedencia(
    lead_min: int, lead_max: int,
    x_usuario: Optional[str] = Header(default=None, alias="X-Usuario"),
) -> dict:
    df = _read_antecedencia_df()
    mask = (df["lead_min_dias"] == lead_min) & (df["lead_max_dias"] == lead_max)
    if not mask.any():
        raise HTTPException(status_code=404, detail="Faixa não encontrada")
    removidas = int(mask.sum())
    df = df[~mask].copy()
    _write_antecedencia_df(df)
    log_operacao(
        _get_usuario(x_usuario), "delete",
        "regras.antecedencia",
        f"{lead_min}-{lead_max}",
        {"linhas_removidas": removidas},
    )
    return {"ok": True, "linhas_removidas": removidas}


# ─── Ocupação (portfólio) ─────────────────────────────────────

OCUP_PORTFOLIO_PARQUET = (
    DATA_ROOT / "regras_posteriori" / "regras_ocupacao_portfolio" / "regras_ocupacao_portfolio.parquet"
)


def _read_ocup_portfolio_df() -> pd.DataFrame:
    if not OCUP_PORTFOLIO_PARQUET.exists():
        return pd.DataFrame(
            columns=[
                "regra_id", "escopo", "escopo_id",
                "janela_dias", "ocupacao_min_pct", "ocupacao_max_pct",
                "ajuste_pct", "cumulativo",
            ]
        )
    df = pd.read_parquet(OCUP_PORTFOLIO_PARQUET)
    if "ativo" in df.columns:
        df = df.drop(columns=["ativo"])
    return df


def _write_ocup_portfolio_df(df: pd.DataFrame) -> None:
    OCUP_PORTFOLIO_PARQUET.parent.mkdir(parents=True, exist_ok=True)
    if not df.empty:
        df = df.copy()
        df["regra_id"] = df["regra_id"].astype("int64")
        df["escopo_id"] = df["escopo_id"].astype("Int64")
        df["janela_dias"] = df["janela_dias"].astype("int64")
        df["ocupacao_min_pct"] = df["ocupacao_min_pct"].astype(float)
        df["ocupacao_max_pct"] = df["ocupacao_max_pct"].astype(float)
        df["ajuste_pct"] = df["ajuste_pct"].astype(float)
    df.to_parquet(OCUP_PORTFOLIO_PARQUET, index=False)


def _serializar_buckets_ocup(df: pd.DataFrame) -> list[dict]:
    buckets: list[dict] = []
    for janela, g in df.groupby("janela_dias"):
        g = g.sort_values("ocupacao_min_pct")
        bandas = [
            {
                "ocupacao_min_pct": float(r["ocupacao_min_pct"]),
                "ocupacao_max_pct": float(r["ocupacao_max_pct"]),
                "ajuste_pct": float(r["ajuste_pct"]),
            }
            for _, r in g.iterrows()
        ]
        buckets.append(
            {
                "janela_dias": int(janela),
                "bandas": bandas,
            }
        )
    buckets.sort(key=lambda b: b["janela_dias"])
    return buckets


@app.get("/regras/ocupacao-portfolio")
def listar_ocupacao_portfolio() -> dict:
    df = _read_ocup_portfolio_df()
    if df.empty:
        return {"buckets": []}
    return {"buckets": _serializar_buckets_ocup(df)}


class BucketOcupIn(BaseModel):
    janela_dias: int = Field(..., ge=0, le=365)
    limites: list[float] = Field(default_factory=list)  # ordenados, todos em (0, 1)
    ajustes: list[float]  # len = limites.len + 1


def _validar_bucket_ocup(body: BucketOcupIn) -> None:
    if body.janela_dias < 0:
        raise HTTPException(status_code=400, detail="janela_dias inválida")
    if len(body.ajustes) != len(body.limites) + 1:
        raise HTTPException(
            status_code=400,
            detail=f"ajustes deve ter {len(body.limites) + 1} itens (limites + 1)",
        )
    for i, lim in enumerate(body.limites):
        if not (0.0 < lim < 1.0):
            raise HTTPException(status_code=400, detail=f"limite {lim} fora de (0, 1)")
        if i > 0 and lim <= body.limites[i - 1]:
            raise HTTPException(status_code=400, detail="limites precisam ser estritamente crescentes")
    for a in body.ajustes:
        if not (-1.0 <= a <= 3.0):
            raise HTTPException(status_code=400, detail="ajuste fora de [-100%, +300%]")


def _bucket_para_linhas(body: BucketOcupIn, start_id: int) -> list[dict]:
    """Converte (janela, limites, ajustes) em linhas de banda."""
    pontos = [0.0] + list(body.limites) + [1.0]
    linhas = []
    next_id = start_id
    for i, adj in enumerate(body.ajustes):
        linhas.append(
            {
                "regra_id": next_id,
                "escopo": "global",
                "escopo_id": None,
                "janela_dias": body.janela_dias,
                "ocupacao_min_pct": pontos[i],
                "ocupacao_max_pct": pontos[i + 1],
                "ajuste_pct": float(adj),
                "cumulativo": True,
            }
        )
        next_id += 1
    return linhas


@app.post("/regras/ocupacao-portfolio/bucket", status_code=201)
def criar_bucket_ocup(
    body: BucketOcupIn,
    x_usuario: Optional[str] = Header(default=None, alias="X-Usuario"),
) -> dict:
    _validar_bucket_ocup(body)
    df = _read_ocup_portfolio_df()
    if (df["janela_dias"] == body.janela_dias).any():
        raise HTTPException(status_code=409, detail=f"Bucket {body.janela_dias} já existe")
    next_id = int(df["regra_id"].max()) + 1 if not df.empty else 1
    linhas = _bucket_para_linhas(body, next_id)
    df = pd.concat([df, pd.DataFrame(linhas)], ignore_index=True)
    _write_ocup_portfolio_df(df)
    log_operacao(
        _get_usuario(x_usuario), "create",
        "regras.ocupacao_portfolio", str(body.janela_dias),
        {"limites": body.limites, "ajustes": body.ajustes},
    )
    return {"ok": True}


@app.put("/regras/ocupacao-portfolio/bucket/{janela_dias}")
def atualizar_bucket_ocup(
    janela_dias: int, body: BucketOcupIn,
    x_usuario: Optional[str] = Header(default=None, alias="X-Usuario"),
) -> dict:
    _validar_bucket_ocup(body)
    df = _read_ocup_portfolio_df()

    if body.janela_dias != janela_dias:
        if not (df["janela_dias"] == janela_dias).any():
            raise HTTPException(status_code=404, detail="Bucket não encontrado")
        if (df["janela_dias"] == body.janela_dias).any():
            raise HTTPException(status_code=409, detail=f"Bucket {body.janela_dias} já existe")
        df = df[df["janela_dias"] != janela_dias].copy()
    else:
        mask = df["janela_dias"] == janela_dias
        if not mask.any():
            raise HTTPException(status_code=404, detail="Bucket não encontrado")
        df = df[~mask].copy()

    next_id = int(df["regra_id"].max()) + 1 if not df.empty else 1
    linhas = _bucket_para_linhas(body, next_id)
    df = pd.concat([df, pd.DataFrame(linhas)], ignore_index=True)
    _write_ocup_portfolio_df(df)
    log_operacao(
        _get_usuario(x_usuario), "update",
        "regras.ocupacao_portfolio", str(janela_dias),
        {"nova_janela": body.janela_dias, "limites": body.limites, "ajustes": body.ajustes},
    )
    return {"ok": True}


@app.delete("/regras/ocupacao-portfolio/bucket/{janela_dias}")
def deletar_bucket_ocup(
    janela_dias: int,
    x_usuario: Optional[str] = Header(default=None, alias="X-Usuario"),
) -> dict:
    df = _read_ocup_portfolio_df()
    mask = df["janela_dias"] == janela_dias
    if not mask.any():
        raise HTTPException(status_code=404, detail="Bucket não encontrado")
    removidas = int(mask.sum())
    df = df[~mask].copy()
    _write_ocup_portfolio_df(df)
    log_operacao(
        _get_usuario(x_usuario), "delete",
        "regras.ocupacao_portfolio", str(janela_dias),
        {"linhas_removidas": removidas},
    )
    return {"ok": True, "linhas_removidas": removidas}


# TEMPORÁRIO — gera dados sintéticos de ocupação real pra testar a UI.
# Remover quando o simulador estiver estável.
FAKE_OCUP_SCRIPT = ROOT / "simulador" / "backend" / "fake_ocupacao_teste.py"


@app.post("/regras/fake-ocupacao")
def gerar_fake_ocupacao(
    x_usuario: Optional[str] = Header(default=None, alias="X-Usuario"),
) -> dict:
    if not FAKE_OCUP_SCRIPT.exists():
        raise HTTPException(
            status_code=500, detail=f"script não encontrado: {FAKE_OCUP_SCRIPT}"
        )
    global CON
    CON.close()
    t0 = time.perf_counter()
    try:
        result = subprocess.run(
            [sys.executable, str(FAKE_OCUP_SCRIPT)],
            capture_output=True,
            text=True,
            timeout=60,
        )
    except subprocess.TimeoutExpired:
        CON = build_duckdb()
        raise HTTPException(status_code=504, detail="script demorou mais de 60s")
    duration_ms = int((time.perf_counter() - t0) * 1000)
    CON = build_duckdb()
    if result.returncode != 0:
        log_operacao(
            _get_usuario(x_usuario), "gerar_fake_ocupacao_erro",
            "sistema", None,
            {"duration_ms": duration_ms, "erro": result.stderr[-500:]},
        )
        raise HTTPException(
            status_code=500,
            detail=f"falha: {result.stderr[-500:]}",
        )
    log_operacao(
        _get_usuario(x_usuario), "gerar_fake_ocupacao",
        "sistema", None,
        {"duration_ms": duration_ms},
    )
    return {
        "ok": True,
        "duration_ms": duration_ms,
        "stdout_tail": result.stdout[-500:],
    }


@app.post("/regras/rebuild-simulador")
def rebuild_simulador(
    x_usuario: Optional[str] = Header(default=None, alias="X-Usuario"),
) -> dict:
    """Regenera o simulador.duckdb a partir dos parquets atualizados."""
    if not SIMULADOR_BUILD_SCRIPT.exists():
        raise HTTPException(
            status_code=500,
            detail=f"script não encontrado: {SIMULADOR_BUILD_SCRIPT}",
        )
    global CON
    CON.close()
    t0 = time.perf_counter()
    try:
        result = subprocess.run(
            [sys.executable, str(SIMULADOR_BUILD_SCRIPT)],
            capture_output=True,
            text=True,
            timeout=120,
        )
    except subprocess.TimeoutExpired:
        CON = build_duckdb()
        raise HTTPException(status_code=504, detail="rebuild demorou mais de 120s")
    duration_ms = int((time.perf_counter() - t0) * 1000)
    CON = build_duckdb()
    if result.returncode != 0:
        log_operacao(
            _get_usuario(x_usuario), "rebuild_simulador_erro",
            "sistema", None,
            {"duration_ms": duration_ms, "erro": result.stderr[-500:]},
        )
        raise HTTPException(
            status_code=500,
            detail=f"falha no rebuild: {result.stderr[-500:]}",
        )
    log_operacao(
        _get_usuario(x_usuario), "rebuild_simulador",
        "sistema", None,
        {"duration_ms": duration_ms},
    )
    return {
        "ok": True,
        "duration_ms": duration_ms,
        "stdout_tail": result.stdout[-300:],
    }


@app.post("/reload")
def reload_views() -> dict:
    """Recarrega as views (útil após regenerar parquets)."""
    global CON
    CON.close()
    CON = build_duckdb()
    return {"ok": True, "reloaded_at": time.time()}
