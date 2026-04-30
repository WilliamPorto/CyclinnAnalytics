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
import random
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
    "fat_ajuste_regiao":     ("ajuste_pct",             "percent",  "unidade"),
    "fat_ajuste_individual": ("ajuste_pct",             "percent",  "unidade"),
    "pi":                    ("valor",                  "currency", "unidade"),
    "d":                     ("valor",                  "currency", "unidade"),
    "ocupacao_regiao":       ("ocupacao_pct",           "percent",  "regiao"),
    "expectativa_regiao":    ("ocupacao_esperada_pct",  "percent",  "regiao"),
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
    view: str = "unidade",  # "unidade" | "regiao" | "predio"
) -> dict:
    if table not in SIMULADOR_TABLES:
        raise HTTPException(status_code=404, detail=f"Tabela '{table}' não suportada")
    if view not in ("unidade", "regiao", "predio"):
        raise HTTPException(status_code=400, detail=f"view inválido: {view}")

    value_col, fmt, native_row_type = SIMULADOR_TABLES[table]
    # Tabelas nativamente por região só aparecem em views agregadas (regiao/predio).
    # Em view=unidade, o frontend já bloqueia a aba — aqui forçamos regiao como fallback.
    if native_row_type == "regiao" and view == "unidade":
        effective_row_type = "regiao"
    else:
        effective_row_type = view

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

    # Labels (unidades, regioes, ou predios)
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
    elif effective_row_type == "predio":
        total = CON.execute("SELECT COUNT(*) FROM cadastro.predios").fetchone()[0]
        label_rows = CON.execute(
            f"""
            SELECT predio_id, nome
            FROM cadastro.predios
            ORDER BY nome
            LIMIT {page_size} OFFSET {offset}
            """
        ).fetchall()
    else:  # regiao
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

    needs_aggregation = native_row_type == "unidade" and effective_row_type != "unidade"

    # Coluna agregadora quando view != unidade pra tabelas unit-level.
    agg_col = "p.regiao_id" if effective_row_type == "regiao" else "u.predio_id"

    # Define se a tabela injeta uma coluna color_pct (usada pra heatmap por
    # diferença em vez de pelo valor absoluto).
    #   pi                → color_pct = (pi − pb) / pb    (fatores a priori)
    #   d                 → color_pct = (d − pb) / pb     (impacto total vs preço base)
    #   ocupacao_regiao   → color_pct = real − esperada
    has_color_pct = table in ("pi", "d", "ocupacao_regiao")

    def build_values_sql(ids_filter: str) -> str:
        # Ocupação real: v = real, color_pct = (real − esperada) → heatmap pelo gap.
        # Em view=predio, faz swap pras tabelas ocupacao_predio + expectativa_predio.
        if table == "ocupacao_regiao":
            if effective_row_type == "predio":
                return f"""
                    SELECT o.predio_id AS id, o.data,
                           o.ocupacao_pct AS v,
                           (o.ocupacao_pct - e.ocupacao_esperada_pct) AS color_pct
                    FROM {SIMULADOR_ALIAS}.main.ocupacao_predio o
                    JOIN {SIMULADOR_ALIAS}.main.expectativa_predio e
                      USING(data_referencia, predio_id, data)
                    WHERE o.data_referencia = DATE '{d_ref_s}'
                      AND o.data BETWEEN DATE '{d_ini_s}' AND DATE '{d_fim_s}'
                      {ids_filter.replace('predio_id', 'o.predio_id')}
                """
            return f"""
                SELECT o.regiao_id AS id, o.data,
                       o.ocupacao_pct AS v,
                       (o.ocupacao_pct - e.ocupacao_esperada_pct) AS color_pct
                FROM {SIMULADOR_ALIAS}.main.ocupacao_regiao o
                JOIN {SIMULADOR_ALIAS}.main.expectativa_regiao e
                  USING(data_referencia, regiao_id, data)
                WHERE o.data_referencia = DATE '{d_ref_s}'
                  AND o.data BETWEEN DATE '{d_ini_s}' AND DATE '{d_fim_s}'
                  {ids_filter.replace('regiao_id', 'o.regiao_id')}
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
                SELECT {agg_col} AS id, pi.data,
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
                GROUP BY {agg_col}, pi.data
            """
        # d: retorna também o % de diferença vs Pb (= impacto total vs preço base)
        if table == "d":
            if not needs_aggregation:
                return f"""
                    SELECT d.unidade_id AS id, d.data, d.valor AS v,
                           (d.valor - pb.valor) / pb.valor AS color_pct
                    FROM {SIMULADOR_ALIAS}.main.d d
                    JOIN {SIMULADOR_ALIAS}.main.pb pb
                      USING(data_referencia, unidade_id, data)
                    WHERE d.data_referencia = DATE '{d_ref_s}'
                      AND d.data BETWEEN DATE '{d_ini_s}' AND DATE '{d_fim_s}'
                      {ids_filter.replace('unidade_id', 'd.unidade_id')}
                """
            return f"""
                SELECT {agg_col} AS id, d.data,
                       AVG(d.valor) AS v,
                       AVG((d.valor - pb.valor) / pb.valor) AS color_pct
                FROM {SIMULADOR_ALIAS}.main.d d
                JOIN {SIMULADOR_ALIAS}.main.pb pb
                  USING(data_referencia, unidade_id, data)
                JOIN cadastro.unidades u ON u.unidade_id = d.unidade_id
                JOIN cadastro.predios p USING(predio_id)
                WHERE d.data_referencia = DATE '{d_ref_s}'
                  AND d.data BETWEEN DATE '{d_ini_s}' AND DATE '{d_fim_s}'
                  {ids_filter}
                GROUP BY {agg_col}, d.data
            """
        # expectativa_regiao em view=predio: lê de expectativa_predio
        if table == "expectativa_regiao" and effective_row_type == "predio":
            return f"""
                SELECT predio_id AS id, data, ocupacao_esperada_pct AS v,
                       NULL::DOUBLE AS color_pct
                FROM {SIMULADOR_ALIAS}.main.expectativa_predio
                WHERE data_referencia = DATE '{d_ref_s}'
                  AND data BETWEEN DATE '{d_ini_s}' AND DATE '{d_fim_s}'
                  {ids_filter}
            """
        if not needs_aggregation:
            key_col = "unidade_id" if native_row_type == "unidade" else "regiao_id"
            return f"""
                SELECT {key_col} AS id, data, {value_col} AS v, NULL::DOUBLE AS color_pct
                FROM {SIMULADOR_ALIAS}.main.{table}
                WHERE data_referencia = DATE '{d_ref_s}'
                  AND data BETWEEN DATE '{d_ini_s}' AND DATE '{d_fim_s}'
                  {ids_filter}
            """
        # Agrega por região ou prédio (média simples)
        return f"""
            SELECT {agg_col} AS id, t.data, AVG(t.{value_col}) AS v, NULL::DOUBLE AS color_pct
            FROM {SIMULADOR_ALIAS}.main.{table} t
            JOIN cadastro.unidades u ON u.unidade_id = t.unidade_id
            JOIN cadastro.predios p USING(predio_id)
            WHERE t.data_referencia = DATE '{d_ref_s}'
              AND t.data BETWEEN DATE '{d_ini_s}' AND DATE '{d_fim_s}'
              {ids_filter}
            GROUP BY {agg_col}, t.data
        """

    matrix_rows: list[dict] = []
    if ids:
        ids_list = ",".join(str(i) for i in ids)
        if needs_aggregation:
            ids_filter = f"AND {agg_col} IN ({ids_list})"
        elif native_row_type == "regiao":
            # tabelas região-nativas: chave é regiao_id ou predio_id (em view=predio)
            key_col = "predio_id" if effective_row_type == "predio" else "regiao_id"
            ids_filter = f"AND {key_col} IN ({ids_list})"
        else:
            ids_filter = f"AND unidade_id IN ({ids_list})"
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

    # Linha de totais (apenas para a matriz `d`): soma do impacto em R$ por dia
    # considerando TODAS as unidades, independente de paginação/view.
    day_totals: Optional[list[Optional[float]]] = None
    if table == "d":
        totals_rows = CON.execute(
            f"""
            SELECT d.data, SUM(d.valor - pb.valor) AS impacto
            FROM {SIMULADOR_ALIAS}.main.d d
            JOIN {SIMULADOR_ALIAS}.main.pb pb
              USING(data_referencia, unidade_id, data)
            WHERE d.data_referencia = DATE '{d_ref_s}'
              AND d.data BETWEEN DATE '{d_ini_s}' AND DATE '{d_fim_s}'
            GROUP BY d.data
            """
        ).fetchall()
        totals_map = {r[0].isoformat(): float(r[1]) if r[1] is not None else None for r in totals_rows}
        day_totals = [totals_map.get(d) for d in date_cols]

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
        "day_totals": day_totals,
    }


# ============================================================
# Explicar preço — decomposição "Por que esse preço?"
# ============================================================
# Pra uma (unidade, data) específica, retorna:
#   pb → fatores a priori (saz, dow, eventos, antecedencia) → pi
#   pi → fatores a posteriori (ocupacao_regiao, ocupacao_individual) → d
# Cada fator inclui as REGRAS FONTE que casaram (id + label) pra navegação ao
# CRUD de regras.


def _data_referencia_default() -> str:
    row = CON.execute(
        f"SELECT MAX(data_referencia) FROM {SIMULADOR_ALIAS}.main.simulador_meta"
    ).fetchone()
    if row and row[0]:
        return row[0].isoformat()
    raise HTTPException(status_code=500, detail="simulador sem data_referencia")


@app.get("/simulador/explicar/{unidade_id}/{data}")
def simulador_explicar(
    unidade_id: int,
    data: str,
    data_referencia: Optional[str] = None,
) -> dict:
    """Decomposição completa do preço de uma (unidade, data)."""
    try:
        d_alvo = date.fromisoformat(data)
    except ValueError:
        raise HTTPException(status_code=400, detail=f"data inválida: {data}")
    d_ref = (
        date.fromisoformat(data_referencia)
        if data_referencia
        else date.fromisoformat(_data_referencia_default())
    )
    d_alvo_s = d_alvo.isoformat()
    d_ref_s = d_ref.isoformat()

    # Metadados da unidade (precisa pra matching de escopo)
    unit = CON.execute(
        f"""
        SELECT u.unidade_id, u.codigo_externo, u.predio_id, p.regiao_id, p.nome AS predio_nome,
               r.nome AS regiao_nome, u.segmento_id, s.nome AS segmento_nome
        FROM cadastro.unidades u
        JOIN cadastro.predios p USING(predio_id)
        JOIN cadastro.regioes r ON r.regiao_id = p.regiao_id
        LEFT JOIN cadastro.segmentos s ON s.segmento_id = u.segmento_id
        WHERE u.unidade_id = {int(unidade_id)}
        """
    ).fetchone()
    if not unit:
        raise HTTPException(status_code=404, detail=f"unidade {unidade_id} não encontrada")
    unit = {
        "unidade_id": unit[0], "codigo_externo": unit[1],
        "predio_id": unit[2], "regiao_id": unit[3], "predio_nome": unit[4],
        "regiao_nome": unit[5], "segmento_id": unit[6], "segmento_nome": unit[7],
    }

    # Valores principais da pipeline (pb, fatores, pi, d)
    valores = CON.execute(
        f"""
        SELECT
          (SELECT valor FROM {SIMULADOR_ALIAS}.main.pb
            WHERE data_referencia=DATE '{d_ref_s}' AND unidade_id={unidade_id} AND data=DATE '{d_alvo_s}') AS pb,
          (SELECT ajuste_pct FROM {SIMULADOR_ALIAS}.main.fat_sazonalidade
            WHERE data_referencia=DATE '{d_ref_s}' AND unidade_id={unidade_id} AND data=DATE '{d_alvo_s}') AS saz,
          (SELECT ajuste_pct FROM {SIMULADOR_ALIAS}.main.fat_dia_semana
            WHERE data_referencia=DATE '{d_ref_s}' AND unidade_id={unidade_id} AND data=DATE '{d_alvo_s}') AS dow,
          (SELECT ajuste_pct FROM {SIMULADOR_ALIAS}.main.fat_eventos
            WHERE data_referencia=DATE '{d_ref_s}' AND unidade_id={unidade_id} AND data=DATE '{d_alvo_s}') AS eventos,
          (SELECT ajuste_pct FROM {SIMULADOR_ALIAS}.main.fat_antecedencia
            WHERE data_referencia=DATE '{d_ref_s}' AND unidade_id={unidade_id} AND data=DATE '{d_alvo_s}') AS ant,
          (SELECT valor FROM {SIMULADOR_ALIAS}.main.pi
            WHERE data_referencia=DATE '{d_ref_s}' AND unidade_id={unidade_id} AND data=DATE '{d_alvo_s}') AS pi,
          (SELECT ajuste_pct FROM {SIMULADOR_ALIAS}.main.fat_ajuste_regiao
            WHERE data_referencia=DATE '{d_ref_s}' AND unidade_id={unidade_id} AND data=DATE '{d_alvo_s}') AS aj_reg,
          (SELECT ajuste_pct FROM {SIMULADOR_ALIAS}.main.fat_ajuste_individual
            WHERE data_referencia=DATE '{d_ref_s}' AND unidade_id={unidade_id} AND data=DATE '{d_alvo_s}') AS aj_ind,
          (SELECT valor FROM {SIMULADOR_ALIAS}.main.d
            WHERE data_referencia=DATE '{d_ref_s}' AND unidade_id={unidade_id} AND data=DATE '{d_alvo_s}') AS d
        """
    ).fetchone()
    if not valores or valores[0] is None:
        raise HTTPException(
            status_code=404,
            detail=f"sem dado pra (unidade={unidade_id}, data={d_alvo_s}) na referência {d_ref_s}",
        )
    pb, saz, dow, eventos, ant, pi, aj_reg, aj_ind, d_val = valores

    # Identifica regras fonte que casaram em cada fator
    regras_priori = {
        "sazonalidade": _explicar_sazonalidade(d_alvo, unit),
        "dia_semana": _explicar_dia_semana(d_alvo, unit),
        "eventos": _explicar_eventos(d_alvo, unit),
        "antecedencia": _explicar_antecedencia(d_alvo, d_ref, unit),
    }
    regras_posteriori = {
        "ocupacao_regiao": _explicar_ocupacao_regiao(d_alvo, d_ref, unit),
        # ocupacao_individual placeholder (sempre 0 hoje)
    }

    return {
        "unidade": unit,
        "data": d_alvo_s,
        "data_referencia": d_ref_s,
        "pb": float(pb),
        "fatores_priori": [
            {
                "tipo": "sazonalidade",
                "label": "Sazonalidade",
                "ajuste_pct": float(saz or 0.0),
                "regras": regras_priori["sazonalidade"],
                "link_crud": "/regras#sazonalidade",
            },
            {
                "tipo": "dia_semana",
                "label": "Dia da semana",
                "ajuste_pct": float(dow or 0.0),
                "regras": regras_priori["dia_semana"],
                "link_crud": "/regras#dia_semana",
            },
            {
                "tipo": "eventos",
                "label": "Eventos",
                "ajuste_pct": float(eventos or 0.0),
                "regras": regras_priori["eventos"],
                "link_crud": "/regras#eventos",
            },
            {
                "tipo": "antecedencia",
                "label": "Antecedência",
                "ajuste_pct": float(ant or 0.0),
                "regras": regras_priori["antecedencia"],
                "link_crud": "/regras#antecedencia",
            },
        ],
        "pi": float(pi),
        "fatores_posteriori": [
            {
                "tipo": "ocupacao_regiao",
                "label": "Ocupação da região",
                "ajuste_pct": float(aj_reg or 0.0),
                "regras": regras_posteriori["ocupacao_regiao"],
                "link_crud": "/regras#ocupacao",
            },
            {
                "tipo": "ocupacao_individual",
                "label": "Ocupação individual",
                "ajuste_pct": float(aj_ind or 0.0),
                "regras": [],
                "link_crud": None,
                "nota": "placeholder — não implementado",
            },
        ],
        "d": float(d_val),
    }


# ── Helpers de matching de regras fonte ────────────────────────


def _esc_match_clause(unit: dict) -> str:
    """SQL clause pra match de escopo — multi-nível (global/regiao/predio/unidade/segmento)."""
    return f"""(
      r.escopo = 'global'
      OR (r.escopo = 'regiao'   AND r.escopo_id = {int(unit['regiao_id'])})
      OR (r.escopo = 'predio'   AND r.escopo_id = {int(unit['predio_id'])})
      OR (r.escopo = 'segmento' AND r.escopo_id = {int(unit['segmento_id']) if unit['segmento_id'] is not None else 0})
      OR (r.escopo = 'unidade'  AND r.escopo_id = {int(unit['unidade_id'])})
    )"""


def _saz_parquet() -> str:
    return f"'{(DATA_ROOT / 'regras_priori/regras_sazonalidade/regras_sazonalidade.parquet').as_posix()}'"


def _dow_parquet() -> str:
    return f"'{(DATA_ROOT / 'regras_priori/regras_dia_semana/regras_dia_semana.parquet').as_posix()}'"


def _eventos_parquet() -> str:
    return f"'{(DATA_ROOT / 'regras_priori/eventos/eventos.parquet').as_posix()}'"


def _evento_impactos_parquet() -> str:
    return f"'{(DATA_ROOT / 'regras_priori/evento_impactos/evento_impactos.parquet').as_posix()}'"


def _antecedencia_parquet() -> str:
    return f"'{(DATA_ROOT / 'regras_priori/regras_antecedencia/regras_antecedencia.parquet').as_posix()}'"


def _ocup_regiao_parquet() -> str:
    return f"'{(DATA_ROOT / 'regras_posteriori/regras_ocupacao_regiao/regras_ocupacao_regiao.parquet').as_posix()}'"


def _explicar_sazonalidade(d_alvo: date, unit: dict) -> list[dict]:
    """Todas as regras de sazonalidade que cobrem essa (unidade, data) — somam."""
    rows = CON.execute(
        f"""
        SELECT r.regra_id, r.escopo, r.escopo_id, r.ajuste_pct,
               r.data_inicio, r.data_fim, r.nome
        FROM read_parquet({_saz_parquet()}) r
        WHERE DATE '{d_alvo.isoformat()}' BETWEEN r.data_inicio AND r.data_fim
          AND {_esc_match_clause(unit)}
        ORDER BY r.regra_id
        """
    ).fetchall()
    return [
        {
            "regra_id": int(r[0]),
            "escopo": r[1],
            "escopo_id": int(r[2]) if r[2] is not None else None,
            "ajuste_pct": float(r[3]),
            "label": (r[6] or f"Sazonalidade {r[4]} → {r[5]}"),
        }
        for r in rows
    ]


def _explicar_dia_semana(d_alvo: date, unit: dict) -> list[dict]:
    """Regra DOW que casou (mais específica ganha)."""
    dow_idx = (d_alvo.weekday())  # 0=segunda
    DOW_NAMES = ["segunda", "terça", "quarta", "quinta", "sexta", "sábado", "domingo"]
    row = CON.execute(
        f"""
        SELECT r.regra_id, r.escopo, r.escopo_id, r.ajuste_pct,
               CASE r.escopo WHEN 'predio' THEN 1 WHEN 'regiao' THEN 2 WHEN 'global' THEN 3 ELSE 99 END AS prio
        FROM read_parquet({_dow_parquet()}) r
        WHERE r.dia_semana = {dow_idx}
          AND r.escopo IN ('global','regiao','predio')
          AND {_esc_match_clause(unit)}
        ORDER BY prio
        LIMIT 1
        """
    ).fetchone()
    if not row:
        return []
    return [
        {
            "regra_id": int(row[0]),
            "escopo": row[1],
            "escopo_id": int(row[2]) if row[2] is not None else None,
            "ajuste_pct": float(row[3]),
            "label": f"{DOW_NAMES[dow_idx].capitalize()} ({row[1]})",
        }
    ]


def _explicar_eventos(d_alvo: date, unit: dict) -> list[dict]:
    """Pra cada evento ativo na data, a regra de impacto mais específica."""
    rows = CON.execute(
        f"""
        WITH eventos AS (
          SELECT evento_id, nome, data_inicio, data_fim
          FROM read_parquet({_eventos_parquet()})
          WHERE DATE '{d_alvo.isoformat()}' BETWEEN data_inicio AND data_fim
        ),
        impactos AS (
          SELECT evento_id, escopo, escopo_id, ajuste_pct, impacto_id,
                 CASE escopo WHEN 'unidade' THEN 1 WHEN 'predio' THEN 2 WHEN 'regiao' THEN 3 WHEN 'global' THEN 4 ELSE 99 END AS prio
          FROM read_parquet({_evento_impactos_parquet()}) r
          WHERE (
            (r.escopo='unidade' AND r.escopo_id={int(unit['unidade_id'])})
            OR (r.escopo='predio' AND r.escopo_id={int(unit['predio_id'])})
            OR (r.escopo='regiao' AND r.escopo_id={int(unit['regiao_id'])})
            OR r.escopo='global'
          )
        ),
        ranked AS (
          SELECT e.evento_id, e.nome, i.escopo, i.escopo_id, i.ajuste_pct, i.impacto_id,
                 ROW_NUMBER() OVER (PARTITION BY e.evento_id ORDER BY i.prio) AS rn
          FROM eventos e JOIN impactos i USING(evento_id)
        )
        SELECT evento_id, nome, escopo, escopo_id, ajuste_pct, impacto_id
        FROM ranked WHERE rn=1
        """
    ).fetchall()
    return [
        {
            "regra_id": int(r[5]) if r[5] is not None else int(r[0]),
            "evento_id": int(r[0]),
            "escopo": r[2],
            "escopo_id": int(r[3]) if r[3] is not None else None,
            "ajuste_pct": float(r[4]),
            "label": f"{r[1]} ({r[2]})",
        }
        for r in rows
    ]


def _explicar_antecedencia(d_alvo: date, d_ref: date, unit: dict) -> list[dict]:
    """Faixa de antecedência que casou."""
    lead = (d_alvo - d_ref).days
    dow_idx = d_alvo.weekday()
    row = CON.execute(
        f"""
        SELECT r.regra_id, r.lead_min_dias, r.lead_max_dias, r.dia_semana, r.ajuste_pct,
               CASE WHEN r.dia_semana IS NULL THEN 2 ELSE 1 END AS prio
        FROM read_parquet({_antecedencia_parquet()}) r
        WHERE {lead} >= r.lead_min_dias AND {lead} < r.lead_max_dias
          AND (r.dia_semana IS NULL OR r.dia_semana = {dow_idx})
        ORDER BY prio
        LIMIT 1
        """
    ).fetchone()
    if not row:
        return []
    label = f"Lead {row[1]}–{row[2]}d"
    if row[3] is not None:
        DOW_NAMES = ["seg", "ter", "qua", "qui", "sex", "sáb", "dom"]
        label += f" / {DOW_NAMES[int(row[3])]}"
    return [
        {
            "regra_id": int(row[0]),
            "lead_min_dias": int(row[1]),
            "lead_max_dias": int(row[2]),
            "dia_semana": int(row[3]) if row[3] is not None else None,
            "ajuste_pct": float(row[4]),
            "label": label,
        }
    ]


def _explicar_ocupacao_regiao(d_alvo: date, d_ref: date, unit: dict) -> list[dict]:
    """Bucket de antecedência + banda de ocupação que casou."""
    # Lê ocupação real da região naquela data
    ocup_row = CON.execute(
        f"""
        SELECT ocupacao_pct
        FROM {SIMULADOR_ALIAS}.main.ocupacao_regiao
        WHERE data_referencia=DATE '{d_ref.isoformat()}'
          AND regiao_id={int(unit['regiao_id'])}
          AND data=DATE '{d_alvo.isoformat()}'
        """
    ).fetchone()
    if not ocup_row or ocup_row[0] is None:
        return []
    ocup_pct = float(ocup_row[0])

    # Determina bucket (igual lógica de fat_ajuste_regiao)
    lead = max(0, (d_alvo - d_ref).days)
    janelas = CON.execute(
        f"SELECT DISTINCT janela_dias FROM read_parquet({_ocup_regiao_parquet()}) ORDER BY janela_dias"
    ).fetchall()
    janelas = [int(j[0]) for j in janelas]
    if not janelas:
        return []
    bucket = next((j for j in janelas if j >= lead), max(janelas))

    row = CON.execute(
        f"""
        SELECT r.regra_id, r.janela_dias, r.ocupacao_min_pct, r.ocupacao_max_pct, r.ajuste_pct
        FROM read_parquet({_ocup_regiao_parquet()}) r
        WHERE r.janela_dias = {bucket}
          AND {ocup_pct} >= r.ocupacao_min_pct
          AND {ocup_pct} <  r.ocupacao_max_pct
        LIMIT 1
        """
    ).fetchone()
    if not row:
        return []
    return [
        {
            "regra_id": int(row[0]),
            "janela_dias": int(row[1]),
            "ocupacao_min_pct": float(row[2]),
            "ocupacao_max_pct": float(row[3]),
            "ajuste_pct": float(row[4]),
            "ocupacao_real_pct": ocup_pct,
            "label": (
                f"Bucket {row[1]}d, ocupação {row[2]*100:.0f}%–{row[3]*100:.0f}% "
                f"(real: {ocup_pct*100:.1f}%)"
            ),
        }
    ]


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


# ─── Ocupação (região) ─────────────────────────────────────

OCUP_REGIAO_PARQUET = (
    DATA_ROOT / "regras_posteriori" / "regras_ocupacao_regiao" / "regras_ocupacao_regiao.parquet"
)


def _read_ocup_regiao_df() -> pd.DataFrame:
    if not OCUP_REGIAO_PARQUET.exists():
        return pd.DataFrame(
            columns=[
                "regra_id", "escopo", "escopo_id",
                "janela_dias", "ocupacao_min_pct", "ocupacao_max_pct",
                "ajuste_pct", "cumulativo",
            ]
        )
    df = pd.read_parquet(OCUP_REGIAO_PARQUET)
    if "ativo" in df.columns:
        df = df.drop(columns=["ativo"])
    return df


def _write_ocup_regiao_df(df: pd.DataFrame) -> None:
    OCUP_REGIAO_PARQUET.parent.mkdir(parents=True, exist_ok=True)
    if not df.empty:
        df = df.copy()
        df["regra_id"] = df["regra_id"].astype("int64")
        df["escopo_id"] = df["escopo_id"].astype("Int64")
        df["janela_dias"] = df["janela_dias"].astype("int64")
        df["ocupacao_min_pct"] = df["ocupacao_min_pct"].astype(float)
        df["ocupacao_max_pct"] = df["ocupacao_max_pct"].astype(float)
        df["ajuste_pct"] = df["ajuste_pct"].astype(float)
    df.to_parquet(OCUP_REGIAO_PARQUET, index=False)


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


@app.get("/regras/ocupacao-regiao")
def listar_ocupacao_regiao() -> dict:
    df = _read_ocup_regiao_df()
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


@app.post("/regras/ocupacao-regiao/bucket", status_code=201)
def criar_bucket_ocup(
    body: BucketOcupIn,
    x_usuario: Optional[str] = Header(default=None, alias="X-Usuario"),
) -> dict:
    _validar_bucket_ocup(body)
    df = _read_ocup_regiao_df()
    if (df["janela_dias"] == body.janela_dias).any():
        raise HTTPException(status_code=409, detail=f"Bucket {body.janela_dias} já existe")
    next_id = int(df["regra_id"].max()) + 1 if not df.empty else 1
    linhas = _bucket_para_linhas(body, next_id)
    df = pd.concat([df, pd.DataFrame(linhas)], ignore_index=True)
    _write_ocup_regiao_df(df)
    log_operacao(
        _get_usuario(x_usuario), "create",
        "regras.ocupacao_regiao", str(body.janela_dias),
        {"limites": body.limites, "ajustes": body.ajustes},
    )
    return {"ok": True}


@app.put("/regras/ocupacao-regiao/bucket/{janela_dias}")
def atualizar_bucket_ocup(
    janela_dias: int, body: BucketOcupIn,
    x_usuario: Optional[str] = Header(default=None, alias="X-Usuario"),
) -> dict:
    _validar_bucket_ocup(body)
    df = _read_ocup_regiao_df()

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
    _write_ocup_regiao_df(df)
    log_operacao(
        _get_usuario(x_usuario), "update",
        "regras.ocupacao_regiao", str(janela_dias),
        {"nova_janela": body.janela_dias, "limites": body.limites, "ajustes": body.ajustes},
    )
    return {"ok": True}


@app.delete("/regras/ocupacao-regiao/bucket/{janela_dias}")
def deletar_bucket_ocup(
    janela_dias: int,
    x_usuario: Optional[str] = Header(default=None, alias="X-Usuario"),
) -> dict:
    df = _read_ocup_regiao_df()
    mask = df["janela_dias"] == janela_dias
    if not mask.any():
        raise HTTPException(status_code=404, detail="Bucket não encontrado")
    removidas = int(mask.sum())
    df = df[~mask].copy()
    _write_ocup_regiao_df(df)
    log_operacao(
        _get_usuario(x_usuario), "delete",
        "regras.ocupacao_regiao", str(janela_dias),
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


# ============================================================
# Publicações no Guesty — snapshots persistentes pra rollback / diff
# ============================================================
# Cada publicação fica salva em parquet:
#   - publicacoes.parquet: header (1 linha por publicação)
#   - publicacao_precos.parquet: detalhe (todos preços enviados)
# Permite listar, comparar e reverter publicações.

PUBS_DIR = DATA_ROOT / "auditoria" / "publicacoes_guesty"
PUBS_HEADER = PUBS_DIR / "publicacoes.parquet"
PUBS_PRECOS = PUBS_DIR / "publicacao_precos.parquet"

PUBS_HEADER_COLS = [
    "publicacao_id", "timestamp", "usuario", "modo", "tipo",
    "escopo", "regiao_id", "regiao_nome",
    "data_referencia", "periodo_ini", "periodo_fim",
    "total_precos", "sucessos", "falhas",
    "impacto_total", "duration_ms",
    "referencia_id", "observacoes",
]

PUBS_PRECOS_COLS = [
    "publicacao_id", "unidade_id", "data", "valor", "valor_anterior",
]


def _read_pubs_header() -> pd.DataFrame:
    if not PUBS_HEADER.exists():
        return pd.DataFrame(columns=PUBS_HEADER_COLS)
    return pd.read_parquet(PUBS_HEADER)


def _read_pubs_precos(pub_id: Optional[int] = None) -> pd.DataFrame:
    if not PUBS_PRECOS.exists():
        return pd.DataFrame(columns=PUBS_PRECOS_COLS)
    df = pd.read_parquet(PUBS_PRECOS)
    if pub_id is not None:
        df = df[df["publicacao_id"] == pub_id].copy()
    return df


def _next_pub_id() -> int:
    df = _read_pubs_header()
    if df.empty:
        return 1
    return int(df["publicacao_id"].max()) + 1


def _save_pub_snapshot(
    header: dict,
    precos_df: pd.DataFrame,
) -> None:
    """Append no header parquet + append nos preços. Idempotente em re-tentativas?
    Não — assume publicacao_id único."""
    PUBS_DIR.mkdir(parents=True, exist_ok=True)

    df_h = _read_pubs_header()
    df_h = pd.concat([df_h, pd.DataFrame([header], columns=PUBS_HEADER_COLS)], ignore_index=True)
    df_h.to_parquet(PUBS_HEADER, index=False)

    df_p = _read_pubs_precos()
    if not precos_df.empty:
        df_p = pd.concat([df_p, precos_df], ignore_index=True)
        df_p.to_parquet(PUBS_PRECOS, index=False)


def _last_pub_for_unit_data() -> dict:
    """Retorna {(unidade_id, data_iso): valor} da última publicação por unidade-data,
    pra computar valor_anterior. Eficiência: 1 query no parquet inteiro,
    pega o último por (unidade_id, data) via timestamp do header."""
    df_h = _read_pubs_header()
    if df_h.empty:
        return {}
    df_p = _read_pubs_precos()
    if df_p.empty:
        return {}
    merged = df_p.merge(
        df_h[["publicacao_id", "timestamp"]],
        on="publicacao_id",
        how="left",
    )
    merged = merged.sort_values("timestamp").drop_duplicates(
        subset=["unidade_id", "data"], keep="last"
    )
    return {
        (int(r.unidade_id), str(r.data)): float(r.valor)
        for r in merged.itertuples()
    }


# ============================================================
# Guesty — publicação de preços (MOCK até OAuth ser configurado)
# ============================================================
# Quando a integração com a Guesty estiver real, o endpoint de publicar
# vai despachar um job assíncrono que percorre o `d` e faz PUT no
# /availability-pricing/api/calendar/listings/{id} respeitando rate limit
# (5.000 req/h, 120/min, 15/seg). Por enquanto, simula sucesso e registra
# na auditoria pra exercitar o fluxo de UI.


@app.get("/guesty/publicar/preview")
def guesty_publicar_preview(
    data_referencia: str,
    data_inicio: str,
    data_fim: str,
    regiao_id: Optional[int] = None,
) -> dict:
    """Stats da publicação proposta, sem efetuar nada. Usado pelo modal."""
    try:
        d_ref = date.fromisoformat(data_referencia)
        d_ini = date.fromisoformat(data_inicio)
        d_fim = date.fromisoformat(data_fim)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"Data inválida: {e}")
    if d_fim < d_ini:
        raise HTTPException(status_code=400, detail="data_fim anterior a data_inicio")

    where_regiao = ""
    if regiao_id is not None:
        where_regiao = f"""
          AND d.unidade_id IN (
            SELECT u.unidade_id FROM cadastro.unidades u
            JOIN cadastro.predios p USING(predio_id)
            WHERE p.regiao_id = {int(regiao_id)}
          )
        """

    row = CON.execute(
        f"""
        SELECT
          COUNT(DISTINCT d.unidade_id) AS unidades,
          COUNT(*) AS total_precos,
          SUM(d.valor - pb.valor) AS impacto_total,
          AVG(d.valor) AS preco_medio
        FROM {SIMULADOR_ALIAS}.main.d d
        JOIN {SIMULADOR_ALIAS}.main.pb pb USING(data_referencia, unidade_id, data)
        WHERE d.data_referencia = DATE '{d_ref.isoformat()}'
          AND d.data BETWEEN DATE '{d_ini.isoformat()}' AND DATE '{d_fim.isoformat()}'
          {where_regiao}
        """
    ).fetchone()

    dias = (d_fim - d_ini).days + 1
    return {
        "unidades": int(row[0] or 0),
        "dias": dias,
        "total_precos": int(row[1] or 0),
        "impacto_total": float(row[2] or 0.0),
        "preco_medio": float(row[3] or 0.0),
    }


class GuestyPublicarBody(BaseModel):
    data_referencia: str
    data_inicio: str
    data_fim: str
    regiao_id: Optional[int] = None
    sobrescrever_travados: bool = False
    pular_bloqueios: bool = True


# Categorias de erro que podem acontecer numa publicação real no Guesty.
# Distribuição (peso) é só pro mock — ajusta a frequência relativa de cada
# tipo no toast de "X falharam" pra demo parecer realista.
GUESTY_ERROR_CATEGORIES = [
    # (key, label, recuperavel, peso)
    ("rate_limit",            "Rate limit (HTTP 429)",     True,  0.40),
    ("erro_rede",             "Erro de rede (5xx)",        True,  0.20),
    ("listing_inativo",       "Listing inativo",           False, 0.15),
    ("data_travada",          "Data travada manualmente",  False, 0.10),
    ("listing_nao_encontrado", "Listing não encontrado",   False, 0.10),
    ("moeda_incompativel",    "Moeda incompatível",        False, 0.05),
]


def _gerar_erros_mock(
    falhas: int,
    body: "GuestyPublicarBody",
) -> list[dict]:
    """Sorteia (unidade_id, data) reais do `d` no escopo dado e atribui
    categorias de erro. Determinístico (seed fixo) pra demo previsível."""
    if falhas <= 0:
        return []

    where_regiao = ""
    if body.regiao_id is not None:
        where_regiao = f"""
          AND d.unidade_id IN (
            SELECT u.unidade_id FROM cadastro.unidades u
            JOIN cadastro.predios p USING(predio_id)
            WHERE p.regiao_id = {int(body.regiao_id)}
          )
        """
    rows = CON.execute(
        f"""
        SELECT d.unidade_id, u.codigo_externo, d.data
        FROM {SIMULADOR_ALIAS}.main.d d
        JOIN cadastro.unidades u ON u.unidade_id = d.unidade_id
        WHERE d.data_referencia = DATE '{body.data_referencia}'
          AND d.data BETWEEN DATE '{body.data_inicio}' AND DATE '{body.data_fim}'
          {where_regiao}
        ORDER BY HASH((d.unidade_id, d.data))
        LIMIT {falhas}
        """
    ).fetchall()

    rng = random.Random(42)
    erros = []
    for uid, label, dt in rows:
        r = rng.random()
        cumulative = 0.0
        for key, lbl, recup, peso in GUESTY_ERROR_CATEGORIES:
            cumulative += peso
            if r < cumulative:
                erros.append({
                    "unidade_id": int(uid),
                    "unidade_label": label,
                    "data": dt.isoformat(),
                    "motivo": key,
                    "motivo_label": lbl,
                    "recuperavel": recup,
                })
                break
    return erros


@app.post("/guesty/publicar")
def guesty_publicar(
    body: GuestyPublicarBody,
    x_usuario: Optional[str] = Header(default=None, alias="X-Usuario"),
) -> dict:
    """
    MOCK: simula a publicação dos preços do `d` no Guesty.

    Quando OAuth com Guesty estiver configurado, este handler vai:
      1. Carregar (unidade_id, data, valor) da matriz `d` no escopo dado
      2. Mapear unidade_id → guesty_listing_id
      3. Run-length encode dias contíguos com mesmo preço por listing
      4. PUT /availability-pricing/api/calendar/listings/{id} com retry/backoff
      5. Coletar sucessos/falhas e registrar na auditoria

    Por enquanto: simula 1.5s de "rede", retorna sucesso + amostra de erros
    categorizados (pra UI ter o que mostrar no estado de falha).
    """
    preview = guesty_publicar_preview(
        body.data_referencia,
        body.data_inicio,
        body.data_fim,
        body.regiao_id,
    )

    t0 = time.perf_counter()
    time.sleep(1.5)  # simula latência de rede com rate limit
    duration_ms = int((time.perf_counter() - t0) * 1000)

    # Mock: simula ~0.3% de falhas (rate limit / listing inativo / etc)
    total = preview["total_precos"]
    falhas_count = max(0, int(total * 0.003)) if total > 0 else 0
    sucessos = total - falhas_count
    erros = _gerar_erros_mock(falhas_count, body)

    # Resumo por categoria (pra UI mostrar agrupado antes do detalhe linha-a-linha)
    resumo: dict[str, dict] = {}
    for e in erros:
        k = e["motivo"]
        if k not in resumo:
            resumo[k] = {
                "motivo": k,
                "motivo_label": e["motivo_label"],
                "recuperavel": e["recuperavel"],
                "quantidade": 0,
            }
        resumo[k]["quantidade"] += 1
    resumo_list = sorted(resumo.values(), key=lambda x: -x["quantidade"])

    # Persistir snapshot da publicação (header + todos os preços enviados)
    pub_id = _next_pub_id()
    snapshot_precos = _coletar_precos_publicados(body)
    if not snapshot_precos.empty:
        last_map = _last_pub_for_unit_data()
        snapshot_precos["valor_anterior"] = snapshot_precos.apply(
            lambda r: last_map.get((int(r["unidade_id"]), str(r["data"])), None),
            axis=1,
        )
        snapshot_precos.insert(0, "publicacao_id", pub_id)

    regiao_nome = None
    if body.regiao_id is not None:
        row = CON.execute(
            f"SELECT nome FROM cadastro.regioes WHERE regiao_id = {int(body.regiao_id)}"
        ).fetchone()
        regiao_nome = row[0] if row else None

    header = {
        "publicacao_id": pub_id,
        "timestamp": datetime.now(timezone.utc),
        "usuario": _get_usuario(x_usuario),
        "modo": "mock",
        "tipo": "publicacao",
        "escopo": "regiao" if body.regiao_id is not None else "todas",
        "regiao_id": body.regiao_id,
        "regiao_nome": regiao_nome,
        "data_referencia": body.data_referencia,
        "periodo_ini": body.data_inicio,
        "periodo_fim": body.data_fim,
        "total_precos": total,
        "sucessos": sucessos,
        "falhas": falhas_count,
        "impacto_total": float(preview["impacto_total"]),
        "duration_ms": duration_ms,
        "referencia_id": None,
        "observacoes": None,
    }
    _save_pub_snapshot(header, snapshot_precos)

    log_operacao(
        _get_usuario(x_usuario),
        "publicacao_mock",
        "guesty.publicacao",
        str(pub_id),
        {
            "publicacao_id": pub_id,
            "data_inicio": body.data_inicio,
            "data_fim": body.data_fim,
            "regiao_id": body.regiao_id,
            "sobrescrever_travados": body.sobrescrever_travados,
            "pular_bloqueios": body.pular_bloqueios,
            "total_precos": total,
            "sucessos": sucessos,
            "falhas": falhas_count,
            "resumo_falhas": resumo_list,
            "duration_ms": duration_ms,
            "modo": "mock",
        },
    )

    return {
        "ok": True,
        "modo": "mock",
        "publicacao_id": pub_id,
        "duration_ms": duration_ms,
        "total_precos": total,
        "sucessos": sucessos,
        "falhas": falhas_count,
        "impacto_total": preview["impacto_total"],
        "erros": erros,
        "resumo_falhas": resumo_list,
    }


def _coletar_precos_publicados(body: "GuestyPublicarBody") -> pd.DataFrame:
    """Lê (unidade_id, data, valor) do `d` no escopo dado pra snapshot."""
    where_regiao = ""
    if body.regiao_id is not None:
        where_regiao = f"""
          AND d.unidade_id IN (
            SELECT u.unidade_id FROM cadastro.unidades u
            JOIN cadastro.predios p USING(predio_id)
            WHERE p.regiao_id = {int(body.regiao_id)}
          )
        """
    return CON.execute(
        f"""
        SELECT d.unidade_id, d.data::VARCHAR AS data, d.valor
        FROM {SIMULADOR_ALIAS}.main.d d
        WHERE d.data_referencia = DATE '{body.data_referencia}'
          AND d.data BETWEEN DATE '{body.data_inicio}' AND DATE '{body.data_fim}'
          {where_regiao}
        """
    ).df()


# ============================================================
# Publicações — listar, detalhes, diff, rollback
# ============================================================


def _publicacao_to_dict(row: pd.Series) -> dict:
    """Normaliza uma linha do header parquet pra dict serializável."""
    d = row.to_dict()
    # timestamp e datas viram string
    ts = d.get("timestamp")
    if ts is not None and not isinstance(ts, str):
        d["timestamp"] = pd.Timestamp(ts).isoformat()
    for k in ("data_referencia", "periodo_ini", "periodo_fim"):
        v = d.get(k)
        if v is not None and not isinstance(v, str):
            d[k] = pd.Timestamp(v).strftime("%Y-%m-%d") if pd.notna(v) else None
    # NaN → None
    for k, v in list(d.items()):
        if pd.isna(v) if not isinstance(v, (list, dict)) else False:
            d[k] = None
    return d


@app.get("/publicacoes")
def listar_publicacoes(
    page: int = 1,
    page_size: int = 50,
) -> dict:
    """Lista paginada de publicações, ordenada por timestamp desc."""
    df = _read_pubs_header()
    if df.empty:
        return {"items": [], "total": 0, "page": 1, "page_size": page_size}

    df = df.sort_values("timestamp", ascending=False).reset_index(drop=True)
    total = len(df)
    page = max(1, int(page))
    page_size = max(1, min(200, int(page_size)))
    start = (page - 1) * page_size
    chunk = df.iloc[start : start + page_size]
    items = [_publicacao_to_dict(r) for _, r in chunk.iterrows()]
    return {"items": items, "total": int(total), "page": page, "page_size": page_size}


@app.get("/publicacoes/{publicacao_id}")
def detalhes_publicacao(publicacao_id: int) -> dict:
    df_h = _read_pubs_header()
    row = df_h[df_h["publicacao_id"] == publicacao_id]
    if row.empty:
        raise HTTPException(status_code=404, detail=f"publicação {publicacao_id} não encontrada")
    header = _publicacao_to_dict(row.iloc[0])

    # Encontra publicação anterior (timestamp imediatamente menor) — pra diff default
    df_h_sorted = df_h.sort_values("timestamp")
    idx = df_h_sorted.index[df_h_sorted["publicacao_id"] == publicacao_id].tolist()
    anterior_id = None
    if idx:
        pos = df_h_sorted.index.get_loc(idx[0])
        if pos > 0:
            anterior_id = int(df_h_sorted.iloc[pos - 1]["publicacao_id"])
    header["anterior_id"] = anterior_id
    return header


@app.get("/publicacoes/{publicacao_id}/diff")
def diff_publicacao(
    publicacao_id: int,
    vs: Optional[int] = None,
) -> dict:
    """Compara a publicação `publicacao_id` com a `vs` (default = anterior).
    Retorna stats agregadas + matriz de delta (unidade × data) pronta pra heatmap."""
    df_h = _read_pubs_header()
    row = df_h[df_h["publicacao_id"] == publicacao_id]
    if row.empty:
        raise HTTPException(status_code=404, detail=f"publicação {publicacao_id} não encontrada")

    if vs is None:
        det = detalhes_publicacao(publicacao_id)
        vs = det.get("anterior_id")

    df_p_atual = _read_pubs_precos(publicacao_id)[["unidade_id", "data", "valor"]].reset_index(drop=True)
    df_p_atual = df_p_atual.rename(columns={"valor": "valor_atual"})

    if vs is None:
        # Sem publicação anterior — diff é "tudo novo"
        merged = df_p_atual.copy()
        merged["valor_anterior"] = None
        merged["delta"] = None
    else:
        df_p_vs = _read_pubs_precos(int(vs))[["unidade_id", "data", "valor"]].reset_index(drop=True)
        df_p_vs = df_p_vs.rename(columns={"valor": "valor_anterior"})
        merged = df_p_atual.merge(df_p_vs, on=["unidade_id", "data"], how="outer").reset_index(drop=True)
        merged["delta"] = merged["valor_atual"] - merged["valor_anterior"]

    # Stats agregadas
    n_inalterados = int(((merged["delta"] == 0) | (merged["delta"].isna() & merged["valor_anterior"].notna() & merged["valor_atual"].notna() & (merged["valor_anterior"] == merged["valor_atual"]))).sum())
    n_aumentaram = int((merged["delta"] > 0).sum())
    n_diminuiram = int((merged["delta"] < 0).sum())
    n_novos = int((merged["valor_anterior"].isna() & merged["valor_atual"].notna()).sum())
    n_removidos = int((merged["valor_atual"].isna() & merged["valor_anterior"].notna()).sum())
    impacto_total = float(merged["delta"].fillna(0).sum())

    # Top 20 maiores deltas (positivos e negativos)
    top = merged.dropna(subset=["delta"]).copy()
    top = top.assign(delta_abs=top["delta"].abs()).sort_values("delta_abs", ascending=False).head(20)

    # Labels de unidade pra top
    if not top.empty:
        unit_ids = ",".join(str(int(u)) for u in top["unidade_id"].unique())
        unit_labels = CON.execute(
            f"SELECT unidade_id, codigo_externo FROM cadastro.unidades WHERE unidade_id IN ({unit_ids})"
        ).fetchall()
        label_map = {int(u[0]): u[1] for u in unit_labels}
    else:
        label_map = {}

    top_list = [
        {
            "unidade_id": int(r["unidade_id"]),
            "unidade_label": label_map.get(int(r["unidade_id"]), str(int(r["unidade_id"]))),
            "data": str(r["data"]),
            "valor_anterior": float(r["valor_anterior"]) if pd.notna(r["valor_anterior"]) else None,
            "valor_atual": float(r["valor_atual"]) if pd.notna(r["valor_atual"]) else None,
            "delta": float(r["delta"]) if pd.notna(r["delta"]) else None,
        }
        for _, r in top.iterrows()
    ]

    # Matriz pra heatmap: linhas = unidades (top 50 por |delta|), colunas = datas
    # Sem isso, com 233 unidades a UI fica pesada. Usuário paginá pra ver mais.
    matriz_top = merged.dropna(subset=["delta"]).copy()
    if not matriz_top.empty:
        # Ranking de unidades pelo somatório absoluto do delta
        rank = matriz_top.assign(d_abs=matriz_top["delta"].abs()).groupby("unidade_id")["d_abs"].sum().sort_values(ascending=False)
        top_units = rank.head(50).index.tolist()
        matriz_top = matriz_top[matriz_top["unidade_id"].isin(top_units)]
    if not matriz_top.empty:
        unit_ids2 = ",".join(str(int(u)) for u in matriz_top["unidade_id"].unique())
        unit_labels2 = CON.execute(
            f"SELECT unidade_id, codigo_externo FROM cadastro.unidades WHERE unidade_id IN ({unit_ids2}) ORDER BY codigo_externo"
        ).fetchall()
        ordered_units = [(int(u[0]), u[1]) for u in unit_labels2]
        all_dates = sorted(matriz_top["data"].unique().tolist())
        delta_map = {(int(r["unidade_id"]), str(r["data"])): float(r["delta"]) for _, r in matriz_top.iterrows()}
        atual_map = {(int(r["unidade_id"]), str(r["data"])): (float(r["valor_atual"]) if pd.notna(r["valor_atual"]) else None) for _, r in matriz_top.iterrows()}
        matriz_rows = [
            {
                "unidade_id": uid,
                "label": lbl,
                "deltas": [delta_map.get((uid, d)) for d in all_dates],
                "valores": [atual_map.get((uid, d)) for d in all_dates],
            }
            for uid, lbl in ordered_units
        ]
        matriz = {"columns": all_dates, "rows": matriz_rows}
    else:
        matriz = {"columns": [], "rows": []}

    return {
        "publicacao_id": publicacao_id,
        "vs": vs,
        "stats": {
            "n_inalterados": n_inalterados,
            "n_aumentaram": n_aumentaram,
            "n_diminuiram": n_diminuiram,
            "n_novos": n_novos,
            "n_removidos": n_removidos,
            "impacto_total": impacto_total,
        },
        "top": top_list,
        "matriz": matriz,
    }


@app.post("/publicacoes/{publicacao_id}/rollback")
def rollback_publicacao(
    publicacao_id: int,
    x_usuario: Optional[str] = Header(default=None, alias="X-Usuario"),
) -> dict:
    """MOCK: re-publica os preços do snapshot dado, criando uma nova publicação tipo=rollback."""
    df_h = _read_pubs_header()
    row = df_h[df_h["publicacao_id"] == publicacao_id]
    if row.empty:
        raise HTTPException(status_code=404, detail=f"publicação {publicacao_id} não encontrada")
    pub = row.iloc[0]

    df_p_origem = _read_pubs_precos(publicacao_id)
    if df_p_origem.empty:
        raise HTTPException(status_code=400, detail="snapshot vazio — nada pra reverter")

    t0 = time.perf_counter()
    time.sleep(1.5)
    duration_ms = int((time.perf_counter() - t0) * 1000)

    total = len(df_p_origem)
    falhas = max(0, int(total * 0.002))  # rollback é mais "limpo", menos falhas
    sucessos = total - falhas

    # Calcula novo valor_anterior (último estado conhecido) e impacto vs último
    last_map = _last_pub_for_unit_data()
    novo_pub_id = _next_pub_id()
    novo_precos = df_p_origem[["unidade_id", "data", "valor"]].copy()
    novo_precos["valor_anterior"] = novo_precos.apply(
        lambda r: last_map.get((int(r["unidade_id"]), str(r["data"])), None),
        axis=1,
    )
    novo_precos.insert(0, "publicacao_id", novo_pub_id)
    impacto_total = float(
        (novo_precos["valor"] - novo_precos["valor_anterior"].fillna(novo_precos["valor"])).sum()
    )

    header_novo = {
        "publicacao_id": novo_pub_id,
        "timestamp": datetime.now(timezone.utc),
        "usuario": _get_usuario(x_usuario),
        "modo": "mock",
        "tipo": "rollback",
        "escopo": pub["escopo"],
        "regiao_id": pub["regiao_id"] if pd.notna(pub["regiao_id"]) else None,
        "regiao_nome": pub["regiao_nome"] if pd.notna(pub["regiao_nome"]) else None,
        "data_referencia": pub["data_referencia"],
        "periodo_ini": pub["periodo_ini"],
        "periodo_fim": pub["periodo_fim"],
        "total_precos": total,
        "sucessos": sucessos,
        "falhas": falhas,
        "impacto_total": impacto_total,
        "duration_ms": duration_ms,
        "referencia_id": int(publicacao_id),
        "observacoes": f"rollback da publicação #{publicacao_id}",
    }
    _save_pub_snapshot(header_novo, novo_precos)

    log_operacao(
        _get_usuario(x_usuario),
        "rollback_publicacao",
        "guesty.publicacao",
        str(novo_pub_id),
        {
            "publicacao_id": novo_pub_id,
            "rollback_de": int(publicacao_id),
            "total_precos": total,
            "sucessos": sucessos,
            "falhas": falhas,
            "impacto_total": impacto_total,
            "duration_ms": duration_ms,
            "modo": "mock",
        },
    )

    return {
        "ok": True,
        "modo": "mock",
        "publicacao_id": novo_pub_id,
        "rollback_de": int(publicacao_id),
        "total_precos": total,
        "sucessos": sucessos,
        "falhas": falhas,
        "impacto_total": impacto_total,
        "duration_ms": duration_ms,
    }
