"""
Cria simulador.duckdb com as 12 tabelas normalizadas propostas no regras.pdf,
populadas a partir dos parquets em /data/ (que foram gerados a partir dos
CSVs reais em /tmp/).

Cada tabela "por unidade" tem schema:
    (data_referencia DATE, unidade_id BIGINT, data DATE, <valor|ajuste_pct>)

Cada tabela "por portfólio" tem schema:
    (data_referencia DATE, portfolio_id BIGINT, data DATE, <pct>)

Portfólio aqui = região (agrupamento de regiões conforme decidido).

Execução (da raiz do projeto ou de qualquer lugar):
    .venv/bin/python simulador/backend/build_simulator_db.py

O script é idempotente — apaga o .duckdb antes de recriar.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import duckdb

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent  # simulador/backend → simulador → pricing
DATA_ROOT = PROJECT_ROOT / "data"
DB_PATH = SCRIPT_DIR / "simulador.duckdb"

TODAY = date(2026, 4, 23)
HORIZON_DAYS = 365  # cobertura: hoje (d0) até hoje + 365 dias = 366 datas
VERSION = "0.1.0"


def pq(rel: str) -> str:
    return f"'{(DATA_ROOT / rel).as_posix()}'"


def count(con, table: str) -> int:
    return con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]


def main() -> None:
    if DB_PATH.exists():
        print(f"removendo DB existente: {DB_PATH}")
        DB_PATH.unlink()

    con = duckdb.connect(str(DB_PATH))

    print(f"origem dos dados: {DATA_ROOT}")
    print(f"destino (DB):     {DB_PATH}")
    print(f"data_referencia:  {TODAY}")
    print(f"horizonte:        {HORIZON_DAYS} dias\n")

    # ────────────────── tabelas auxiliares (temp) ──────────────────
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE cal AS
        SELECT (DATE '{TODAY}' + INTERVAL (n) DAY)::DATE AS data
        FROM range(0, {HORIZON_DAYS + 1}) t(n)
    """)
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE unit_info AS
        SELECT u.unidade_id, u.predio_id, p.regiao_id, u.segmento_id
        FROM read_parquet({pq('cadastro/unidades/unidades.parquet')}) u
        JOIN read_parquet({pq('cadastro/predios/predios.parquet')}) p USING(predio_id)
    """)
    print(f"unidades: {count(con, 'unit_info')}")
    print(f"dias no calendário: {count(con, 'cal')}\n")

    # ────────────────── 1) pb — Preço Base ──────────────────
    con.execute(f"""
        CREATE OR REPLACE TABLE pb AS
        SELECT
          DATE '{TODAY}' AS data_referencia,
          ui.unidade_id,
          c.data,
          pbs.valor
        FROM unit_info ui
        CROSS JOIN cal c
        JOIN (
          SELECT unidade_id, valor
          FROM read_parquet({pq('preco_base/precos_base/precos_base.parquet')})
          WHERE vigencia_fim IS NULL
        ) pbs USING(unidade_id)
    """)
    print(f"pb                    {count(con, 'pb'):>8} linhas")

    # ────────────────── 2) fat_sazonalidade ──────────────────
    con.execute(f"""
        CREATE OR REPLACE TABLE fat_sazonalidade AS
        SELECT
          DATE '{TODAY}' AS data_referencia,
          ui.unidade_id,
          c.data,
          COALESCE(SUM(r.ajuste_pct), 0.0)::DOUBLE AS ajuste_pct
        FROM unit_info ui
        CROSS JOIN cal c
        LEFT JOIN read_parquet({pq('regras_priori/regras_sazonalidade/regras_sazonalidade.parquet')}) r
          ON c.data BETWEEN r.data_inicio AND r.data_fim
          AND (
            r.escopo = 'global'
            OR (r.escopo = 'regiao'   AND r.escopo_id = ui.regiao_id)
            OR (r.escopo = 'predio'   AND r.escopo_id = ui.predio_id)
            OR (r.escopo = 'segmento' AND r.escopo_id = ui.segmento_id)
            OR (r.escopo = 'unidade'  AND r.escopo_id = ui.unidade_id)
          )
        GROUP BY ui.unidade_id, c.data
    """)
    print(f"fat_sazonalidade      {count(con, 'fat_sazonalidade'):>8} linhas")

    # ────────────────── 3) fat_dia_semana ──────────────────
    # Convenção DOW: 0=segunda … 6=domingo (igual Python .weekday())
    # DuckDB: (isodow(data) - 1) mapeia para essa convenção
    # Política: "mais específico ganha" — prédio > região > global.
    # Apenas regras ativas (ativo=true ou ativo ausente).
    con.execute(f"""
        CREATE OR REPLACE TABLE fat_dia_semana AS
        WITH regras AS (
          SELECT escopo, escopo_id, dia_semana, ajuste_pct,
                 CASE escopo
                   WHEN 'predio' THEN 1
                   WHEN 'regiao' THEN 2
                   WHEN 'global' THEN 3
                   ELSE 99
                 END AS prio_escopo
          FROM read_parquet({pq('regras_priori/regras_dia_semana/regras_dia_semana.parquet')})
          WHERE escopo IN ('global', 'regiao', 'predio')
        ),
        matched AS (
          SELECT
            ui.unidade_id,
            c.data,
            r.ajuste_pct,
            ROW_NUMBER() OVER (
              PARTITION BY ui.unidade_id, c.data
              ORDER BY r.prio_escopo
            ) AS rn
          FROM unit_info ui
          CROSS JOIN cal c
          JOIN regras r
            ON r.dia_semana = (isodow(c.data) - 1)
            AND (
              (r.escopo = 'predio' AND r.escopo_id = ui.predio_id)
              OR (r.escopo = 'regiao' AND r.escopo_id = ui.regiao_id)
              OR r.escopo = 'global'
            )
        )
        SELECT
          DATE '{TODAY}' AS data_referencia,
          ui.unidade_id,
          c.data,
          COALESCE(m.ajuste_pct, 0.0)::DOUBLE AS ajuste_pct
        FROM unit_info ui
        CROSS JOIN cal c
        LEFT JOIN matched m
          ON m.unidade_id = ui.unidade_id AND m.data = c.data AND m.rn = 1
    """)
    print(f"fat_dia_semana        {count(con, 'fat_dia_semana'):>8} linhas")

    # ────────────────── 4) fat_eventos ──────────────────
    # Política: para cada (unidade, dia, evento), usar a regra MAIS ESPECÍFICA
    # (unidade > predio > regiao > global). Somar ajustes entre EVENTOS DIFERENTES.
    # Apenas eventos e impactos ativos (ativo=true).
    con.execute(f"""
        CREATE OR REPLACE TABLE fat_eventos AS
        WITH eventos_ativos AS (
          SELECT evento_id, data_inicio, data_fim
          FROM read_parquet({pq('regras_priori/eventos/eventos.parquet')})
                  ),
        impactos_ativos AS (
          SELECT evento_id, escopo, escopo_id, ajuste_pct,
                 CASE escopo
                   WHEN 'unidade' THEN 1
                   WHEN 'predio' THEN 2
                   WHEN 'regiao' THEN 3
                   WHEN 'global' THEN 4
                   ELSE 99
                 END AS prio_escopo
          FROM read_parquet({pq('regras_priori/evento_impactos/evento_impactos.parquet')})
                  ),
        matches AS (
          SELECT
            ui.unidade_id,
            c.data,
            e.evento_id,
            i.ajuste_pct,
            ROW_NUMBER() OVER (
              PARTITION BY ui.unidade_id, c.data, e.evento_id
              ORDER BY i.prio_escopo
            ) AS rn
          FROM unit_info ui
          CROSS JOIN cal c
          JOIN eventos_ativos e ON c.data BETWEEN e.data_inicio AND e.data_fim
          JOIN impactos_ativos i ON i.evento_id = e.evento_id
            AND (
              (i.escopo = 'unidade' AND i.escopo_id = ui.unidade_id)
              OR (i.escopo = 'predio' AND i.escopo_id = ui.predio_id)
              OR (i.escopo = 'regiao' AND i.escopo_id = ui.regiao_id)
              OR i.escopo = 'global'
            )
        )
        SELECT
          DATE '{TODAY}' AS data_referencia,
          ui.unidade_id,
          c.data,
          COALESCE(SUM(m.ajuste_pct), 0.0)::DOUBLE AS ajuste_pct
        FROM unit_info ui
        CROSS JOIN cal c
        LEFT JOIN matches m
          ON m.unidade_id = ui.unidade_id AND m.data = c.data AND m.rn = 1
        GROUP BY ui.unidade_id, c.data
    """)
    print(f"fat_eventos           {count(con, 'fat_eventos'):>8} linhas")

    # ────────────────── 5) fat_antecedencia ──────────────────
    # lead = data - data_referencia (dias). Regras NÃO cumulativas (só uma faixa aplica).
    # Dentro da faixa: regra DOW-específica ganha da regra uniforme. Inativas ignoradas.
    con.execute(f"""
        CREATE OR REPLACE TABLE fat_antecedencia AS
        WITH regras AS (
          SELECT lead_min_dias, lead_max_dias, dia_semana, ajuste_pct,
                 CASE WHEN dia_semana IS NULL THEN 2 ELSE 1 END AS prio
          FROM read_parquet({pq('regras_priori/regras_antecedencia/regras_antecedencia.parquet')})
                  ),
        matched AS (
          SELECT
            ui.unidade_id,
            c.data,
            r.ajuste_pct,
            ROW_NUMBER() OVER (
              PARTITION BY ui.unidade_id, c.data
              ORDER BY r.prio
            ) AS rn
          FROM unit_info ui
          CROSS JOIN cal c
          JOIN regras r
            ON datediff('day', DATE '{TODAY}', c.data) >= r.lead_min_dias
            AND datediff('day', DATE '{TODAY}', c.data) <  r.lead_max_dias
            AND (r.dia_semana IS NULL OR r.dia_semana = (isodow(c.data) - 1))
        )
        SELECT
          DATE '{TODAY}' AS data_referencia,
          ui.unidade_id,
          c.data,
          COALESCE(m.ajuste_pct, 0.0)::DOUBLE AS ajuste_pct
        FROM unit_info ui
        CROSS JOIN cal c
        LEFT JOIN matched m
          ON m.unidade_id = ui.unidade_id AND m.data = c.data AND m.rn = 1
    """)
    print(f"fat_antecedencia      {count(con, 'fat_antecedencia'):>8} linhas")

    # ────────────────── 6) fat_ajuste_portfolio ──────────────────
    # Para cada (unidade, data): determina o bucket pela antecedência,
    # pega a ocupação real do portfólio (região) naquele dia e aplica a banda.
    # Regras globais (iguais pra todas as regiões), gap-free (bandas cobrem 0-100%).
    con.execute(f"""
        CREATE OR REPLACE TABLE fat_ajuste_portfolio AS
        WITH regras AS (
          SELECT janela_dias, ocupacao_min_pct, ocupacao_max_pct, ajuste_pct
          FROM read_parquet({pq('regras_posteriori/regras_ocupacao_portfolio/regras_ocupacao_portfolio.parquet')})
                  ),
        janelas AS (
          SELECT DISTINCT janela_dias FROM regras
        ),
        janela_max AS (SELECT MAX(janela_dias) AS max_j FROM janelas),
        -- Para cada dia, escolhe o bucket: menor janela_dias >= lead; senão a maior
        cal_bucket AS (
          SELECT c.data,
                 COALESCE(
                   (SELECT MIN(j.janela_dias) FROM janelas j
                    WHERE j.janela_dias >= datediff('day', DATE '{TODAY}', c.data)),
                   (SELECT max_j FROM janela_max)
                 ) AS bucket
          FROM cal c
        ),
        -- Ocupação real por portfólio (região) × dia, derivada das reserva_diarias
        total_por_regiao AS (
          SELECT regiao_id, COUNT(*) AS total FROM unit_info GROUP BY regiao_id
        ),
        ocupadas AS (
          SELECT ui.regiao_id, rd.data, COUNT(DISTINCT rd.unidade_id) AS ocupadas
          FROM read_parquet({pq('reservas/reserva_diarias/reserva_diarias.parquet')}) rd
          JOIN unit_info ui USING(unidade_id)
          GROUP BY ui.regiao_id, rd.data
        ),
        ocup AS (
          SELECT t.regiao_id, c.data,
                 COALESCE(o.ocupadas::DOUBLE / t.total, 0.0) AS ocupacao_pct
          FROM total_por_regiao t
          CROSS JOIN cal c
          LEFT JOIN ocupadas o ON o.regiao_id = t.regiao_id AND o.data = c.data
        ),
        aplicada AS (
          SELECT ui.unidade_id, cb.data, r.ajuste_pct
          FROM unit_info ui
          CROSS JOIN cal_bucket cb
          LEFT JOIN ocup o ON o.regiao_id = ui.regiao_id AND o.data = cb.data
          LEFT JOIN regras r
            ON r.janela_dias = cb.bucket
            AND o.ocupacao_pct >= r.ocupacao_min_pct
            AND o.ocupacao_pct <  r.ocupacao_max_pct
        )
        SELECT
          DATE '{TODAY}' AS data_referencia,
          unidade_id,
          data,
          COALESCE(ajuste_pct, 0.0)::DOUBLE AS ajuste_pct
        FROM aplicada
    """)
    print(f"fat_ajuste_portfolio  {count(con, 'fat_ajuste_portfolio'):>8} linhas")

    # ────────────────── 7) fat_ajuste_individual (placeholder: 0) ──────────────────
    # TODO: computar comparando ocupação futura da unidade com expectativa.
    con.execute(f"""
        CREATE OR REPLACE TABLE fat_ajuste_individual AS
        SELECT
          DATE '{TODAY}' AS data_referencia,
          ui.unidade_id,
          c.data,
          0.0::DOUBLE AS ajuste_pct
        FROM unit_info ui
        CROSS JOIN cal c
    """)
    print(f"fat_ajuste_individual {count(con, 'fat_ajuste_individual'):>8} linhas  (placeholder = 0)")

    # ────────────────── 8) pi — Preço Inicial ──────────────────
    # Pi = Pb × (1 + Saz + DiaSem + Ev + Ant)
    con.execute("""
        CREATE OR REPLACE TABLE pi AS
        SELECT
          pb.data_referencia,
          pb.unidade_id,
          pb.data,
          ROUND(
            pb.valor * (1
              + COALESCE(fs.ajuste_pct, 0)
              + COALESCE(fd.ajuste_pct, 0)
              + COALESCE(fe.ajuste_pct, 0)
              + COALESCE(fa.ajuste_pct, 0)
            ), 2
          ) AS valor
        FROM pb
        LEFT JOIN fat_sazonalidade fs USING(data_referencia, unidade_id, data)
        LEFT JOIN fat_dia_semana   fd USING(data_referencia, unidade_id, data)
        LEFT JOIN fat_eventos      fe USING(data_referencia, unidade_id, data)
        LEFT JOIN fat_antecedencia fa USING(data_referencia, unidade_id, data)
    """)
    print(f"pi                    {count(con, 'pi'):>8} linhas")

    # ────────────────── 9) d — Diária final (somatório) ──────────────────
    # D = Pi × (1 + AjustePort + AjusteInd)
    con.execute("""
        CREATE OR REPLACE TABLE d AS
        SELECT
          p.data_referencia,
          p.unidade_id,
          p.data,
          ROUND(
            p.valor * (1
              + COALESCE(fp.ajuste_pct, 0)
              + COALESCE(fi.ajuste_pct, 0)
            ), 2
          ) AS valor
        FROM pi p
        LEFT JOIN fat_ajuste_portfolio  fp USING(data_referencia, unidade_id, data)
        LEFT JOIN fat_ajuste_individual fi USING(data_referencia, unidade_id, data)
    """)
    print(f"d                     {count(con, 'd'):>8} linhas")

    # ────────────────── 10) ocupacao_portfolio (real) ──────────────────
    # Por região = unidades com reserva_diaria / total de unidades da região
    con.execute(f"""
        CREATE OR REPLACE TABLE ocupacao_portfolio AS
        WITH total AS (
          SELECT regiao_id, COUNT(*) AS total
          FROM unit_info GROUP BY regiao_id
        ),
        ocupadas AS (
          SELECT ui.regiao_id, rd.data, COUNT(DISTINCT rd.unidade_id) AS ocupadas
          FROM read_parquet({pq('reservas/reserva_diarias/reserva_diarias.parquet')}) rd
          JOIN unit_info ui USING(unidade_id)
          GROUP BY ui.regiao_id, rd.data
        )
        SELECT
          DATE '{TODAY}' AS data_referencia,
          t.regiao_id AS portfolio_id,
          c.data,
          ROUND(COALESCE(o.ocupadas::DOUBLE / t.total, 0.0), 4) AS ocupacao_pct
        FROM total t
        CROSS JOIN cal c
        LEFT JOIN ocupadas o ON o.regiao_id = t.regiao_id AND o.data = c.data
    """)
    print(f"ocupacao_portfolio    {count(con, 'ocupacao_portfolio'):>8} linhas")

    # ────────────────── 11) expectativa_portfolio ──────────────────
    # Agrega por região (média simples entre segmentos). Dias sem dado → 0.65 default.
    con.execute(f"""
        CREATE OR REPLACE TABLE expectativa_portfolio AS
        WITH regioes AS (SELECT DISTINCT regiao_id FROM unit_info),
             exp_agg AS (
               SELECT regiao_id, data, AVG(ocupacao_esperada_pct) AS pct
               FROM read_parquet({pq('regras_posteriori/expectativa_portfolio/expectativa_portfolio.parquet')})
               GROUP BY regiao_id, data
             )
        SELECT
          DATE '{TODAY}' AS data_referencia,
          r.regiao_id AS portfolio_id,
          c.data,
          ROUND(COALESCE(e.pct, 0.65), 4) AS ocupacao_esperada_pct
        FROM regioes r
        CROSS JOIN cal c
        LEFT JOIN exp_agg e ON e.regiao_id = r.regiao_id AND e.data = c.data
    """)
    print(f"expectativa_portfolio {count(con, 'expectativa_portfolio'):>8} linhas")

    # ────────────────── 12) simulador_meta ──────────────────
    con.execute("""
        CREATE OR REPLACE TABLE simulador_meta (
          data_referencia DATE PRIMARY KEY,
          gerado_em TIMESTAMP,
          versao VARCHAR,
          observacoes VARCHAR
        )
    """)
    obs = (
        "Base inicial (MVP do simulador). fat_ajuste_portfolio e fat_ajuste_individual "
        "preenchidos com 0 — a serem implementados em iterações futuras com a lógica "
        "de bucket/banda (AMB-02). Expectativa fora do horizonte dos dados reais usa "
        "fallback de 0.65."
    )
    con.execute(
        "INSERT INTO simulador_meta VALUES (?, now(), ?, ?)",
        [TODAY, VERSION, obs],
    )
    print(f"simulador_meta        {count(con, 'simulador_meta'):>8} linhas")

    # ────────────────── sanidade ──────────────────
    print("\n── sanidade ──")
    df = con.execute(f"""
        SELECT
          (SELECT ROUND(AVG(valor), 2)        FROM pb WHERE data = DATE '{TODAY}') AS pb_medio_d0,
          (SELECT ROUND(AVG(valor), 2)        FROM pi WHERE data = DATE '{TODAY}') AS pi_medio_d0,
          (SELECT ROUND(AVG(valor), 2)        FROM d  WHERE data = DATE '{TODAY}') AS d_medio_d0,
          (SELECT ROUND(AVG(ocupacao_pct)*100, 1)           FROM ocupacao_portfolio    WHERE data = DATE '{TODAY}') AS ocup_real_pct,
          (SELECT ROUND(AVG(ocupacao_esperada_pct)*100, 1)  FROM expectativa_portfolio WHERE data = DATE '{TODAY}') AS ocup_esperada_pct
    """).df()
    print(df.to_string(index=False))

    con.close()
    print(f"\nOK. DB criado em {DB_PATH}")


if __name__ == "__main__":
    main()
