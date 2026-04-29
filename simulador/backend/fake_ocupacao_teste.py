"""
Sobrescreve as 4 tabelas de ocupação no simulador.duckdb com dados sintéticos
calibrados pra exercitar a UI:

  • expectativa_regiao  (ocupação esperada por região)
  • ocupacao_regiao     (ocupação real por região)
  • expectativa_predio  (ocupação esperada por prédio)
  • ocupacao_predio     (ocupação real por prédio)

Modelo (mesmo pras 4 tabelas):

  1) Curva no tempo:
        pct(lead) = BASELINE + (PEAK - BASELINE) * exp(-lead / TAU_DAYS)
     • lead = 0      → ~PEAK (alta perto da data de referência)
     • lead = TAU    → ~56% do caminho até BASELINE
     • lead grande   → ~BASELINE (ocupação cai bastante)

  2) Variação por entidade (offset constante no tempo): ±REGIAO_JITTER /
     ±PREDIO_JITTER pra diferenciar regiões e prédios entre si.

  3) Ruído diário gaussiano (DAILY_NOISE) pra evitar curva perfeita.

  4) Real ≈ esperada na maioria (gap N(0, SMALL_GAP_STD) clipado em
     ±SMALL_GAP_CLIP). ~SHARE_OUTLIERS das células recebem um gap deliberado
     (±OUTLIER_MIN a OUTLIER_MAX) pra dar visibilidade ao heatmap de
     diferença real − esperada.

Em seguida, REPROCESSA em cascata `fat_ajuste_regiao` e `d` (preço final),
já que a ocupação por região alimenta a regra a posteriori.

Execução:
  .venv/bin/python simulador/backend/fake_ocupacao_teste.py

Pra voltar aos dados reais (derivados de reserva_diarias):
  .venv/bin/python simulador/backend/build_simulator_db.py
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent
DATA_ROOT = PROJECT_ROOT / "data"
DB_PATH = SCRIPT_DIR / "simulador.duckdb"

TODAY = date(2026, 4, 23)  # mantém em sincronia com build_simulator_db.py

SEED = 42

# Curva no tempo
PEAK = 0.90       # ocupação esperada perto da data de referência
BASELINE = 0.35   # ocupação esperada no horizonte distante
TAU_DAYS = 30.0   # constante de decaimento (em dias)

# Variação entre entidades (mesmo entity_id mantém o mesmo offset em todo período)
REGIAO_JITTER = 0.03
PREDIO_JITTER = 0.04

# Ruído gaussiano diário (aplicado a expectativa)
DAILY_NOISE = 0.02

# Gap esperada → real (na maioria pequeno; alguns outliers visíveis)
SMALL_GAP_STD = 0.02
SMALL_GAP_CLIP = 0.05
SHARE_OUTLIERS = 0.06
OUTLIER_MIN, OUTLIER_MAX = 0.15, 0.25


def pq(rel: str) -> str:
    return f"'{(DATA_ROOT / rel).as_posix()}'"


def lead_curve(lead_days: np.ndarray) -> np.ndarray:
    """Ocupação esperada em função do lead time (decaimento exponencial)."""
    return BASELINE + (PEAK - BASELINE) * np.exp(-lead_days / TAU_DAYS)


def main() -> None:
    if not DB_PATH.exists():
        raise SystemExit(
            f"{DB_PATH} não existe. Rode build_simulator_db.py primeiro."
        )

    con = duckdb.connect(str(DB_PATH))
    rng = np.random.default_rng(SEED)

    def gen_pair(entity_col: str, jitter: float, source_table: str) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
        """
        Gera (expectativa_df, real_df) sintéticas pro nível dado.
        Usa a tabela existente só pra extrair as combinações (entity_id, data).
        Retorna também um dict com estatísticas pro sumário.
        """
        df = con.execute(
            f"SELECT data_referencia, {entity_col}, data FROM {source_table}"
        ).df()
        if df.empty:
            return pd.DataFrame(), pd.DataFrame(), {}

        # Lead em dias (data_referencia → data)
        lead = (
            pd.to_datetime(df["data"]) - pd.to_datetime(df["data_referencia"])
        ).dt.days.values

        # Curva base + offset por entidade (consistente) + ruído diário
        base = lead_curve(lead)
        unique_ids = df[entity_col].unique()
        ent_offset_map = {
            int(eid): float(rng.uniform(-jitter, jitter)) for eid in unique_ids
        }
        ent_offset = df[entity_col].map(ent_offset_map).values.astype(float)
        daily_noise = rng.normal(0, DAILY_NOISE, len(df))
        expectativa = np.clip(base + ent_offset + daily_noise, 0.0, 1.0)

        # Real = expectativa + gap. Maioria small; SHARE_OUTLIERS recebem gap grande.
        small_gap = np.clip(
            rng.normal(0, SMALL_GAP_STD, len(df)),
            -SMALL_GAP_CLIP,
            SMALL_GAP_CLIP,
        )
        outlier_mask = rng.random(len(df)) < SHARE_OUTLIERS
        outlier_sign = rng.choice([-1, 1], len(df))
        outlier_mag = rng.uniform(OUTLIER_MIN, OUTLIER_MAX, len(df))
        outlier_gap = outlier_sign * outlier_mag
        gap = np.where(outlier_mask, outlier_gap, small_gap)
        real = np.clip(expectativa + gap, 0.0, 1.0)

        df_exp = pd.DataFrame({
            "data_referencia": df["data_referencia"],
            entity_col: df[entity_col],
            "data": df["data"],
            "ocupacao_esperada_pct": np.round(expectativa, 4),
        })
        df_real = pd.DataFrame({
            "data_referencia": df["data_referencia"],
            entity_col: df[entity_col],
            "data": df["data"],
            "ocupacao_pct": np.round(real, 4),
        })
        stats = {
            "n": len(df),
            "n_outliers": int(outlier_mask.sum()),
            "exp_min": float(expectativa.min()),
            "exp_mean": float(expectativa.mean()),
            "exp_max": float(expectativa.max()),
            "real_min": float(real.min()),
            "real_mean": float(real.mean()),
            "real_max": float(real.max()),
        }
        return df_exp, df_real, stats

    # ── Região ────────────────────────────────────────────────────
    df_exp_reg, df_real_reg, stats_reg = gen_pair(
        "regiao_id", REGIAO_JITTER, "expectativa_regiao"
    )
    if df_exp_reg.empty:
        raise SystemExit("expectativa_regiao vazia. Rode build_simulator_db.py.")

    con.register("df_exp_reg", df_exp_reg)
    con.register("df_real_reg", df_real_reg)
    con.execute("DELETE FROM expectativa_regiao")
    con.execute("""
        INSERT INTO expectativa_regiao
        SELECT data_referencia, regiao_id, data, ocupacao_esperada_pct
        FROM df_exp_reg
    """)
    con.execute("DELETE FROM ocupacao_regiao")
    con.execute("""
        INSERT INTO ocupacao_regiao
        SELECT data_referencia, regiao_id, data, ocupacao_pct
        FROM df_real_reg
    """)

    # ── Prédio ────────────────────────────────────────────────────
    df_exp_pred, df_real_pred, stats_pred = gen_pair(
        "predio_id", PREDIO_JITTER, "expectativa_predio"
    )
    if not df_exp_pred.empty:
        con.register("df_exp_pred", df_exp_pred)
        con.register("df_real_pred", df_real_pred)
        con.execute("DELETE FROM expectativa_predio")
        con.execute("""
            INSERT INTO expectativa_predio
            SELECT data_referencia, predio_id, data, ocupacao_esperada_pct
            FROM df_exp_pred
        """)
        con.execute("DELETE FROM ocupacao_predio")
        con.execute("""
            INSERT INTO ocupacao_predio
            SELECT data_referencia, predio_id, data, ocupacao_pct
            FROM df_real_pred
        """)

    # ────────── Cascata: recompõe fat_ajuste_regiao e d ──────────
    # unit_info é temp table criada em build_simulator_db; recriamos aqui
    # pra reprocessar sem depender daquele script estar "rodando".
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE unit_info AS
        SELECT u.unidade_id, u.predio_id, p.regiao_id, u.segmento_id
        FROM read_parquet({pq('cadastro/unidades/unidades.parquet')}) u
        JOIN read_parquet({pq('cadastro/predios/predios.parquet')}) p USING(predio_id)
    """)
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE cal AS
        SELECT (DATE '{TODAY}' + INTERVAL (n) DAY)::DATE AS data
        FROM range(0, 366) t(n)
    """)
    con.execute(f"""
        CREATE OR REPLACE TABLE fat_ajuste_regiao AS
        WITH regras AS (
          SELECT janela_dias, ocupacao_min_pct, ocupacao_max_pct, ajuste_pct
          FROM read_parquet({pq('regras_posteriori/regras_ocupacao_regiao/regras_ocupacao_regiao.parquet')})
        ),
        janelas AS (SELECT DISTINCT janela_dias FROM regras),
        janela_max AS (SELECT MAX(janela_dias) AS max_j FROM janelas),
        cal_bucket AS (
          SELECT c.data,
                 COALESCE(
                   (SELECT MIN(j.janela_dias) FROM janelas j
                    WHERE j.janela_dias >= datediff('day', DATE '{TODAY}', c.data)),
                   (SELECT max_j FROM janela_max)
                 ) AS bucket
          FROM cal c
        ),
        aplicada AS (
          SELECT ui.unidade_id, cb.data, r.ajuste_pct
          FROM unit_info ui
          CROSS JOIN cal_bucket cb
          LEFT JOIN ocupacao_regiao o
            ON o.regiao_id = ui.regiao_id AND o.data = cb.data
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
        LEFT JOIN fat_ajuste_regiao     fp USING(data_referencia, unidade_id, data)
        LEFT JOIN fat_ajuste_individual fi USING(data_referencia, unidade_id, data)
    """)
    con.close()

    # ── Sumário ───────────────────────────────────────────────────
    print(
        "atualizadas tabelas: expectativa_regiao, ocupacao_regiao, "
        "expectativa_predio, ocupacao_predio (+ cascade fat_ajuste_regiao, d)"
    )
    print(f"\ncurva: lead=0 → ~{PEAK:.0%}   |   lead→∞ → ~{BASELINE:.0%}   (tau={TAU_DAYS:.0f} dias)")
    print(f"outliers (gap visível): {SHARE_OUTLIERS:.0%} das células com ±{OUTLIER_MIN:.0%}–{OUTLIER_MAX:.0%}")
    if stats_reg:
        n = stats_reg["n"]
        out = stats_reg["n_outliers"]
        print(f"\n[região] {n} linhas, {out} outliers ({out/n:.1%})")
        print(f"  esperada: min={stats_reg['exp_min']:.1%}  média={stats_reg['exp_mean']:.1%}  max={stats_reg['exp_max']:.1%}")
        print(f"  real    : min={stats_reg['real_min']:.1%}  média={stats_reg['real_mean']:.1%}  max={stats_reg['real_max']:.1%}")
    if stats_pred:
        n = stats_pred["n"]
        out = stats_pred["n_outliers"]
        print(f"\n[prédio] {n} linhas, {out} outliers ({out/n:.1%})")
        print(f"  esperada: min={stats_pred['exp_min']:.1%}  média={stats_pred['exp_mean']:.1%}  max={stats_pred['exp_max']:.1%}")
        print(f"  real    : min={stats_pred['real_min']:.1%}  média={stats_pred['real_mean']:.1%}  max={stats_pred['real_max']:.1%}")
    print("\nOK. Pra reverter pra dados reais: rode build_simulator_db.py")


if __name__ == "__main__":
    main()
