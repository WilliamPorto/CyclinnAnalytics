"""
Sobrescreve `ocupacao_regiao` no simulador.duckdb com dados sintéticos
calibrados pra ficar próximos da `expectativa_regiao`, e REPROCESSA em
cascata `fat_ajuste_regiao` e `d` — útil pra testar a UI do heatmap
sem que tudo fique no vermelho.

Distribuição da ocupação fake:
  • 85% das células → gap N(0, 3%), clipado em ±10%      (pequenos)
  • 15% das células → gap uniforme ±15% a ±40%            (outliers)

Depois, `ocupacao_pct = esperada + gap` clipado em [0, 1].

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
SHARE_OUTLIERS = 0.15
SMALL_STD = 0.03
SMALL_CLIP = 0.10
BIG_MIN, BIG_MAX = 0.15, 0.40


def pq(rel: str) -> str:
    return f"'{(DATA_ROOT / rel).as_posix()}'"


def main() -> None:
    if not DB_PATH.exists():
        raise SystemExit(
            f"{DB_PATH} não existe. Rode build_simulator_db.py primeiro."
        )

    con = duckdb.connect(str(DB_PATH))
    rng = np.random.default_rng(SEED)

    def synth(esperada: np.ndarray) -> np.ndarray:
        """Gera ocupação real sintética perto da esperada (gaps pequenos + outliers)."""
        m = len(esperada)
        gap_small = np.clip(rng.normal(0, SMALL_STD, m), -SMALL_CLIP, SMALL_CLIP)
        outlier_mask = rng.random(m) < SHARE_OUTLIERS
        outlier_sign = rng.choice([-1, 1], m)
        outlier_mag = rng.uniform(BIG_MIN, BIG_MAX, m)
        gap_big = outlier_sign * outlier_mag
        gap = np.where(outlier_mask, gap_big, gap_small)
        return np.clip(esperada + gap, 0.0, 1.0), gap

    # ── ocupacao_regiao ───────────────────────────────────────────
    exp = con.execute("""
        SELECT data_referencia, regiao_id, data, ocupacao_esperada_pct
        FROM expectativa_regiao
    """).df()
    if exp.empty:
        raise SystemExit("expectativa_regiao vazia.")

    real, gap = synth(exp["ocupacao_esperada_pct"].values)
    df_real = pd.DataFrame({
        "data_referencia": exp["data_referencia"],
        "regiao_id": exp["regiao_id"],
        "data": exp["data"],
        "ocupacao_pct": np.round(real, 4),
    })
    con.register("df_real", df_real)
    con.execute("DELETE FROM ocupacao_regiao")
    con.execute("""
        INSERT INTO ocupacao_regiao
        SELECT data_referencia, regiao_id, data, ocupacao_pct FROM df_real
    """)

    # ── ocupacao_predio (por prédio, mesmo modelo de ruído) ───────
    exp_p = con.execute("""
        SELECT data_referencia, predio_id, data, ocupacao_esperada_pct
        FROM expectativa_predio
    """).df()
    if not exp_p.empty:
        real_p, _ = synth(exp_p["ocupacao_esperada_pct"].values)
        df_real_p = pd.DataFrame({
            "data_referencia": exp_p["data_referencia"],
            "predio_id": exp_p["predio_id"],
            "data": exp_p["data"],
            "ocupacao_pct": np.round(real_p, 4),
        })
        con.register("df_real_p", df_real_p)
        con.execute("DELETE FROM ocupacao_predio")
        con.execute("""
            INSERT INTO ocupacao_predio
            SELECT data_referencia, predio_id, data, ocupacao_pct FROM df_real_p
        """)
    n = len(exp)

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

    # Sumário
    abs_gap = np.abs(gap)
    n_big = int((abs_gap > SMALL_CLIP).sum())
    n_small = n - n_big
    print(f"atualizadas {n} linhas em ocupacao_regiao + ocupacao_predio (+ cascade fat_ajuste_regiao, d)")
    print(f"  pequenos gaps (±{SMALL_CLIP*100:.0f}%):  {n_small:>5} ({n_small/n:.1%})")
    print(f"  outliers (±{BIG_MIN*100:.0f}–{BIG_MAX*100:.0f}%): {n_big:>5} ({n_big/n:.1%})")
    print(f"\n  ocupação real (reg.): min={real.min():.1%}  média={real.mean():.1%}  max={real.max():.1%}")
    print(f"  gap absoluto         : média={abs_gap.mean():.2%}  p95={np.percentile(abs_gap,95):.2%}  max={abs_gap.max():.2%}")
    print(f"\nOK. Pra reverter pra dados reais: rode build_simulator_db.py")


if __name__ == "__main__":
    main()
