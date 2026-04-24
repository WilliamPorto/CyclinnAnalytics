"""
Sobrescreve `ocupacao_portfolio` no simulador.duckdb com dados sintéticos
calibrados pra ficar próximos da `expectativa_portfolio` — útil pra testar
a UI do heatmap sem que tudo fique no vermelho.

Distribuição:
  • 85% das células → gap N(0, 3%), clipado em ±10%      (pequenos)
  • 15% das células → gap uniforme ±15% a ±40%            (outliers)

Depois, `ocupacao_pct = esperada + gap` clipado em [0, 1].

Execução:
  .venv/bin/python simulador/backend/fake_ocupacao_teste.py

Pra voltar aos dados reais (derivados de reserva_diarias):
  .venv/bin/python simulador/backend/build_simulator_db.py
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

SCRIPT_DIR = Path(__file__).resolve().parent
DB_PATH = SCRIPT_DIR / "simulador.duckdb"

SEED = 42
SHARE_OUTLIERS = 0.15
SMALL_STD = 0.03
SMALL_CLIP = 0.10
BIG_MIN, BIG_MAX = 0.15, 0.40


def main() -> None:
    if not DB_PATH.exists():
        raise SystemExit(
            f"{DB_PATH} não existe. Rode build_simulator_db.py primeiro."
        )

    con = duckdb.connect(str(DB_PATH))
    exp = con.execute(
        """
        SELECT data_referencia, portfolio_id, data, ocupacao_esperada_pct
        FROM expectativa_portfolio
        """
    ).df()

    if exp.empty:
        raise SystemExit("expectativa_portfolio vazia.")

    rng = np.random.default_rng(SEED)
    n = len(exp)

    # Pequeno gap: N(0, SMALL_STD), clipado em ±SMALL_CLIP
    gap_small = np.clip(rng.normal(0, SMALL_STD, n), -SMALL_CLIP, SMALL_CLIP)

    # Outliers: sinal aleatório * magnitude uniforme
    outlier_mask = rng.random(n) < SHARE_OUTLIERS
    outlier_sign = rng.choice([-1, 1], n)
    outlier_mag = rng.uniform(BIG_MIN, BIG_MAX, n)
    gap_big = outlier_sign * outlier_mag

    gap = np.where(outlier_mask, gap_big, gap_small)
    real = np.clip(exp["ocupacao_esperada_pct"].values + gap, 0.0, 1.0)

    # Monta dataframe final com o MESMO schema de ocupacao_portfolio
    df_real = pd.DataFrame({
        "data_referencia": exp["data_referencia"],
        "portfolio_id": exp["portfolio_id"],
        "data": exp["data"],
        "ocupacao_pct": np.round(real, 4),
    })

    con.register("df_real", df_real)
    con.execute("DELETE FROM ocupacao_portfolio")
    con.execute(
        """
        INSERT INTO ocupacao_portfolio
        SELECT data_referencia, portfolio_id, data, ocupacao_pct FROM df_real
        """
    )
    con.close()

    # Sumário
    abs_gap = np.abs(gap)
    n_small = int(abs_gap.__le__(SMALL_CLIP).sum())
    n_big = int(outlier_mask.sum())
    print(f"atualizadas {n} linhas em ocupacao_portfolio")
    print(f"  pequenos gaps (±{SMALL_CLIP*100:.0f}%):  {n_small:>5} ({n_small/n:.1%})")
    print(f"  outliers (±{BIG_MIN*100:.0f}–{BIG_MAX*100:.0f}%): {n_big:>5} ({n_big/n:.1%})")
    print(f"\n  ocupação real: min={real.min():.1%}  média={real.mean():.1%}  max={real.max():.1%}")
    print(f"  gap absoluto : média={abs_gap.mean():.2%}  p95={np.percentile(abs_gap,95):.2%}  max={abs_gap.max():.2%}")
    print(f"\nOK. Pra reverter pra dados reais: rode build_simulator_db.py")


if __name__ == "__main__":
    main()
