"""
Esvazia todos os parquets em data/<schema>/<tabela>/<tabela>.parquet,
preservando o schema (colunas e tipos) — deixa 0 linhas em cada tabela.

Uso:
  .venv/bin/python scripts/truncate_all.py

Para reverter:
  .venv/bin/python scripts/generate_sample_data.py
  # ou:
  git checkout HEAD -- data/
"""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parent.parent
DATA_ROOT = ROOT / "data"


def truncate(path: Path) -> int:
    """Sobrescreve o parquet com 0 linhas, preservando schema. Retorna rows antes."""
    before = pq.read_metadata(path).num_rows
    schema = pq.read_schema(path)
    empty = pa.Table.from_pylist([], schema=schema)
    pq.write_table(empty, path)
    return before


def main() -> None:
    if not DATA_ROOT.exists():
        raise SystemExit(f"DATA_ROOT não existe: {DATA_ROOT}")

    total_files = 0
    total_rows = 0
    for parquet in sorted(DATA_ROOT.rglob("*.parquet")):
        rel = parquet.relative_to(DATA_ROOT)
        rows = truncate(parquet)
        total_files += 1
        total_rows += rows
        print(f"  {str(rel):<60} {rows:>6} → 0")

    print(f"\nOK. {total_files} arquivos esvaziados ({total_rows} linhas removidas).")
    print("Lembre de recarregar as views: curl -X POST http://localhost:8000/reload")


if __name__ == "__main__":
    main()
