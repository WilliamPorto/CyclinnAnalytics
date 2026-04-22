# Cyclinn Pricing

Motor de precificação dinâmica para aluguel de apartamentos da Cyclinn — ambiente local de desenvolvimento com base sintética em Parquet, backend DuckDB + FastAPI e explorer SQL em Next.js.

## Estrutura

```
.
├── data/                        # Parquets (gerados por scripts/)
│   └── <schema>/<tabela>/<tabela>.parquet
├── scripts/
│   └── generate_sample_data.py  # Gera amostra sintética consistente
├── backend/                     # FastAPI + DuckDB
│   ├── main.py
│   └── requirements.txt
├── frontend/                    # Next.js + CodeMirror (editor SQL)
│   ├── app/
│   └── package.json
└── docs/
    └── analise_regras.md        # Análise do regras.pdf (ambiguidades, lacunas, perguntas)
```

## Schemas

Os 8 domínios do modelo (ver `docs/analise_regras.md` e o schema de dados):

| Schema | Tabelas |
|---|---|
| `cadastro` | brands, regioes, predios, segmentos, faixas_precificacao, canais, unidades |
| `preco_base` | precos_base |
| `regras_priori` | regras_sazonalidade, regras_dia_semana, eventos, evento_impactos, regras_antecedencia |
| `regras_posteriori` | regras_ocupacao_individual, regras_ocupacao_portfolio, expectativa_portfolio, ocupacao_externa |
| `reservas` | reservas, reserva_diarias |
| `calendario` | calendario_unidade, calendario_unidade_historico |
| `guardrails` | guardrails_unidade, overrides_preco |
| `auditoria` | log_recalculo, alteracao_relevante_eventos |

## Pré-requisitos

- Python 3.12+ com `venv` e `pip` (em Debian/Ubuntu: `sudo apt install python3.12-venv python3-pip`)
- Node.js 20+ e npm

## Setup

```bash
# Python
python3 -m venv .venv
.venv/bin/pip install -r backend/requirements.txt

# Node
npm install --prefix frontend
```

## Gerar a base de dados sintética

```bash
.venv/bin/python scripts/generate_sample_data.py
```

Isso cria ~13.000 linhas em ~24 parquets dentro de `data/`. É idempotente — pode rodar de novo para recriar tudo.

## Rodar

Em dois terminais:

```bash
# Terminal 1: backend
.venv/bin/uvicorn backend.main:app --reload --port 8000

# Terminal 2: frontend
npm run dev --prefix frontend
```

Abra [http://localhost:3000](http://localhost:3000).

O frontend faz proxy de `/api/*` para o backend (`http://localhost:8000`). Ajustável via `NEXT_PUBLIC_API_URL`.

## Como usar o explorer

- Sidebar lista schemas e tabelas. Clicar numa tabela **insere** `SELECT * FROM <schema>.<tabela> LIMIT 100;` no editor.
- Editor SQL tem syntax highlight (CodeMirror + dialeto PostgreSQL).
- **Ctrl+Enter** (ou Cmd+Enter) executa a query.
- Apenas `SELECT`/`WITH` são permitidos (backend bloqueia DDL/DML).
- Resultados acima de 5000 linhas são truncados.

## Queries de exemplo

```sql
-- Unidades por prédio com preço base médio
SELECT p.nome AS predio, COUNT(*) AS unidades, ROUND(AVG(pb.valor), 2) AS pb_medio
FROM cadastro.unidades u
JOIN cadastro.predios p USING(predio_id)
JOIN preco_base.precos_base pb
  ON pb.unidade_id = u.unidade_id AND pb.vigencia_fim IS NULL
GROUP BY p.nome ORDER BY pb_medio DESC;

-- Reservas futuras por canal
SELECT c.nome AS canal, COUNT(*) AS reservas, SUM(r.valor_total) AS gmv
FROM reservas.reservas r
JOIN cadastro.canais c USING(canal_id)
WHERE r.data_checkin >= CURRENT_DATE AND r.status = 'confirmada'
GROUP BY c.nome ORDER BY gmv DESC;

-- Diária média no calendário por prédio e mês
SELECT p.nome predio, DATE_TRUNC('month', c.data) AS mes,
       ROUND(AVG(c.diaria_final_clamped), 2) AS diaria_media
FROM calendario.calendario_unidade c
JOIN cadastro.unidades u USING(unidade_id)
JOIN cadastro.predios p USING(predio_id)
GROUP BY p.nome, mes ORDER BY p.nome, mes;
```

## Recarregar parquets sem reiniciar o backend

Depois de rodar `generate_sample_data.py` novamente:

```bash
curl -X POST http://localhost:8000/reload
```

## Próximos passos

1. Fechar ambiguidades do `docs/analise_regras.md` (blocos A–F das perguntas)
2. Implementar o **motor de cálculo** (substitui o placeholder de `AjustePortfolio`/`AjusteIndividual` em `calendario_unidade`)
3. Expor endpoint `/recalculate/<unidade_id>/<data>` no backend
4. Adicionar visualização de preço vs. tempo no frontend
