"""
Gera amostra de dados sintéticos para o motor de pricing da Cyclinn.

Estrutura: data/<schema>/<tabela>/<tabela>.parquet

Os 8 schemas:
  cadastro          — brands, regioes, predios, segmentos, faixas, canais, unidades
  preco_base        — precos_base (versionado)
  regras_priori     — sazonalidade, dia_semana, eventos, evento_impactos, antecedencia
  regras_posteriori — ocupacao_individual, ocupacao_portfolio, expectativa, ocupacao_externa
  reservas          — reservas, reserva_diarias
  calendario        — calendario_unidade, calendario_unidade_historico
  guardrails        — guardrails_unidade, overrides_preco
  auditoria         — log_recalculo, alteracao_relevante_eventos

Execução:
  .venv/bin/python scripts/generate_sample_data.py
"""

from __future__ import annotations

import random
import uuid
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

SEED = 42
random.seed(SEED)

ROOT = Path(__file__).resolve().parent.parent
DATA_ROOT = ROOT / "data"

TODAY = date(2026, 4, 22)
HORIZON_DAYS = 180
END_DATE = TODAY + timedelta(days=HORIZON_DAYS)
HISTORY_START = TODAY - timedelta(days=90)
CALENDAR_START = HISTORY_START
CALENDAR_END = END_DATE


def write_parquet(schema: str, table: str, df: pd.DataFrame) -> None:
    path = DATA_ROOT / schema / table
    path.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path / f"{table}.parquet", index=False)
    print(f"  {schema:<18}/{table:<34} {len(df):>6} rows")


def daterange(start: date, end: date):
    d = start
    while d < end:
        yield d
        d += timedelta(days=1)


# ============================================================
# CADASTRO
# ============================================================

def gen_cadastro() -> dict:
    print("[cadastro]")

    brands = pd.DataFrame(
        [
            {"brand_id": 1, "nome": "Cyclinn", "ativo": True},
            {"brand_id": 2, "nome": "Stay.cy", "ativo": True},
        ]
    )
    write_parquet("cadastro", "brands", brands)

    regioes = pd.DataFrame(
        [
            {"regiao_id": 1, "nome": "SP - Vila Olímpia", "cidade": "São Paulo", "uf": "SP", "tipo_regiao": "urbano"},
            {"regiao_id": 2, "nome": "SP - Itaim Bibi", "cidade": "São Paulo", "uf": "SP", "tipo_regiao": "urbano"},
            {"regiao_id": 3, "nome": "Santo André - Centro", "cidade": "Santo André", "uf": "SP", "tipo_regiao": "urbano"},
            {"regiao_id": 4, "nome": "BH - Savassi", "cidade": "Belo Horizonte", "uf": "MG", "tipo_regiao": "urbano"},
            {"regiao_id": 5, "nome": "Cuiabá - Centro", "cidade": "Cuiabá", "uf": "MT", "tipo_regiao": "urbano"},
        ]
    )
    write_parquet("cadastro", "regioes", regioes)

    predios = pd.DataFrame(
        [
            {"predio_id": 1, "regiao_id": 1, "nome": "VO Design", "endereco": "Rua Gomes de Carvalho, 1500"},
            {"predio_id": 2, "regiao_id": 1, "nome": "VO Corporate", "endereco": "Av. Juscelino Kubitschek, 1830"},
            {"predio_id": 3, "regiao_id": 2, "nome": "Itaim Residence", "endereco": "Rua João Cachoeira, 200"},
            {"predio_id": 4, "regiao_id": 3, "nome": "SA Central", "endereco": "Av. Industrial, 500"},
            {"predio_id": 5, "regiao_id": 4, "nome": "Savassi Flats", "endereco": "Rua Pernambuco, 800"},
            {"predio_id": 6, "regiao_id": 4, "nome": "Savassi Premium", "endereco": "Av. do Contorno, 6500"},
            {"predio_id": 7, "regiao_id": 5, "nome": "Cuiabá Business", "endereco": "Av. Getúlio Vargas, 100"},
        ]
    )
    write_parquet("cadastro", "predios", predios)

    segmentos = pd.DataFrame(
        [
            {"segmento_id": 1, "nome": "studio"},
            {"segmento_id": 2, "nome": "flat"},
            {"segmento_id": 3, "nome": "1dorm"},
        ]
    )
    write_parquet("cadastro", "segmentos", segmentos)

    faixas = pd.DataFrame(
        [
            {"faixa_id": 1, "nome": "economy", "ordem": 1},
            {"faixa_id": 2, "nome": "standard", "ordem": 2},
            {"faixa_id": 3, "nome": "premium", "ordem": 3},
        ]
    )
    write_parquet("cadastro", "faixas_precificacao", faixas)

    canais = pd.DataFrame(
        [
            {"canal_id": 1, "nome": "airbnb", "comissao_pct": 0.03},
            {"canal_id": 2, "nome": "booking", "comissao_pct": 0.15},
            {"canal_id": 3, "nome": "direto", "comissao_pct": 0.00},
            {"canal_id": 4, "nome": "stay_cy", "comissao_pct": 0.00},
        ]
    )
    write_parquet("cadastro", "canais", canais)

    unidade_rows = []
    unidade_id = 1
    distribution = [(1, 3), (2, 3), (3, 3), (4, 2), (5, 3), (6, 3), (7, 3)]
    for predio_id, num_units in distribution:
        for _ in range(num_units):
            brand_id = 2 if random.random() < 0.2 else 1
            segmento_id = random.choice([1, 2, 3])
            faixa_id = random.choices([1, 2, 3], weights=[0.3, 0.5, 0.2])[0]
            unidade_rows.append(
                {
                    "unidade_id": unidade_id,
                    "predio_id": predio_id,
                    "brand_id": brand_id,
                    "segmento_id": segmento_id,
                    "faixa_id": faixa_id,
                    "codigo_externo": f"CY-{unidade_id:04d}",
                    "capacidade": random.choice([2, 2, 3, 4]),
                    "ativo": True,
                }
            )
            unidade_id += 1
    unidades = pd.DataFrame(unidade_rows)
    write_parquet("cadastro", "unidades", unidades)

    return {
        "brands": brands,
        "regioes": regioes,
        "predios": predios,
        "segmentos": segmentos,
        "faixas": faixas,
        "canais": canais,
        "unidades": unidades,
    }


# ============================================================
# PRECO_BASE
# ============================================================

def gen_preco_base(cad: dict) -> pd.DataFrame:
    print("[preco_base]")
    unidades = cad["unidades"]

    base_by_faixa = {1: 220.0, 2: 360.0, 3: 580.0}

    rows = []
    preco_base_id = 1
    for _, u in unidades.iterrows():
        valor_atual = base_by_faixa[u["faixa_id"]] * random.uniform(0.9, 1.1)
        tem_historico = random.random() < 0.3
        if tem_historico:
            valor_antigo = valor_atual * random.uniform(0.85, 0.95)
            rows.append(
                {
                    "preco_base_id": preco_base_id,
                    "unidade_id": int(u["unidade_id"]),
                    "valor": round(valor_antigo, 2),
                    "vigencia_inicio": date(2025, 1, 1),
                    "vigencia_fim": date(2025, 12, 31),
                    "motivo": "reajuste anual",
                }
            )
            preco_base_id += 1
        rows.append(
            {
                "preco_base_id": preco_base_id,
                "unidade_id": int(u["unidade_id"]),
                "valor": round(valor_atual, 2),
                "vigencia_inicio": date(2026, 1, 1) if tem_historico else date(2025, 1, 1),
                "vigencia_fim": None,
                "motivo": "reajuste anual" if tem_historico else "cadastro inicial",
            }
        )
        preco_base_id += 1

    df = pd.DataFrame(rows)
    df["vigencia_inicio"] = pd.to_datetime(df["vigencia_inicio"]).dt.date
    df["vigencia_fim"] = pd.to_datetime(df["vigencia_fim"]).dt.date
    write_parquet("preco_base", "precos_base", df)
    return df


def pb_atual(df_pb: pd.DataFrame) -> dict:
    """Retorna {unidade_id: preço_base_vigente}"""
    atuais = df_pb[df_pb["vigencia_fim"].isna()]
    return dict(zip(atuais["unidade_id"], atuais["valor"]))


# ============================================================
# REGRAS_PRIORI
# ============================================================

def gen_regras_priori(cad: dict) -> dict:
    print("[regras_priori]")

    # --- Sazonalidade ---
    saz_rows = []
    regra_id = 1
    saz_rows.append(
        {
            "regra_id": regra_id, "escopo": "global", "escopo_id": None,
            "nome": "Verão 2026", "data_inicio": date(2026, 12, 15), "data_fim": date(2027, 2, 28),
            "ajuste_pct": 0.25, "recorrente_anual": True, "prioridade": 10,
        }
    )
    regra_id += 1
    saz_rows.append(
        {
            "regra_id": regra_id, "escopo": "global", "escopo_id": None,
            "nome": "Férias Julho", "data_inicio": date(2026, 7, 1), "data_fim": date(2026, 7, 31),
            "ajuste_pct": 0.15, "recorrente_anual": True, "prioridade": 10,
        }
    )
    regra_id += 1
    saz_rows.append(
        {
            "regra_id": regra_id, "escopo": "regiao", "escopo_id": 5,
            "nome": "Seca Pantanal (alta Cuiabá)", "data_inicio": date(2026, 5, 1), "data_fim": date(2026, 9, 30),
            "ajuste_pct": 0.20, "recorrente_anual": True, "prioridade": 20,
        }
    )
    regra_id += 1
    saz_rows.append(
        {
            "regra_id": regra_id, "escopo": "global", "escopo_id": None,
            "nome": "Baixa Pós-Carnaval", "data_inicio": date(2026, 2, 20), "data_fim": date(2026, 3, 15),
            "ajuste_pct": -0.10, "recorrente_anual": True, "prioridade": 10,
        }
    )
    regra_id += 1
    saz_rows.append(
        {
            "regra_id": regra_id, "escopo": "global", "escopo_id": None,
            "nome": "Virada de Ano", "data_inicio": date(2026, 12, 28), "data_fim": date(2027, 1, 3),
            "ajuste_pct": 0.40, "recorrente_anual": True, "prioridade": 30,
        }
    )
    regra_id += 1
    saz = pd.DataFrame(saz_rows)
    saz["data_inicio"] = pd.to_datetime(saz["data_inicio"]).dt.date
    saz["data_fim"] = pd.to_datetime(saz["data_fim"]).dt.date
    write_parquet("regras_priori", "regras_sazonalidade", saz)

    # --- Dia da semana ---
    dow_rows = []
    regra_id = 1
    # Urbano (regiões 1,2,3,4,5): meio de semana mais caro (business)
    urbano_adjustments = {0: -0.05, 1: 0.08, 2: 0.10, 3: 0.10, 4: 0.08, 5: -0.05, 6: -0.10}
    for regiao_id in [1, 2, 3, 4, 5]:
        for dow, adj in urbano_adjustments.items():
            dow_rows.append(
                {
                    "regra_id": regra_id, "escopo": "regiao", "escopo_id": regiao_id,
                    "dia_semana": dow, "ajuste_pct": adj,
                }
            )
            regra_id += 1
    dow_df = pd.DataFrame(dow_rows)
    write_parquet("regras_priori", "regras_dia_semana", dow_df)

    # --- Eventos ---
    eventos_rows = [
        {"evento_id": 1, "nome": "F1 GP São Paulo 2026", "data_inicio": date(2026, 11, 6), "data_fim": date(2026, 11, 8), "categoria": "esportivo"},
        {"evento_id": 2, "nome": "The Town 2026", "data_inicio": date(2026, 9, 3), "data_fim": date(2026, 9, 12), "categoria": "show"},
        {"evento_id": 3, "nome": "Lollapalooza 2026", "data_inicio": date(2026, 3, 27), "data_fim": date(2026, 3, 29), "categoria": "show"},
        {"evento_id": 4, "nome": "Carnaval 2026", "data_inicio": date(2026, 2, 14), "data_fim": date(2026, 2, 18), "categoria": "feriado"},
        {"evento_id": 5, "nome": "Tiradentes", "data_inicio": date(2026, 4, 21), "data_fim": date(2026, 4, 21), "categoria": "feriado"},
        {"evento_id": 6, "nome": "Corpus Christi", "data_inicio": date(2026, 6, 4), "data_fim": date(2026, 6, 4), "categoria": "feriado"},
        {"evento_id": 7, "nome": "Independência", "data_inicio": date(2026, 9, 7), "data_fim": date(2026, 9, 7), "categoria": "feriado"},
        {"evento_id": 8, "nome": "N. Sra. Aparecida", "data_inicio": date(2026, 10, 12), "data_fim": date(2026, 10, 12), "categoria": "feriado"},
        {"evento_id": 9, "nome": "Finados", "data_inicio": date(2026, 11, 2), "data_fim": date(2026, 11, 2), "categoria": "feriado"},
        {"evento_id": 10, "nome": "Agrishow Cuiabá", "data_inicio": date(2026, 5, 25), "data_fim": date(2026, 5, 29), "categoria": "convencao"},
    ]
    eventos = pd.DataFrame(eventos_rows)
    eventos["data_inicio"] = pd.to_datetime(eventos["data_inicio"]).dt.date
    eventos["data_fim"] = pd.to_datetime(eventos["data_fim"]).dt.date
    write_parquet("regras_priori", "eventos", eventos)

    # --- Evento impactos ---
    impactos_rows = []
    impacto_id = 1
    impact_spec = [
        (1, "regiao", 1, 0.80), (1, "regiao", 2, 0.70), (1, "regiao", 3, 0.35),
        (2, "regiao", 1, 0.55), (2, "regiao", 2, 0.50),
        (3, "regiao", 1, 0.45), (3, "regiao", 2, 0.40),
        (4, "regiao", 1, -0.05), (4, "regiao", 4, -0.05), (4, "regiao", 5, -0.05),
        (5, "regiao", 1, 0.05), (5, "regiao", 4, 0.05),
        (6, "regiao", 1, 0.05), (6, "regiao", 4, 0.05),
        (7, "regiao", 1, 0.05), (7, "regiao", 4, 0.05),
        (8, "regiao", 1, 0.05), (8, "regiao", 4, 0.05),
        (9, "regiao", 1, 0.05),
        (10, "regiao", 5, 0.60),
    ]
    for ev_id, escopo, escopo_id, ajuste in impact_spec:
        impactos_rows.append(
            {
                "impacto_id": impacto_id,
                "evento_id": ev_id,
                "escopo": escopo,
                "escopo_id": escopo_id,
                "ajuste_pct": ajuste,
            }
        )
        impacto_id += 1
    impactos = pd.DataFrame(impactos_rows)
    write_parquet("regras_priori", "evento_impactos", impactos)

    # --- Antecedência ---
    ant_rows = []
    regra_id = 1
    # Faixas "todos os dias"
    for lead_min, lead_max, adj in [(180, 365, 0.35), (90, 180, 0.20), (30, 90, 0.05)]:
        ant_rows.append(
            {
                "regra_id": regra_id, "escopo": "global", "escopo_id": None,
                "lead_min_dias": lead_min, "lead_max_dias": lead_max,
                "dia_semana": None, "ajuste_pct": adj,
            }
        )
        regra_id += 1
    # Faixa 0-15 por dia da semana
    dow_ant = {0: 0.05, 1: 0.10, 2: 0.14, 3: 0.10, 4: 0.0, 5: 0.0, 6: 0.0}
    for dow, adj in dow_ant.items():
        ant_rows.append(
            {
                "regra_id": regra_id, "escopo": "global", "escopo_id": None,
                "lead_min_dias": 0, "lead_max_dias": 15,
                "dia_semana": dow, "ajuste_pct": adj,
            }
        )
        regra_id += 1
    ant_df = pd.DataFrame(ant_rows)
    write_parquet("regras_priori", "regras_antecedencia", ant_df)

    return {"sazonalidade": saz, "dia_semana": dow_df, "eventos": eventos, "impactos": impactos, "antecedencia": ant_df}


# ============================================================
# REGRAS_POSTERIORI
# ============================================================

def gen_regras_posteriori(cad: dict) -> dict:
    print("[regras_posteriori]")

    # --- Ocupacao individual (tabela do PDF) ---
    ind_spec = [
        (21, 0.30, None, 0.10),
        (14, 0.40, None, 0.10),
        (7, None, 0.30, -0.10),
        (7, None, 0.40, -0.05),
        (7, 0.50, None, 0.10),
        (5, None, 0.40, -0.10),
        (5, None, 0.50, -0.05),
        (5, 0.60, None, 0.10),
        (3, None, 0.50, -0.10),
        (3, None, 0.60, -0.05),
        (3, 0.70, None, 0.10),
        (1, None, 0.65, -0.10),
        (1, None, 0.75, -0.05),
        (1, 0.85, None, 0.10),
        (0, None, 0.70, -0.10),
        (0, None, 0.80, -0.05),
        (0, 0.90, None, 0.10),
    ]
    # Nota: no PDF alguns thresholds são "<X → reduz", outros "≥X → aumenta". Mapeamos:
    # ajuste_pct > 0 ⇒ aplicar quando ocup >= ocupacao_min_pct
    # ajuste_pct < 0 ⇒ aplicar quando ocup <  ocupacao_max_pct
    ind_rows = []
    regra_id = 1
    for janela, ocup_min, ocup_max, adj in ind_spec:
        ind_rows.append(
            {
                "regra_id": regra_id,
                "escopo": "global",
                "escopo_id": None,
                "janela_dias": janela,
                "ocupacao_min_pct": ocup_min,
                "ocupacao_max_pct": ocup_max,
                "ajuste_pct": adj,
                "cumulativo": True,  # AMB-02 — assumido, a confirmar com cliente
            }
        )
        regra_id += 1
    ind_df = pd.DataFrame(ind_rows)
    write_parquet("regras_posteriori", "regras_ocupacao_individual", ind_df)

    # --- Ocupacao portfolio (AMB-05 — não definido no PDF, amostra plausível) ---
    port_spec = [
        (14, None, 0.40, -0.08),
        (14, 0.60, None, 0.08),
        (7, None, 0.45, -0.10),
        (7, 0.65, None, 0.10),
        (3, None, 0.55, -0.10),
        (3, 0.75, None, 0.10),
    ]
    port_rows = []
    regra_id = 1
    for janela, ocup_min, ocup_max, adj in port_spec:
        for regiao_id in [1, 2, 3, 4, 5]:
            port_rows.append(
                {
                    "regra_id": regra_id,
                    "escopo": "regiao",
                    "escopo_id": regiao_id,
                    "janela_dias": janela,
                    "ocupacao_min_pct": ocup_min,
                    "ocupacao_max_pct": ocup_max,
                    "ajuste_pct": adj,
                    "cumulativo": True,
                }
            )
            regra_id += 1
    port_df = pd.DataFrame(port_rows)
    write_parquet("regras_posteriori", "regras_ocupacao_portfolio", port_df)

    # --- Expectativa portfolio (uma linha por regiao x segmento x dia) ---
    exp_rows = []
    exp_id = 1
    for regiao_id in [1, 2, 3, 4, 5]:
        for segmento_id in [1, 2, 3]:
            for d in daterange(CALENDAR_START, CALENDAR_END):
                dow = d.weekday()
                # Baseline por segmento
                base = {1: 0.62, 2: 0.65, 3: 0.68}[segmento_id]
                # Modulação por dow: meio-semana urbano maior
                dow_mod = {0: -0.05, 1: 0.03, 2: 0.05, 3: 0.05, 4: 0.03, 5: -0.02, 6: -0.08}[dow]
                # Modulação sazonal simples
                month = d.month
                saz_mod = 0.05 if month in (7, 12) else (-0.05 if month == 3 else 0.0)
                esperada = max(0.3, min(0.92, base + dow_mod + saz_mod))
                exp_rows.append(
                    {
                        "expectativa_id": exp_id,
                        "regiao_id": regiao_id,
                        "segmento_id": segmento_id,
                        "data": d,
                        "ocupacao_esperada_pct": round(esperada, 4),
                        "fonte": "historico_interno",
                    }
                )
                exp_id += 1
    exp_df = pd.DataFrame(exp_rows)
    exp_df["data"] = pd.to_datetime(exp_df["data"]).dt.date
    write_parquet("regras_posteriori", "expectativa_portfolio", exp_df)

    # --- Ocupacao externa (benchmark de mercado) ---
    oc_ext_rows = []
    for regiao_id in [1, 2, 3, 4, 5]:
        for d in daterange(CALENDAR_START, CALENDAR_END):
            dow = d.weekday()
            base = 0.58 + random.uniform(-0.05, 0.05)
            dow_mod = {0: -0.05, 1: 0.02, 2: 0.05, 3: 0.05, 4: 0.02, 5: -0.05, 6: -0.10}[dow]
            ocup = max(0.25, min(0.95, base + dow_mod))
            adr = 300 + (regiao_id * 25) + random.uniform(-30, 30)
            oc_ext_rows.append(
                {
                    "regiao_id": regiao_id,
                    "data": d,
                    "ocupacao_mercado_pct": round(ocup, 4),
                    "adr_mercado": round(adr, 2),
                }
            )
    oc_ext_df = pd.DataFrame(oc_ext_rows)
    oc_ext_df["data"] = pd.to_datetime(oc_ext_df["data"]).dt.date
    write_parquet("regras_posteriori", "ocupacao_externa", oc_ext_df)

    return {"ocup_ind": ind_df, "ocup_port": port_df, "expectativa": exp_df, "ocup_ext": oc_ext_df}


# ============================================================
# RESERVAS
# ============================================================

def gen_reservas(cad: dict, pb_map: dict) -> dict:
    print("[reservas]")

    unidades = cad["unidades"]
    unidade_ids = unidades["unidade_id"].tolist()
    canal_ids = [1, 2, 3, 4]
    canal_weights = [0.45, 0.25, 0.25, 0.05]

    hospedes_pool = [
        "Ana Silva", "Bruno Costa", "Carlos Oliveira", "Daniela Souza", "Eduardo Lima",
        "Fernanda Martins", "Gustavo Rocha", "Helena Dias", "Igor Pereira", "Julia Alves",
        "Kaio Ribeiro", "Larissa Freitas", "Marcos Gomes", "Natália Castro", "Otávio Mendes",
        "Paula Barbosa", "Ricardo Teixeira", "Sofia Correia", "Thiago Araújo", "Vitória Melo",
    ]

    reservas = []
    reserva_diarias = []
    reserva_id = 1

    # Gerar reservas cobrindo o intervalo [HISTORY_START, END_DATE)
    total_reservas_target = 200

    for _ in range(total_reservas_target):
        unidade_id = random.choice(unidade_ids)
        canal_id = random.choices(canal_ids, weights=canal_weights)[0]

        # Distribuir checkins: parte passada (histórica), parte futura
        if random.random() < 0.55:
            checkin = HISTORY_START + timedelta(days=random.randint(0, (TODAY - HISTORY_START).days - 1))
        else:
            checkin = TODAY + timedelta(days=random.randint(0, HORIZON_DAYS - 3))

        # Length of stay: a maioria curta, algumas longas (Cyclinn faz long stay)
        r = random.random()
        if r < 0.6:
            noites = random.randint(1, 4)
        elif r < 0.9:
            noites = random.randint(5, 14)
        else:
            noites = random.randint(15, 60)

        checkout = checkin + timedelta(days=noites)
        if checkout > END_DATE:
            checkout = END_DATE
            noites = (checkout - checkin).days
            if noites <= 0:
                continue

        # criada_em: entre 1 e 180 dias antes do checkin
        antecedencia_dias = random.randint(1, 180)
        criada_em = datetime.combine(checkin - timedelta(days=antecedencia_dias), datetime.min.time(), tzinfo=timezone.utc)
        # Adiciona um horário aleatório
        criada_em += timedelta(hours=random.randint(6, 22), minutes=random.randint(0, 59))

        status = "confirmada"
        if random.random() < 0.07:
            status = "cancelada"
        elif random.random() < 0.02:
            status = "no_show"

        pb = pb_map.get(unidade_id, 300.0)
        # Preço efetivo com alguma variação (simula o que o motor aplicou no passado)
        diaria_media = pb * random.uniform(0.85, 1.45)
        valor_total = round(diaria_media * noites, 2)

        reservas.append(
            {
                "reserva_id": reserva_id,
                "unidade_id": int(unidade_id),
                "canal_id": canal_id,
                "hospede_nome": random.choice(hospedes_pool),
                "data_checkin": checkin,
                "data_checkout": checkout,
                "criada_em": criada_em,
                "status": status,
                "valor_total": valor_total,
                "noites": noites,
            }
        )

        if status == "confirmada":
            for i in range(noites):
                d = checkin + timedelta(days=i)
                # Variação leve da diária por noite
                rd_valor = round(diaria_media * random.uniform(0.95, 1.05), 2)
                reserva_diarias.append(
                    {
                        "unidade_id": int(unidade_id),
                        "data": d,
                        "reserva_id": reserva_id,
                        "valor_diaria": rd_valor,
                    }
                )

        reserva_id += 1

    # Dedup reserva_diarias: se duas reservas colidem na mesma unidade/data (data race sintético), manter a primeira
    rd_df = pd.DataFrame(reserva_diarias)
    rd_df = rd_df.drop_duplicates(subset=["unidade_id", "data"], keep="first")

    rsv_df = pd.DataFrame(reservas)
    rsv_df["data_checkin"] = pd.to_datetime(rsv_df["data_checkin"]).dt.date
    rsv_df["data_checkout"] = pd.to_datetime(rsv_df["data_checkout"]).dt.date
    rd_df["data"] = pd.to_datetime(rd_df["data"]).dt.date

    write_parquet("reservas", "reservas", rsv_df)
    write_parquet("reservas", "reserva_diarias", rd_df)

    return {"reservas": rsv_df, "reserva_diarias": rd_df}


# ============================================================
# CALENDARIO
# ============================================================

def gen_calendario(cad: dict, pb_map: dict, reservas_state: dict) -> dict:
    print("[calendario]")

    unidades = cad["unidades"]
    rd_df = reservas_state["reserva_diarias"]
    # Set para lookup rápido de ocupação
    ocupadas = set(zip(rd_df["unidade_id"], rd_df["data"]))

    cal_rows = []
    for _, u in unidades.iterrows():
        unidade_id = int(u["unidade_id"])
        pb = pb_map.get(unidade_id, 300.0)
        for d in daterange(CALENDAR_START, CALENDAR_END):
            lead = (d - TODAY).days
            dow = d.weekday()

            # Aproximação simples dos fatores (o motor real calcularia a partir das regras)
            saz = 0.0
            if d.month == 12 and d.day >= 15:
                saz = 0.25
            elif d.month == 1 or d.month == 2 and d.day <= 28:
                saz = 0.25 if d.month == 1 else 0.15
            elif d.month == 7:
                saz = 0.15

            dia_sem = {0: -0.05, 1: 0.08, 2: 0.10, 3: 0.10, 4: 0.08, 5: -0.05, 6: -0.10}[dow]

            ev = 0.0
            if d == date(2026, 11, 7) or d == date(2026, 11, 8):
                ev = 0.80
            elif d in [date(2026, 2, 14), date(2026, 2, 15), date(2026, 2, 16), date(2026, 2, 17)]:
                ev = -0.05  # Carnaval é ruim em SP/BH/Cuiabá (urbano)

            # Antecedencia
            if lead < 0:
                ant = 0.0  # histórico
            elif lead >= 180:
                ant = 0.35
            elif lead >= 90:
                ant = 0.20
            elif lead >= 30:
                ant = 0.05
            elif lead >= 15:
                ant = 0.0
            else:
                ant = {0: 0.05, 1: 0.10, 2: 0.14, 3: 0.10, 4: 0.0, 5: 0.0, 6: 0.0}[dow]

            pi = pb * (1 + saz + dia_sem + ev + ant)

            # Ajustes posteriori (placeholder — o motor real calcularia)
            ajuste_port = random.uniform(-0.05, 0.05)
            ajuste_ind = random.uniform(-0.05, 0.05)
            # Se a data já está ocupada, não recalcula (congelada)
            if (unidade_id, d) in ocupadas:
                ajuste_port = 0.0
                ajuste_ind = 0.0

            diaria = pi * (1 + ajuste_port + ajuste_ind)
            diaria_clamped = max(pb * 0.6, min(pb * 3.0, diaria))

            cal_rows.append(
                {
                    "unidade_id": unidade_id,
                    "data": d,
                    "pb": round(pb, 2),
                    "saz_pct": round(saz, 4),
                    "dia_sem_pct": round(dia_sem, 4),
                    "ev_pct": round(ev, 4),
                    "ant_pct": round(ant, 4),
                    "pi": round(pi, 2),
                    "ajuste_portfolio_pct": round(ajuste_port, 4),
                    "ajuste_individual_pct": round(ajuste_ind, 4),
                    "diaria_final": round(diaria, 2),
                    "diaria_final_clamped": round(diaria_clamped, 2),
                    "calculado_em": datetime.now(timezone.utc),
                    "versao": 1,
                }
            )

    cal_df = pd.DataFrame(cal_rows)
    cal_df["data"] = pd.to_datetime(cal_df["data"]).dt.date
    write_parquet("calendario", "calendario_unidade", cal_df)

    # Histórico — amostra (10% com versões anteriores)
    hist_rows = []
    sample = cal_df.sample(frac=0.1, random_state=SEED).copy()
    sample["diaria_final"] = sample["diaria_final"] * random.uniform(0.9, 1.1)
    sample["diaria_final_clamped"] = sample["diaria_final"]
    sample["versao"] = 0
    sample["calculado_em"] = sample["calculado_em"] - pd.Timedelta(days=7)
    sample["valido_ate"] = pd.Timestamp.now(tz="UTC") - pd.Timedelta(days=7)
    write_parquet("calendario", "calendario_unidade_historico", sample)

    return {"calendario": cal_df}


# ============================================================
# GUARDRAILS
# ============================================================

def gen_guardrails(cad: dict, pb_map: dict) -> dict:
    print("[guardrails]")

    rows = []
    for _, u in cad["unidades"].iterrows():
        unidade_id = int(u["unidade_id"])
        pb = pb_map.get(unidade_id, 300.0)
        rows.append(
            {
                "unidade_id": unidade_id,
                "preco_min": round(pb * 0.60, 2),
                "preco_max": round(pb * 3.00, 2),
                "delta_max_pct": 0.15,
                "delta_max_abs": round(pb * 0.50, 2),
            }
        )
    gr_df = pd.DataFrame(rows)
    write_parquet("guardrails", "guardrails_unidade", gr_df)

    # Overrides de exemplo
    unidade_ids = cad["unidades"]["unidade_id"].tolist()
    overrides = []
    for i, u_id in enumerate(random.sample(unidade_ids, 5), start=1):
        pb = pb_map.get(int(u_id), 300.0)
        di = TODAY + timedelta(days=random.randint(30, 60))
        df_end = di + timedelta(days=random.randint(2, 7))
        usa_preco_fixo = random.random() < 0.5
        overrides.append(
            {
                "override_id": i,
                "unidade_id": int(u_id),
                "data_inicio": di,
                "data_fim": df_end,
                "preco_fixo": round(pb * 1.8, 2) if usa_preco_fixo else None,
                "ajuste_pct_forcado": None if usa_preco_fixo else 0.50,
                "motivo": "trava por evento privado" if usa_preco_fixo else "promo direcionada",
                "criado_por": "rm@cyclinn.com.br",
                "ativo": True,
            }
        )
    ov_df = pd.DataFrame(overrides)
    ov_df["data_inicio"] = pd.to_datetime(ov_df["data_inicio"]).dt.date
    ov_df["data_fim"] = pd.to_datetime(ov_df["data_fim"]).dt.date
    write_parquet("guardrails", "overrides_preco", ov_df)

    return {"guardrails": gr_df, "overrides": ov_df}


# ============================================================
# AUDITORIA
# ============================================================

def gen_auditoria(cad: dict) -> dict:
    print("[auditoria]")

    unidade_ids = cad["unidades"]["unidade_id"].tolist()

    logs = []
    for _ in range(100):
        rodada = str(uuid.uuid4())
        u_id = random.choice(unidade_ids)
        d = HISTORY_START + timedelta(days=random.randint(0, (CALENDAR_END - HISTORY_START).days - 1))
        logs.append(
            {
                "log_id": len(logs) + 1,
                "rodada_id": rodada,
                "unidade_id": int(u_id),
                "data": d,
                "trigger": random.choice(["cron_diario", "nova_reserva", "evento_novo", "manual"]),
                "duracao_ms": random.randint(5, 180),
                "erro": None,
            }
        )
    log_df = pd.DataFrame(logs)
    log_df["data"] = pd.to_datetime(log_df["data"]).dt.date
    write_parquet("auditoria", "log_recalculo", log_df)

    eventos = []
    for i in range(50):
        u_id = random.choice(unidade_ids)
        d = TODAY + timedelta(days=random.randint(0, HORIZON_DAYS - 1))
        periodo = random.choice([21, 14, 7, 5, 3, 1, 0])
        delta = random.uniform(-0.20, 0.20)
        limite = 0.10
        eventos.append(
            {
                "unidade_id": int(u_id),
                "data": d,
                "periodo_analisado_dias": periodo,
                "delta_ocupacao_pct": round(delta, 4),
                "limite_configurado": limite,
                "foi_relevante": abs(delta) > limite,
            }
        )
    ev_df = pd.DataFrame(eventos)
    ev_df["data"] = pd.to_datetime(ev_df["data"]).dt.date
    write_parquet("auditoria", "alteracao_relevante_eventos", ev_df)


# ============================================================
# MAIN
# ============================================================

def main():
    print(f"Target directory: {DATA_ROOT}")
    DATA_ROOT.mkdir(parents=True, exist_ok=True)

    cad = gen_cadastro()
    pb_df = gen_preco_base(cad)
    pb_map = pb_atual(pb_df)
    _ = gen_regras_priori(cad)
    _ = gen_regras_posteriori(cad)
    rsv = gen_reservas(cad, pb_map)
    _ = gen_calendario(cad, pb_map, rsv)
    _ = gen_guardrails(cad, pb_map)
    _ = gen_auditoria(cad)

    print("\nOK. Arquivos gerados em data/<schema>/<tabela>/<tabela>.parquet")


if __name__ == "__main__":
    main()
