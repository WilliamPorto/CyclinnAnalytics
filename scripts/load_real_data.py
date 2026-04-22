"""
Carrega amostra REAL da Cyclinn (CSVs em tmp/) para os parquets em data/.

Fontes:
  tmp/cadastro-predio.csv   - 44 prédios em SP
  tmp/status_quartos.csv    - 18 unidades com marca (Cyclinn / Stay.cy)
  tmp/operacao_quartos.csv  - histórico de operação 2024 (não usado na V1)
  tmp/reservas.csv          - 27.508 reservas

Regras de mapeamento (confirmadas pelo usuário):
  1) Os 12 prédios órfãos (códigos em reservas sem cadastro) viram stubs
     na região "SP - Indefinido".
  2) Segmento padrão = flat; faixa padrão = standard.
  3) Mantemos os placeholders de regras (priori/posteriori/guardrails/auditoria)
     inferidos do regras.pdf — só reajustamos escopos pra apontarem pras
     novas regiões.
  4) calendario_unidade é reconstruído retrospectivamente a partir do
     preço efetivo (accommodation_fare / noites) das reservas.

Execução:
  .venv/bin/python scripts/load_real_data.py
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
TMP = ROOT / "tmp"

TODAY = date(2026, 4, 22)


def write_parquet(schema: str, table: str, df: pd.DataFrame) -> None:
    path = DATA_ROOT / schema / table
    path.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path / f"{table}.parquet", index=False)
    print(f"  {schema:<18}/{table:<34} {len(df):>7} rows")


def parse_br_money(s):
    if pd.isna(s) or s == "":
        return None
    return float(str(s).replace(".", "").replace(",", "."))


def parse_br_datetime(s):
    if pd.isna(s) or s == "":
        return None
    try:
        return pd.to_datetime(s, format="%d/%m/%Y %H:%M")
    except Exception:
        try:
            return pd.to_datetime(s, format="%d/%m/%Y")
        except Exception:
            return None


def daterange(start: date, end: date):
    d = start
    while d < end:
        yield d
        d += timedelta(days=1)


# ============================================================
# CADASTRO
# ============================================================

def load_cadastro():
    print("[cadastro]")

    cad_csv = pd.read_csv(TMP / "cadastro-predio.csv")
    status_csv = pd.read_csv(TMP / "status_quartos.csv", encoding="utf-8-sig")
    rsv_csv = pd.read_csv(TMP / "reservas.csv", encoding="utf-8-sig", sep=";", low_memory=False)

    # --- brands ---
    brand_names = sorted(status_csv["Operação"].dropna().unique().tolist())
    if "Cyclinn" not in brand_names:
        brand_names.insert(0, "Cyclinn")
    brands = pd.DataFrame(
        [{"brand_id": i + 1, "nome": n, "ativo": True} for i, n in enumerate(brand_names)]
    )
    brand_map = dict(zip(brands["nome"], brands["brand_id"]))
    write_parquet("cadastro", "brands", brands)

    # --- regioes ---
    # Preserva (regiao, uf, cidade) distintos do cadastro
    reg_df = cad_csv[["regiao", "uf", "cidade"]].drop_duplicates().reset_index(drop=True)
    regioes_rows = []
    for i, r in reg_df.iterrows():
        regioes_rows.append(
            {
                "regiao_id": i + 1,
                "nome": f"{r['uf']} - {r['regiao']}",
                "cidade": r["cidade"],
                "uf": r["uf"],
                "tipo_regiao": "urbano",
            }
        )
    # Região indefinida pros stubs
    indef_id = len(regioes_rows) + 1
    regioes_rows.append(
        {
            "regiao_id": indef_id,
            "nome": "SP - Indefinido",
            "cidade": "São Paulo",
            "uf": "SP",
            "tipo_regiao": "urbano",
        }
    )
    regioes = pd.DataFrame(regioes_rows)
    regiao_map = {(r["uf"], r["cidade"].split(" - ")[-1] if " - " in r["nome"] else r["nome"].split(" - ")[-1]): r["regiao_id"] for _, r in regioes.iterrows()}
    # mapping por nome da regiao original
    regiao_by_name = {}
    for i, r in reg_df.iterrows():
        regiao_by_name[r["regiao"]] = i + 1
    regiao_by_name["__indefinido__"] = indef_id
    write_parquet("cadastro", "regioes", regioes)

    # --- predios ---
    predios_rows = []
    predio_code_to_id = {}
    for i, row in cad_csv.iterrows():
        pid = i + 1
        predios_rows.append(
            {
                "predio_id": pid,
                "regiao_id": regiao_by_name[row["regiao"]],
                "nome": row["predio"],
                "endereco": None,
                "codigo": row["codigo"],
            }
        )
        predio_code_to_id[row["codigo"]] = pid

    # Stubs para prédios órfãos (códigos em reservas sem cadastro)
    listings = rsv_csv["listing_s_nickname"].dropna().astype(str)
    rsv_codes = set(listings.str.split().str[0].unique())
    cad_codes = set(cad_csv["codigo"].unique())
    orphan_codes = sorted(rsv_codes - cad_codes)
    for code in orphan_codes:
        pid = len(predios_rows) + 1
        predios_rows.append(
            {
                "predio_id": pid,
                "regiao_id": indef_id,
                "nome": f"Prédio {code} (stub)",
                "endereco": None,
                "codigo": code,
            }
        )
        predio_code_to_id[code] = pid

    predios = pd.DataFrame(predios_rows)
    write_parquet("cadastro", "predios", predios)

    # --- segmentos ---
    segmentos = pd.DataFrame(
        [
            {"segmento_id": 1, "nome": "studio"},
            {"segmento_id": 2, "nome": "flat"},
            {"segmento_id": 3, "nome": "1dorm"},
        ]
    )
    write_parquet("cadastro", "segmentos", segmentos)

    # --- faixas ---
    faixas = pd.DataFrame(
        [
            {"faixa_id": 1, "nome": "economy", "ordem": 1},
            {"faixa_id": 2, "nome": "standard", "ordem": 2},
            {"faixa_id": 3, "nome": "premium", "ordem": 3},
        ]
    )
    write_parquet("cadastro", "faixas_precificacao", faixas)

    # --- canais ---
    canal_commission = {
        "airbnb2": 0.18,
        "Booking.com": 0.15,
        "website": 0.00,
        "manual": 0.00,
        "owner": 0.00,
        "owner-guest": 0.00,
    }
    canais_rows = []
    for i, src in enumerate(sorted(rsv_csv["source"].dropna().unique())):
        canais_rows.append(
            {
                "canal_id": i + 1,
                "nome": src,
                "comissao_pct": canal_commission.get(src, 0.00),
            }
        )
    canais = pd.DataFrame(canais_rows)
    canal_map = dict(zip(canais["nome"], canais["canal_id"]))
    write_parquet("cadastro", "canais", canais)

    # --- unidades (a partir dos listings distintos) ---
    # Brand por prefixo (a partir de status_quartos)
    status_csv["code"] = status_csv["Entidade"].str.split().str[0]
    brand_by_prefix: dict[str, str] = {}
    for _, row in status_csv.iterrows():
        code = row["code"]
        op = row["Operação"]
        brand_by_prefix.setdefault(code, op)

    # Capacidade: usar max(number_of_guests) por listing (fallback 2)
    cap_by_listing = (
        rsv_csv.groupby("listing_s_nickname")["number_of_guests"].max().fillna(2).astype(int)
    )

    listings_uniq = sorted(listings.unique())
    unidades_rows = []
    listing_to_uid = {}
    for i, lst in enumerate(listings_uniq):
        uid = i + 1
        prefix = lst.split()[0]
        predio_id = predio_code_to_id.get(prefix)
        if predio_id is None:
            continue  # segurança — não deveria acontecer
        brand_name = brand_by_prefix.get(prefix, "Cyclinn")
        if brand_name not in brand_map:
            brand_name = "Cyclinn"
        unidades_rows.append(
            {
                "unidade_id": uid,
                "predio_id": predio_id,
                "brand_id": brand_map[brand_name],
                "segmento_id": 2,  # flat (default)
                "faixa_id": 2,  # standard (default)
                "codigo_externo": lst,
                "capacidade": int(cap_by_listing.get(lst, 2)) or 2,
                "ativo": True,
            }
        )
        listing_to_uid[lst] = uid

    unidades = pd.DataFrame(unidades_rows)
    write_parquet("cadastro", "unidades", unidades)

    return {
        "brands": brands,
        "regioes": regioes,
        "predios": predios,
        "segmentos": segmentos,
        "faixas": faixas,
        "canais": canais,
        "unidades": unidades,
        "listing_to_uid": listing_to_uid,
        "canal_map": canal_map,
        "brand_map": brand_map,
        "regiao_by_name": regiao_by_name,
        "indef_region_id": indef_id,
        "rsv_raw": rsv_csv,
    }


# ============================================================
# RESERVAS
# ============================================================

def load_reservas(cad):
    print("[reservas]")

    rsv_csv = cad["rsv_raw"]
    listing_to_uid = cad["listing_to_uid"]
    canal_map = cad["canal_map"]

    date_cols = ["creation_date", "confirmation_date", "alteration_date", "cancellation_date", "check_in", "check_out"]
    money_cols = ["accommodation_fare", "cleaning_fare", "channel_commission", "subtotal_price", "total_paid"]

    df = rsv_csv.copy()
    for c in date_cols:
        df[c] = df[c].apply(parse_br_datetime)
    for c in money_cols:
        df[c] = df[c].apply(parse_br_money)

    df["unidade_id"] = df["listing_s_nickname"].map(listing_to_uid)
    df["canal_id"] = df["source"].map(canal_map)

    # Drop reservas sem mapeamento (não deveria existir)
    drop_mask = df["unidade_id"].isna() | df["canal_id"].isna()
    if drop_mask.any():
        print(f"  WARN: descartadas {drop_mask.sum()} reservas sem FK válida")
    df = df[~drop_mask].copy()
    df["unidade_id"] = df["unidade_id"].astype(int)
    df["canal_id"] = df["canal_id"].astype(int)

    # reserva_id sequencial
    df = df.sort_values("creation_date").reset_index(drop=True)
    df["reserva_id"] = range(1, len(df) + 1)

    # valor_total: total_paid quando > 0 (Airbnb, website); senão subtotal_price
    # (Booking.com não cobra via plataforma → total_paid=0 porém subtotal_price tem o valor)
    valor_total = df["total_paid"].where(df["total_paid"].fillna(0) > 0, df["subtotal_price"])

    reservas = pd.DataFrame(
        {
            "reserva_id": df["reserva_id"],
            "unidade_id": df["unidade_id"],
            "canal_id": df["canal_id"],
            "hospede_nome": df["guest_s_name"],
            "data_checkin": df["check_in"].dt.date,
            "data_checkout": df["check_out"].dt.date,
            "criada_em": df["creation_date"].dt.tz_localize("UTC", ambiguous="NaT", nonexistent="NaT"),
            "status": df["status"],
            "valor_total": valor_total,
            "noites": df["number_of_nights"].astype("Int64"),
            "confirmation_code": df["confirmation_code"],
            "accommodation_fare": df["accommodation_fare"],
            "cleaning_fare": df["cleaning_fare"],
            "channel_commission": df["channel_commission"],
        }
    )
    write_parquet("reservas", "reservas", reservas)

    # --- reserva_diarias: explodir por noite ---
    rd_rows = []
    for _, r in df.iterrows():
        if pd.isna(r["check_in"]) or pd.isna(r["check_out"]) or r["status"] != "confirmed":
            continue
        noites = int(r["number_of_nights"])
        if noites <= 0 or pd.isna(r["accommodation_fare"]):
            continue
        valor_noite = round(r["accommodation_fare"] / noites, 2)
        ci = r["check_in"].date()
        for i in range(noites):
            rd_rows.append(
                {
                    "unidade_id": int(r["unidade_id"]),
                    "data": ci + timedelta(days=i),
                    "reserva_id": int(r["reserva_id"]),
                    "valor_diaria": valor_noite,
                }
            )
    rd = pd.DataFrame(rd_rows)
    # Conflitos: mesma unidade/data por duas reservas ativas — keep first (cronológico)
    rd = rd.drop_duplicates(subset=["unidade_id", "data"], keep="first")
    rd["data"] = pd.to_datetime(rd["data"]).dt.date
    write_parquet("reservas", "reserva_diarias", rd)

    return {"reservas": reservas, "reserva_diarias": rd, "df_full": df}


# ============================================================
# PRECO_BASE (derivado da média histórica por unidade)
# ============================================================

def load_preco_base(cad, rsv_state):
    print("[preco_base]")

    unidades = cad["unidades"]
    df = rsv_state["df_full"]

    df_ok = df[(df["status"] == "confirmed") & (df["accommodation_fare"].notna()) & (df["number_of_nights"] > 0)].copy()
    df_ok["diaria"] = df_ok["accommodation_fare"] / df_ok["number_of_nights"]
    pb_by_uid = df_ok.groupby("unidade_id")["diaria"].median()

    # Fallback: mediana global se alguma unidade não tiver histórico
    fallback = pb_by_uid.median() if not pb_by_uid.empty else 300.0

    rows = []
    pb_id = 1
    for _, u in unidades.iterrows():
        uid = int(u["unidade_id"])
        valor = float(pb_by_uid.get(uid, fallback))
        rows.append(
            {
                "preco_base_id": pb_id,
                "unidade_id": uid,
                "valor": round(valor, 2),
                "vigencia_inicio": date(2025, 1, 1),
                "vigencia_fim": None,
                "motivo": "derivado da mediana histórica (accommodation_fare / noites)",
            }
        )
        pb_id += 1

    df_pb = pd.DataFrame(rows)
    df_pb["vigencia_inicio"] = pd.to_datetime(df_pb["vigencia_inicio"]).dt.date
    df_pb["vigencia_fim"] = pd.to_datetime(df_pb["vigencia_fim"]).dt.date
    write_parquet("preco_base", "precos_base", df_pb)
    return df_pb


def pb_atual(df_pb: pd.DataFrame) -> dict:
    atuais = df_pb[df_pb["vigencia_fim"].isna()]
    return dict(zip(atuais["unidade_id"], atuais["valor"]))


# ============================================================
# REGRAS PRIORI (placeholders do sample, ajustados ao catálogo real)
# ============================================================

def gen_regras_priori(cad):
    print("[regras_priori]")

    regioes = cad["regioes"]
    all_regiao_ids = regioes["regiao_id"].tolist()

    # --- Sazonalidade ---
    saz = pd.DataFrame(
        [
            {"regra_id": 1, "escopo": "global", "escopo_id": None,
             "nome": "Verão 2026", "data_inicio": date(2026, 12, 15), "data_fim": date(2027, 2, 28),
             "ajuste_pct": 0.25, "recorrente_anual": True, "prioridade": 10},
            {"regra_id": 2, "escopo": "global", "escopo_id": None,
             "nome": "Férias Julho", "data_inicio": date(2026, 7, 1), "data_fim": date(2026, 7, 31),
             "ajuste_pct": 0.15, "recorrente_anual": True, "prioridade": 10},
            {"regra_id": 3, "escopo": "global", "escopo_id": None,
             "nome": "Baixa Pós-Carnaval", "data_inicio": date(2026, 2, 20), "data_fim": date(2026, 3, 15),
             "ajuste_pct": -0.10, "recorrente_anual": True, "prioridade": 10},
            {"regra_id": 4, "escopo": "global", "escopo_id": None,
             "nome": "Virada de Ano", "data_inicio": date(2026, 12, 28), "data_fim": date(2027, 1, 3),
             "ajuste_pct": 0.40, "recorrente_anual": True, "prioridade": 30},
        ]
    )
    saz["data_inicio"] = pd.to_datetime(saz["data_inicio"]).dt.date
    saz["data_fim"] = pd.to_datetime(saz["data_fim"]).dt.date
    write_parquet("regras_priori", "regras_sazonalidade", saz)

    # --- Dia da semana (por região) ---
    dow_adj = {0: -0.05, 1: 0.08, 2: 0.10, 3: 0.10, 4: 0.08, 5: -0.05, 6: -0.10}
    dow_rows = []
    regra_id = 1
    for regiao_id in all_regiao_ids:
        for dow, adj in dow_adj.items():
            dow_rows.append(
                {"regra_id": regra_id, "escopo": "regiao", "escopo_id": regiao_id,
                 "dia_semana": dow, "ajuste_pct": adj}
            )
            regra_id += 1
    write_parquet("regras_priori", "regras_dia_semana", pd.DataFrame(dow_rows))

    # --- Eventos ---
    eventos = pd.DataFrame(
        [
            {"evento_id": 1, "nome": "F1 GP São Paulo 2026", "data_inicio": date(2026, 11, 6), "data_fim": date(2026, 11, 8), "categoria": "esportivo"},
            {"evento_id": 2, "nome": "The Town 2026", "data_inicio": date(2026, 9, 3), "data_fim": date(2026, 9, 12), "categoria": "show"},
            {"evento_id": 3, "nome": "Lollapalooza 2026", "data_inicio": date(2026, 3, 27), "data_fim": date(2026, 3, 29), "categoria": "show"},
            {"evento_id": 4, "nome": "Carnaval 2026", "data_inicio": date(2026, 2, 14), "data_fim": date(2026, 2, 18), "categoria": "feriado"},
            {"evento_id": 5, "nome": "Tiradentes", "data_inicio": date(2026, 4, 21), "data_fim": date(2026, 4, 21), "categoria": "feriado"},
            {"evento_id": 6, "nome": "Corpus Christi", "data_inicio": date(2026, 6, 4), "data_fim": date(2026, 6, 4), "categoria": "feriado"},
            {"evento_id": 7, "nome": "Independência", "data_inicio": date(2026, 9, 7), "data_fim": date(2026, 9, 7), "categoria": "feriado"},
            {"evento_id": 8, "nome": "N. Sra. Aparecida", "data_inicio": date(2026, 10, 12), "data_fim": date(2026, 10, 12), "categoria": "feriado"},
            {"evento_id": 9, "nome": "Finados", "data_inicio": date(2026, 11, 2), "data_fim": date(2026, 11, 2), "categoria": "feriado"},
        ]
    )
    eventos["data_inicio"] = pd.to_datetime(eventos["data_inicio"]).dt.date
    eventos["data_fim"] = pd.to_datetime(eventos["data_fim"]).dt.date
    write_parquet("regras_priori", "eventos", eventos)

    # --- Evento impactos (em todas as regiões de SP) ---
    impactos = []
    impacto_id = 1
    event_global_adj = {1: 0.70, 2: 0.50, 3: 0.45, 4: -0.05, 5: 0.05, 6: 0.05, 7: 0.05, 8: 0.05, 9: 0.05}
    for evt_id, adj in event_global_adj.items():
        for regiao_id in all_regiao_ids:
            impactos.append({"impacto_id": impacto_id, "evento_id": evt_id, "escopo": "regiao", "escopo_id": regiao_id, "ajuste_pct": adj})
            impacto_id += 1
    write_parquet("regras_priori", "evento_impactos", pd.DataFrame(impactos))

    # --- Antecedência ---
    ant_rows = []
    rid = 1
    for lead_min, lead_max, adj in [(180, 365, 0.35), (90, 180, 0.20), (30, 90, 0.05)]:
        ant_rows.append({"regra_id": rid, "escopo": "global", "escopo_id": None,
                         "lead_min_dias": lead_min, "lead_max_dias": lead_max,
                         "dia_semana": None, "ajuste_pct": adj})
        rid += 1
    dow_ant = {0: 0.05, 1: 0.10, 2: 0.14, 3: 0.10, 4: 0.0, 5: 0.0, 6: 0.0}
    for dow, adj in dow_ant.items():
        ant_rows.append({"regra_id": rid, "escopo": "global", "escopo_id": None,
                         "lead_min_dias": 0, "lead_max_dias": 15,
                         "dia_semana": dow, "ajuste_pct": adj})
        rid += 1
    write_parquet("regras_priori", "regras_antecedencia", pd.DataFrame(ant_rows))


# ============================================================
# REGRAS POSTERIORI (placeholders, escopos ajustados)
# ============================================================

def gen_regras_posteriori(cad, calendar_start, calendar_end):
    print("[regras_posteriori]")

    all_regiao_ids = cad["regioes"]["regiao_id"].tolist()

    # --- Ocupação individual (tabela do PDF, global) ---
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
    ind_rows = []
    rid = 1
    for j, omin, omax, adj in ind_spec:
        ind_rows.append({"regra_id": rid, "escopo": "global", "escopo_id": None,
                         "janela_dias": j, "ocupacao_min_pct": omin, "ocupacao_max_pct": omax,
                         "ajuste_pct": adj, "cumulativo": True})
        rid += 1
    write_parquet("regras_posteriori", "regras_ocupacao_individual", pd.DataFrame(ind_rows))

    # --- Ocupação portfolio (por região) ---
    port_spec = [
        (14, None, 0.40, -0.08), (14, 0.60, None, 0.08),
        (7, None, 0.45, -0.10), (7, 0.65, None, 0.10),
        (3, None, 0.55, -0.10), (3, 0.75, None, 0.10),
    ]
    port_rows = []
    rid = 1
    for j, omin, omax, adj in port_spec:
        for regiao_id in all_regiao_ids:
            port_rows.append({"regra_id": rid, "escopo": "regiao", "escopo_id": regiao_id,
                              "janela_dias": j, "ocupacao_min_pct": omin, "ocupacao_max_pct": omax,
                              "ajuste_pct": adj, "cumulativo": True})
            rid += 1
    write_parquet("regras_posteriori", "regras_ocupacao_portfolio", pd.DataFrame(port_rows))

    # --- Expectativa portfolio ---
    exp_rows = []
    exp_id = 1
    for regiao_id in all_regiao_ids:
        for seg_id in [1, 2, 3]:
            for d in daterange(calendar_start, calendar_end):
                dow = d.weekday()
                base = {1: 0.62, 2: 0.65, 3: 0.68}[seg_id]
                dow_mod = {0: -0.05, 1: 0.03, 2: 0.05, 3: 0.05, 4: 0.03, 5: -0.02, 6: -0.08}[dow]
                month = d.month
                saz_mod = 0.05 if month in (7, 12) else (-0.05 if month == 3 else 0.0)
                esperada = max(0.3, min(0.92, base + dow_mod + saz_mod))
                exp_rows.append({"expectativa_id": exp_id, "regiao_id": regiao_id, "segmento_id": seg_id,
                                 "data": d, "ocupacao_esperada_pct": round(esperada, 4),
                                 "fonte": "historico_interno"})
                exp_id += 1
    exp = pd.DataFrame(exp_rows)
    exp["data"] = pd.to_datetime(exp["data"]).dt.date
    write_parquet("regras_posteriori", "expectativa_portfolio", exp)

    # --- Ocupação externa ---
    oc_ext_rows = []
    for regiao_id in all_regiao_ids:
        for d in daterange(calendar_start, calendar_end):
            dow = d.weekday()
            base = 0.58 + random.uniform(-0.05, 0.05)
            dow_mod = {0: -0.05, 1: 0.02, 2: 0.05, 3: 0.05, 4: 0.02, 5: -0.05, 6: -0.10}[dow]
            ocup = max(0.25, min(0.95, base + dow_mod))
            adr = 300 + (regiao_id * 20) + random.uniform(-30, 30)
            oc_ext_rows.append({"regiao_id": regiao_id, "data": d,
                                "ocupacao_mercado_pct": round(ocup, 4),
                                "adr_mercado": round(adr, 2)})
    oc_ext = pd.DataFrame(oc_ext_rows)
    oc_ext["data"] = pd.to_datetime(oc_ext["data"]).dt.date
    write_parquet("regras_posteriori", "ocupacao_externa", oc_ext)


# ============================================================
# CALENDARIO (retrospectivo a partir de reserva_diarias)
# ============================================================

def gen_calendario(cad, pb_map, rsv_state, calendar_start, calendar_end):
    print("[calendario]")

    unidades = cad["unidades"]
    rd = rsv_state["reserva_diarias"]
    rd_map = dict(zip(zip(rd["unidade_id"], rd["data"]), rd["valor_diaria"]))

    cal_rows = []
    for _, u in unidades.iterrows():
        uid = int(u["unidade_id"])
        pb = pb_map.get(uid, 300.0)
        for d in daterange(calendar_start, calendar_end):
            lead = (d - TODAY).days
            dow = d.weekday()

            saz = 0.0
            if d.month == 12 and d.day >= 15:
                saz = 0.25
            elif d.month == 1 or (d.month == 2 and d.day <= 28):
                saz = 0.25 if d.month == 1 else 0.15
            elif d.month == 7:
                saz = 0.15

            dia_sem = {0: -0.05, 1: 0.08, 2: 0.10, 3: 0.10, 4: 0.08, 5: -0.05, 6: -0.10}[dow]

            ev = 0.0
            if d in [date(2026, 11, 7), date(2026, 11, 8)]:
                ev = 0.70

            if lead < 0:
                ant = 0.0
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
            efetivo = rd_map.get((uid, d))

            if efetivo is not None:
                diaria = float(efetivo)
                ajuste_port = 0.0
                ajuste_ind = 0.0
            else:
                ajuste_port = random.uniform(-0.04, 0.04)
                ajuste_ind = random.uniform(-0.04, 0.04)
                diaria = pi * (1 + ajuste_port + ajuste_ind)

            clamped = max(pb * 0.6, min(pb * 3.0, diaria))

            cal_rows.append(
                {
                    "unidade_id": uid,
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
                    "diaria_final_clamped": round(clamped, 2),
                    "calculado_em": datetime.now(timezone.utc),
                    "versao": 1,
                }
            )
    cal = pd.DataFrame(cal_rows)
    cal["data"] = pd.to_datetime(cal["data"]).dt.date
    write_parquet("calendario", "calendario_unidade", cal)

    # Historico: amostra 5% com versão anterior
    sample = cal.sample(frac=0.05, random_state=SEED).copy()
    sample["diaria_final"] = sample["diaria_final"] * random.uniform(0.9, 1.1)
    sample["diaria_final_clamped"] = sample["diaria_final"]
    sample["versao"] = 0
    sample["calculado_em"] = sample["calculado_em"] - pd.Timedelta(days=7)
    sample["valido_ate"] = pd.Timestamp.now(tz="UTC") - pd.Timedelta(days=7)
    write_parquet("calendario", "calendario_unidade_historico", sample)


# ============================================================
# GUARDRAILS / AUDITORIA (placeholders)
# ============================================================

def gen_guardrails(cad, pb_map):
    print("[guardrails]")

    rows = []
    for _, u in cad["unidades"].iterrows():
        uid = int(u["unidade_id"])
        pb = pb_map.get(uid, 300.0)
        rows.append(
            {"unidade_id": uid, "preco_min": round(pb * 0.60, 2),
             "preco_max": round(pb * 3.00, 2), "delta_max_pct": 0.15,
             "delta_max_abs": round(pb * 0.50, 2)}
        )
    write_parquet("guardrails", "guardrails_unidade", pd.DataFrame(rows))

    # Overrides de exemplo (5 unidades aleatórias)
    unidade_ids = cad["unidades"]["unidade_id"].tolist()
    overrides = []
    for i, uid in enumerate(random.sample(unidade_ids, min(5, len(unidade_ids))), start=1):
        pb = pb_map.get(int(uid), 300.0)
        di = TODAY + timedelta(days=random.randint(30, 60))
        df_end = di + timedelta(days=random.randint(2, 7))
        usa_fixo = random.random() < 0.5
        overrides.append(
            {"override_id": i, "unidade_id": int(uid),
             "data_inicio": di, "data_fim": df_end,
             "preco_fixo": round(pb * 1.8, 2) if usa_fixo else None,
             "ajuste_pct_forcado": None if usa_fixo else 0.50,
             "motivo": "trava por evento privado" if usa_fixo else "promo direcionada",
             "criado_por": "rm@cyclinn.com.br", "ativo": True}
        )
    ov = pd.DataFrame(overrides)
    ov["data_inicio"] = pd.to_datetime(ov["data_inicio"]).dt.date
    ov["data_fim"] = pd.to_datetime(ov["data_fim"]).dt.date
    write_parquet("guardrails", "overrides_preco", ov)


def gen_auditoria(cad, calendar_start, calendar_end):
    print("[auditoria]")

    unidade_ids = cad["unidades"]["unidade_id"].tolist()

    logs = []
    for i in range(100):
        uid = random.choice(unidade_ids)
        d = calendar_start + timedelta(days=random.randint(0, (calendar_end - calendar_start).days - 1))
        logs.append(
            {"log_id": i + 1, "rodada_id": str(uuid.uuid4()),
             "unidade_id": int(uid), "data": d,
             "trigger": random.choice(["cron_diario", "nova_reserva", "evento_novo", "manual"]),
             "duracao_ms": random.randint(5, 180), "erro": None}
        )
    log_df = pd.DataFrame(logs)
    log_df["data"] = pd.to_datetime(log_df["data"]).dt.date
    write_parquet("auditoria", "log_recalculo", log_df)

    ev = []
    for _ in range(50):
        uid = random.choice(unidade_ids)
        d = TODAY + timedelta(days=random.randint(0, 180))
        periodo = random.choice([21, 14, 7, 5, 3, 1, 0])
        delta = random.uniform(-0.20, 0.20)
        ev.append(
            {"unidade_id": int(uid), "data": d,
             "periodo_analisado_dias": periodo,
             "delta_ocupacao_pct": round(delta, 4),
             "limite_configurado": 0.10,
             "foi_relevante": abs(delta) > 0.10}
        )
    ev_df = pd.DataFrame(ev)
    ev_df["data"] = pd.to_datetime(ev_df["data"]).dt.date
    write_parquet("auditoria", "alteracao_relevante_eventos", ev_df)


# ============================================================
# VERIFICAÇÃO DE INTEGRIDADE
# ============================================================

def verify_integrity():
    import duckdb
    con = duckdb.connect()
    def q(sql):
        return con.execute(sql).fetchall()

    print("\n[verificação de integridade]")

    checks = [
        ("unidades sem predio", """
            SELECT COUNT(*) FROM read_parquet('data/cadastro/unidades/unidades.parquet') u
            LEFT JOIN read_parquet('data/cadastro/predios/predios.parquet') p USING(predio_id)
            WHERE p.predio_id IS NULL
        """),
        ("unidades sem brand", """
            SELECT COUNT(*) FROM read_parquet('data/cadastro/unidades/unidades.parquet') u
            LEFT JOIN read_parquet('data/cadastro/brands/brands.parquet') b USING(brand_id)
            WHERE b.brand_id IS NULL
        """),
        ("predios sem regiao", """
            SELECT COUNT(*) FROM read_parquet('data/cadastro/predios/predios.parquet') p
            LEFT JOIN read_parquet('data/cadastro/regioes/regioes.parquet') r USING(regiao_id)
            WHERE r.regiao_id IS NULL
        """),
        ("reservas sem unidade", """
            SELECT COUNT(*) FROM read_parquet('data/reservas/reservas/reservas.parquet') r
            LEFT JOIN read_parquet('data/cadastro/unidades/unidades.parquet') u USING(unidade_id)
            WHERE u.unidade_id IS NULL
        """),
        ("reservas sem canal", """
            SELECT COUNT(*) FROM read_parquet('data/reservas/reservas/reservas.parquet') r
            LEFT JOIN read_parquet('data/cadastro/canais/canais.parquet') c USING(canal_id)
            WHERE c.canal_id IS NULL
        """),
        ("reserva_diarias sem reserva", """
            SELECT COUNT(*) FROM read_parquet('data/reservas/reserva_diarias/reserva_diarias.parquet') rd
            LEFT JOIN read_parquet('data/reservas/reservas/reservas.parquet') r USING(reserva_id)
            WHERE r.reserva_id IS NULL
        """),
        ("precos_base sem unidade", """
            SELECT COUNT(*) FROM read_parquet('data/preco_base/precos_base/precos_base.parquet') pb
            LEFT JOIN read_parquet('data/cadastro/unidades/unidades.parquet') u USING(unidade_id)
            WHERE u.unidade_id IS NULL
        """),
        ("calendario sem unidade", """
            SELECT COUNT(*) FROM read_parquet('data/calendario/calendario_unidade/calendario_unidade.parquet') c
            LEFT JOIN read_parquet('data/cadastro/unidades/unidades.parquet') u USING(unidade_id)
            WHERE u.unidade_id IS NULL
        """),
    ]

    fail = 0
    for label, sql in checks:
        (cnt,) = q(sql)[0]
        status = "OK" if cnt == 0 else f"FAIL ({cnt} órfãos)"
        print(f"  {label:<35} {status}")
        if cnt:
            fail += 1

    print(f"\n{'' if fail == 0 else '⚠  '}integridade: {len(checks) - fail}/{len(checks)} checks OK")


# ============================================================
# MAIN
# ============================================================

def main():
    print(f"DATA_ROOT: {DATA_ROOT}")
    DATA_ROOT.mkdir(parents=True, exist_ok=True)

    cad = load_cadastro()
    rsv = load_reservas(cad)

    pb_df = load_preco_base(cad, rsv)
    pb_map = pb_atual(pb_df)

    # Janela do calendário com base nas reservas reais
    df = rsv["df_full"]
    ci_min = df["check_in"].dropna().min()
    co_max = df["check_out"].dropna().max()
    calendar_start = ci_min.date() if pd.notna(ci_min) else date(2025, 6, 1)
    calendar_end = (co_max.date() + timedelta(days=1)) if pd.notna(co_max) else date(2026, 12, 31)
    print(f"\njanela do calendário: {calendar_start} → {calendar_end} ({(calendar_end - calendar_start).days} dias)")

    gen_regras_priori(cad)
    gen_regras_posteriori(cad, calendar_start, calendar_end)
    gen_calendario(cad, pb_map, rsv, calendar_start, calendar_end)
    gen_guardrails(cad, pb_map)
    gen_auditoria(cad, calendar_start, calendar_end)

    verify_integrity()
    print("\nOK. Dados reais carregados em data/<schema>/<tabela>/<tabela>.parquet")


if __name__ == "__main__":
    main()
