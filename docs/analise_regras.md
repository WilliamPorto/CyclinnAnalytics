# Análise do documento `regras.pdf`

> Documento base: `regras.pdf` — especificação do motor de precificação dinâmica da Cyclinn.
> Esta análise identifica o que está bem definido, o que é ambíguo, o que está incompleto, e quais perguntas precisam ser respondidas antes da implementação.

---

## 1. Resumo executivo

O PDF define a arquitetura de um motor de **dynamic pricing / Best Available Rate (BAR)** para aluguel de curta e longa estadia. O modelo é compatível com o padrão de mercado usado por players como Sonder, Nomah, Tabas e Housi.

**Estrutura em duas camadas:**

- **Camada "a priori"** — variáveis conhecidas antecipadamente (sazonalidade, dia da semana, eventos, antecedência) que definem um **Preço Inicial** `Pi`.
- **Camada "a posteriori"** — ajustes reativos com base em ocupação (do portfólio e da unidade individual) que convertem `Pi` na **Diária Final** `D`.

**Fórmulas centrais:**

```
D(u, d)  = Pi(u, d) × (1 + AjustePortfolio + AjusteIndividual)
Pi(u, d) = Pb(u)    × (1 + Saz + DiaSem + Ev + Ant)
```

Onde `u` = unidade, `d` = dia, `Pb` = preço base por faixa de precificação.

---

## 2. O que está bem definido

| Item | Status |
|---|---|
| Fórmula principal (D e Pi) | Clara e implementável |
| Antecedência — 4 faixas não-cumulativas (180-365, 90-180, 30-90, 0-15) | Completa, com detalhamento por dia da semana na faixa 0-15 |
| Ocupação individual — thresholds por janela (21d, 14d, 7d, 5d, 3d, 1d, no dia) | Thresholds e percentuais definidos |
| Proposta de estrutura em tabelas (uma por fator, "caminham" 1 dia/dia) | Boa base para MVP em planilha |
| Conceito de `ExpectativaPortfolio` vs ocupação atual | Direção correta |

---

## 3. Ambiguidades que precisam ser resolvidas

Cada ambiguidade tem um **ID** para facilitar o rastreamento em workshop com o cliente.

### AMB-01 — "Cancela 1" não está explicado
> Trecho: *"Se AjustePortfolio == AjusteIndividual; Cancela 1"*

Três leituras possíveis:
- **(a)** Aplica apenas um dos dois (evita dobrar um +10% em +20%) — **interpretação mais provável**
- (b) Subtrai 1 ponto percentual de um dos lados
- (c) Outra lógica

**Impacto:** determina se uma unidade vazia em portfólio vazio pega -20% ou -10%.

### AMB-02 — "Ocupação (Cumulativo)" cumulativo com quem?
As janelas (21d, 14d, 7d, 5d, 3d, 1d, no dia) **se sobrepõem**. Três interpretações:
- **(a)** Todas as janelas aplicáveis somam — unidade vazia pega `-10% + -10% + -10% + -10% = -40%`, o que **quebra margem**.
- **(b)** Somente a janela mais curta aplicável vale — aí não é cumulativo de fato.
- **(c)** Cumulativo entre `AjustePortfolio` e `AjusteIndividual`, mas só a janela mais curta dentro de cada camada.

**Impacto:** comportamento do preço em baixa ocupação muda drasticamente.

### AMB-03 — Gap de 15–30 dias na antecedência
As faixas definidas cobrem `0-15`, `30-90`, `90-180`, `180-365`. O intervalo `15-30` dias não está especificado. Assume-se 0%, mas precisa estar explícito.

### AMB-04 — `AlteraçãoRelevante` está incompleta
O PDF termina na definição para `Ant > 180` (período analisado = 7 dias). Faltam regras para as demais faixas de antecedência (≤ 180, 90, 30, 15, 7, 3, 1) e o valor do limite `x`.

### AMB-05 — Fórmula `AjustePortfolio = Ocup(p)(d)` está incompleta
Como está, sugere que o ajuste é igual à própria taxa de ocupação (uma porcentagem absoluta). Deveria ser uma **função** da comparação entre `Ocup` atual e `ExpectativaPortfolio`. Thresholds e percentuais da camada de portfólio **não estão no documento** — só os da camada individual.

### AMB-06 — Origem de `ExpectativaPortfolio` não está definida
Possíveis fontes:
- Histórico interno (mesmo mês/dia da semana do ano anterior)
- Benchmark externo (AirDNA, STR, Mercado de Temporada)
- Mix ponderado
- Input manual do revenue manager

**Decisão de produto importante** — sem fonte confiável, o ajuste vira ruído.

### AMB-07 — Interno vs externo
> *"Ocupação considerada é do portfólio da localidade (interno) ou mercado (externo)"*

O "ou" exige escolha explícita. Trade-off:
- **Interno**: dado confiável, mas portfólio pequeno → amostra ruim
- **Externo**: reflete mercado, mas custa (AirDNA ~US$ 50–200/mês por cidade) e tem lag

### AMB-08 — Aditivo vs multiplicativo
Todos os fatores entram como `(1 + a + b + c + d)` — soma dentro do parêntese. Em situações de stack de fatores fortes pode estourar:
- F1 (+50%) + Verão (+25%) + Sábado (+15%) + Antecedência 180d (+35%) = +125% (2,25×)

Se a intenção fosse multiplicativa (`1,50 × 1,25 × 1,15 × 1,35 = 2,91×`), seria ainda mais agressivo. Validar intenção.

### AMB-09 — Feriado: `evento` ou `sazonalidade`?
O PDF lista feriado dentro de **Eventos**. Mas feriado é recorrente e previsível — poderia estar em **Saz** ou **DiaSem**. Definir para evitar dupla contagem.

---

## 4. Lacunas (itens não tratados no PDF)

| # | Item | Por que importa |
|---|---|---|
| LAC-01 | **Segmentação de regras** (urbano/lazer, flat/studio) | Mencionada, mas regras não variam por segmento no doc |
| LAC-02 | **Faixas de `Pb`** (economy/standard/premium) | Conceito mencionado, faixas não definidas |
| LAC-03 | **Length-of-Stay discounts** | Crítico para Cyclinn, que faz **estadias longas**. Padrão: -10% para 7+ noites, -20% para 28+ |
| LAC-04 | **Diferenciação por canal** | Airbnb, Booking e direto têm comissões e elasticidades distintas |
| LAC-05 | **Marca Stay.cy** | Mesma fórmula com Pb diferente ou algoritmo próprio? |
| LAC-06 | **Frequência de recálculo** | Diário? A cada reserva? Quando muda um evento? |
| LAC-07 | **Override manual** | Revenue manager precisa poder "travar" uma data |
| LAC-08 | **Rate shopping (preço do concorrente)** | Ocupação é um sinal; preço do concorrente é outro |
| LAC-09 | **Floor e ceiling** | Sem eles, o algoritmo pode reduzir preço até abaixo do custo |
| LAC-10 | **`deltaX` quantificado** | Mencionado como limite de variação, valor não definido |

---

## 5. Riscos identificados

### RISC-01 — Espiral de descontos
Sem preço mínimo (floor) e com a ambiguidade AMB-02 resolvida como "cumulativo total", uma unidade em baixa ocupação pode receber -40% ou mais, destruindo margem.

**Mitigação:** definir `preco_min` por unidade antes do launch.

### RISC-02 — Preços absurdos em stacks de fatores
Antecedência +35% combinada com evento forte (+50%) e sazonalidade alta (+25%) pode gerar preço 2× a 3× o Pb. Pode ser intencional, mas precisa de ceiling para casos patológicos.

**Mitigação:** definir `preco_max` e validar com histórico real de bookings.

### RISC-03 — Reprecificação instável
Sem `deltaX` definido, o preço pode oscilar dia-a-dia conforme entram/saem reservas, deteriorando a experiência do cliente que acompanha o preço.

**Mitigação:** limitar variação diária (ex: máx ±15% vs D-1) e congelar datas após reserva confirmada.

### RISC-04 — `ExpectativaPortfolio` mal calibrada
Se a expectativa for mal formada (ex: baseada em histórico de época atípica), o `AjustePortfolio` induz decisões erradas o tempo todo.

**Mitigação:** começar com expectativa conservadora + override manual fácil enquanto dados maturam.

### RISC-05 — Dupla contagem (Ant × Ocup)
Antecedência é tempo puro; ocupação também é função de tempo. Ambos capturam o mesmo "sinal de demanda" de ângulos diferentes. Pode haver duplicação.

**Mitigação:** tratar `Ant` como discount puro de tempo/valor-do-dinheiro e `Ocup` como sinal de demanda real; validar com backtest.

---

## 6. Perguntas a levar para o workshop com a Cyclinn

Agrupei em blocos para facilitar a reunião.

### Bloco A — Semântica das regras (responde AMB-01 a AMB-03, AMB-08, AMB-09)
1. O que "Cancela 1" significa exatamente?
2. Em "Ocupação (Cumulativo)", cumulativo entre quais elementos?
3. O que acontece com antecedência entre 15 e 30 dias?
4. A composição `(1 + a + b + c + d)` é intencional, ou vocês queriam multiplicativa?
5. Feriado é `evento` ou entra em `sazonalidade`/`dia da semana`?

### Bloco B — Completude do algoritmo (responde AMB-04, AMB-05)
6. Quais as regras de `AlteraçãoRelevante` para `Ant ≤ 180`?
7. Qual o valor do limite `x` em `AlteraçãoRelevante`?
8. Como é a tabela de thresholds do `AjustePortfolio` (análoga à do individual)?

### Bloco C — Dados (responde AMB-06, AMB-07)
9. `ExpectativaPortfolio` vem de onde? Histórico interno, benchmark externo, manual?
10. A ocupação usada é a do portfólio interno da cidade ou benchmark de mercado?

### Bloco D — Governança e guardrails (responde LAC-07, LAC-09, LAC-10)
11. Quem pode fazer override manual? Que permissão tem?
12. Qual o preço mínimo aceitável por unidade? (custo + margem mínima)
13. Qual a variação diária máxima aceitável?

### Bloco E — Segmentação e marcas (responde LAC-01, LAC-02, LAC-05)
14. Quais as faixas de `Pb`? (economy/standard/premium)
15. Regras diferem entre urbano/lazer e entre flat/studio?
16. Stay.cy usa a mesma fórmula ou outra lógica?

### Bloco F — Extensões de modelo (responde LAC-03, LAC-04, LAC-08)
17. Desconto por Length-of-Stay (LOS) existe? Qual a política?
18. Preço varia por canal (Airbnb vs direto)?
19. Existe rate shopping de concorrentes? Fonte de dados?

---

## 7. Próximos passos recomendados

1. **Workshop de 1h** com Cyclinn para fechar os Blocos A–C (ambiguidades críticas).
2. **Protótipo em Python** da fórmula com dados sintéticos para visualizar comportamento do preço nos casos de borda.
3. **Definir guardrails** (floor/ceiling/deltaX) antes de qualquer tuning fino.
4. **Backtest** com 90 dias de histórico de reservas reais — comparar preço gerado pelo motor vs preço praticado.
5. **Schema de dados** e pipeline para suportar operação diária (em progresso — ver `schema.md` quando criado).

---

## 8. Apêndice — quadro das regras tal como estão no PDF

### Antecedência (não-cumulativo)
| Faixa | Ajuste | Observação |
|---|---|---|
| 180–365 dias | +35% | todos os dias da semana |
| 90–180 dias | +20% | todos os dias da semana |
| 30–90 dias | +5% | todos os dias da semana |
| 15–30 dias | **(não definido)** | — |
| 0–15 dias (seg) | +5% | |
| 0–15 dias (ter, qui) | +10% | |
| 0–15 dias (qua) | +14% | |
| 0–15 dias (sex, sáb, dom) | 0% | |

### Ocupação individual (cumulativo — semântica ambígua, ver AMB-02)
| Janela | Regra |
|---|---|
| 21+ dias | > 30% → +10% |
| 14 dias | > 40% → +10% |
| 7 dias | < 30% → -10%; < 40% → -5%; > 50% → +10% |
| 5 dias | < 40% → -10%; < 50% → -5%; > 60% → +10% |
| 3 dias | < 50% → -10%; < 60% → -5%; > 70% → +10% |
| 1 dia | < 65% → -10%; < 75% → -5%; > 85% → +10% |
| No dia | < 70% → -10%; < 80% → -5%; > 90% → +10% |
