# Funcionalidades potenciais do simulador — backlog inventariado

> Inventário de funcionalidades que poderiam ser adicionadas ao simulador de pricing dinâmico, organizadas por tema. Não é roadmap — é menu pra consultar quando for priorizar próximos sprints.
>
> **Data:** abril/2026.
> **Estado atual do simulador:** MVP funcional cobrindo o "arroz com feijão" (5 matrizes Pb→Pi→D, 5 tipos de regra com CRUD, 3 views, heatmaps com diff, day_totals em R$, auditoria, fluxo de publicação no Guesty mock).
> **Comparativo:** equivalente ao PriceLabs em estágio inicial.
> **Para evoluir:** ver as categorias abaixo.

---

## 1. Inteligência sobre os preços

| Funcionalidade | O que faz |
|---|---|
| **"Por que esse preço?"** | Click numa célula da matriz → painel mostra decomposição: `Pb=R$300 + saz(+10%) + dow(+5%) + ocup(−3%) = R$348`. Hoje a UI mostra o resultado mas não explica |
| **Anomaly detection** | Marca células onde o preço foge muito do padrão da unidade (ex: 3 desvios da mediana). Pega bug em regra antes de publicar errado |
| **Top movers** | Tabela "5 maiores deltas em R$" e "5 maiores em %". Útil pra debugar regras barulhentas |
| **Sandbox what-if** | Edita regra → vê delta no `d` antes de salvar. Não toca no estado publicado |
| **Backtesting** | "Se este motor estivesse aplicado nos últimos 6 meses, qual seria o impacto?". Compara `d` simulado contra preço real cobrado |
| **A/B testing de regras** | Ativa regra X em metade do portfólio, compara performance contra controle |

## 2. Análise temporal

| Funcionalidade | O que faz |
|---|---|
| **Gráfico de linha por unidade** | "Como o preço de Apt 5 evoluiu nos últimos 90 dias e como projeta nos próximos 90?" Hoje só temos heatmap |
| **Booking pace tracker** | Curva de pickup: quão rápido cada data está enchendo vs período anterior. Sinal mais útil pra ajustar regras na hora certa |
| **Pickup analysis** | "Quantas reservas novas vieram nos últimos 7 dias por dia futuro?". Mostra onde está caindo a demanda |
| **Forecast de receita** | Não só preço, mas **receita esperada** = preço × probabilidade de venda |
| **Demand calendar** | View dedicada de "o que está vindo": eventos, feriados, datas com histórico de alta demanda |

## 3. Tipos de regras adicionais

| Regra | Caso de uso |
|---|---|
| **Gap filler / orphan night** | Detecta noite isolada entre 2 reservas e baixa preço agressivo (ou força min stay). Tipo de regra que dá "ROI imediato visível" |
| **Last-minute específico** | Regra dedicada pras últimas 48h, com lógica diferente da antecedência geral |
| **Length-of-stay** | Desconto progressivo por estadia longa (Guesty já tem `weeklyPriceFactor`/`monthlyPriceFactor` — podemos ler/escrever) |
| **Multi-channel pricing** | Preço diferente por canal: Booking +5% (cobre comissão), direto −3% (incentiva) |
| **Pickup-based adjustment** | Sobe preço quando reservas chegam mais rápido que esperado (sinal de demanda) |
| **Per-bedroom dynamics** | Apt 1Q vs 2Q têm dinâmicas diferentes; agrupamento por capacidade |

## 4. Operacional ao redor do preço

| Funcionalidade | Valor |
|---|---|
| **Cadastro de manutenções programadas** | "Apt 12B sai do estoque de 15-25/jul pra reforma". Sai da matriz, sai do push |
| **Bloqueios em massa** | "Bloquear 25/dez em todas as 233 unidades" em 1 click |
| **Calendário consolidado** | Visão 233 unidades × dias: identificar buracos, vagas, blocos. "Onde tenho mais inventário disponível pra daqui 30 dias?" |
| **Owner override visualização** | "Owner reservou Apt 8 de 10-15/mai (uso pessoal). Você perdeu R$ 1.800 de receita potencial nessa janela" |

## 5. Inteligência de mercado

| Funcionalidade | O que faz |
|---|---|
| **Comparação com concorrente** | Meu preço vs P50 do mercado pra mesmos critérios (região, capacidade, segmento). Integra com AirDNA, AirROI ou scraping próprio |
| **Detecção automática de eventos** | Lê Google Calendar público, calendário oficial da cidade, agenda de estádio/casa de show — sugere ajuste sem precisar cadastrar manual |
| **Índice de preço de mercado** | "Berrini está R$ 50 acima do mercado, Itaim Bibi R$ 30 abaixo" |
| **Cancellation prediction** | Detecta reservas com sinais de risco de cancelar (canal, lead time, histórico do hóspede) |

## 6. Métricas e KPIs

| Funcionalidade | O que mostra |
|---|---|
| **Dashboard executivo** | Página inicial com: receita prevista vs realizada, ocupação média, ADR, RevPAR, comparado a semana/mês passado |
| **Goals por região** | "Meta de ocupação 85% em Berrini". Sistema mostra desvio |
| **Performance por gerente** | Multi-tenant interno: cada gerente vê sua carteira. KPIs específicos |
| **Relatório PDF mensal** | Auto-gerado, com gráficos, pronto pra mandar ao investidor / dono do apto |

## 7. Workflow e governança

| Funcionalidade | Valor |
|---|---|
| **Snapshots + diff entre publicações** | Cada publicação no Guesty fica salva. Permite "reverter pra versão de ontem" se algo deu errado |
| **Aprovação humana pra deltas grandes** | Workflow: se `d` mudou >R$ 100 ou >25% vs último publicado, exige aprovação manual |
| **Versionamento de regras** | "Quem mudou essa regra, quando, e qual era o valor anterior?" — auditoria já tem parte disso, mas pode ficar mais navegável |
| **Notes/comentários por regra** | "Adicionei +20% no dia 25/dez por causa de ABC" |

## 8. Notificações e alertas

| Funcionalidade | Valor |
|---|---|
| **Alertas inteligentes** | "Berrini caiu 8% de ocupação esta semana", "Você tem 12 unidades com preço travado há +14 dias" |
| **Push por email/Slack/WhatsApp** | Pra time operacional não precisar checar o sistema toda hora |
| **Alerta de regra inativa** | "Regra X não foi aplicada em nenhuma célula nas últimas 30 publicações — está obsoleta?" |

## 9. Export e integração

| Funcionalidade | Valor |
|---|---|
| **Export CSV/Excel das matrizes** | Cliente exporta pra mandar pra owner ou pra contabilidade |
| **API pública pra leitura do `d`** | Outros sistemas (BI, ERP do cliente) consomem o preço |
| **Webhooks de saída** | "Quando preço de X mudar, avisar este endpoint" — útil pra integrações que clientes farão |

---

## 10. Avaliação honesta — está OK?

### Pra apresentação de demo / vendas
**Sim, está OK.** Mostra motor funcional + regras editáveis + fluxo de publicação + auditoria. Cliente entende o conceito e potencial.

### Pra cliente usar no dia-a-dia operacional
**Faltam pelo menos 3 coisas críticas**:

1. **"Por que esse preço?"** — sem isso, quando algo der estranho, ninguém sabe debugar
2. **What-if sandbox** — pricing manager precisa testar mudança antes de aplicar. Hoje a única forma é editar e ver o efeito (ou seja, aplica de verdade)
3. **Booking pace** — é o sinal mais acionável. Sem isso, regras de ocupação são reativas, não preditivas

### Pra ser produto vendável (vs PriceLabs / Wheelhouse / Buoy)
**Faltam ainda 3+ pra competir**:

4. **Modelo de expectativa real** (substituir a fórmula sintética hardcoded)
5. **Eventos automáticos** (eliminar trabalho manual de cadastrar feriado)
6. **Comparação com mercado** (cliente pergunta "estou caro ou barato?" — hoje não respondemos)

---

## 11. Cortes alternativos para priorização

### Por persona

#### Pricing Manager (uso diário)
- "Por que esse preço?" + decomposição
- What-if sandbox
- Top movers + anomalia
- Booking pace
- Backtesting
- Notes/comentários por regra

#### Gerente Regional (semanal)
- Dashboard executivo (KPIs)
- Goals por região
- Alertas inteligentes
- Performance por carteira
- Comparação com mercado externo
- Relatório PDF mensal

#### C-level / Investidor (mensal)
- Dashboard executivo
- Forecast de receita
- Relatório PDF mensal
- Comparação com mercado

#### Time operacional (quando algo dá problema)
- Snapshot + rollback
- Aprovação humana pra deltas grandes
- Auditoria navegável
- Bloqueios em massa
- Cadastro de manutenções

### Por estágio do produto

#### MVP (já tem)
- 5 matrizes (Pb, Pi, Oc.esperada, Oc.real, D)
- 5 tipos de regra com CRUD
- Heatmap com diff
- 3 views (unidade/predio/regiao)
- Auditoria
- Publicação mock

#### Production-ready (próximo passo crítico)
- Login + roles
- Guardrails de preço (min/max + delta diário)
- Snapshots + diff entre publicações
- Aprovação humana pra deltas grandes
- Modelo de expectativa real (não sintético)
- Testes automatizados

#### Diferenciação competitiva
- Eventos automáticos
- Multi-channel pricing
- Comparação com mercado externo
- Modelo de demanda preditivo
- Gap filler / orphan night
- Cancellation prediction

#### Escala / multi-cliente
- Multi-tenant com permissões
- Performance benchmarks
- API pública pra leitura
- Webhooks de saída
- Sandbox de teste pra cada cliente

---

## 12. Funcionalidades que NÃO recomendo (agora)

| Item | Por quê |
|---|---|
| **ML / modelo preditivo sofisticado** | Bonito, mas até ter histórico real de muitos clientes, não compensa o esforço. Regressão simples cobre 80% dos casos |
| **Mapa geográfico** | Bonito, mas pricing é decisão tabular. Mapa não acrescenta info acionável |
| **Modo apresentação / dark mode** | UX polish demais sem demanda real |
| **Gamification** | Não cabe em produto operacional sério |
| **Chat / IA conversacional** | Hype atual. Não resolve problema concreto até ter base de dados grande pra treinar |

---

## 13. Próximas perguntas pra revisitar

Antes de transformar isso em backlog priorizado:

1. **Qual feedback teve do cliente Cyclinn na demo?** Algum problema específico que ele apontou e a gente cobriu mal?
2. **Existe alguma funcionalidade que faria a diferença entre "ele quer pagar" vs "achou legal"?**
3. **Tem alguma das funcionalidades acima que parece essencial pro próximo cliente também (hotel, parking, coworking)?** Se sim, vale construir genérica desde já.
4. **Quanto tempo até precisar mostrar pro 2º cliente?** Define se foca em production-ready (cliente atual usar) ou em diferenciação (vender pro próximo).

---

## 14. Documentos relacionados

- [analise_regras.md](analise_regras.md) — análise do PDF original e ambiguidades
- [analise_integracao_guesty.md](analise_integracao_guesty.md) — visão arquitetural da integração
- [api_guesty_estudo.md](api_guesty_estudo.md) — endpoint a endpoint do Guesty
- [analise_expansao_verticais.md](analise_expansao_verticais.md) — outras verticais (hotelaria, parking, coworking, etc.)
