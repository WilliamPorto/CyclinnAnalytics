# Análise prévia — Integração com Guesty

> Avaliação técnica da viabilidade de integrar o motor de pricing ao Guesty (PMS para aluguel de curta estadia, usado pela Cyclinn como ERP).
> Baseada na documentação pública da Guesty Open API em abril/2026. Precede o início da implementação.

---

## 1. Resumo executivo

Integração é **viável**, sem surpresa arquitetural. O Guesty é um PMS maduro e expõe uma API REST (OAuth2) que cobre os três fluxos necessários para um motor de pricing externo: **ler listings**, **ler reservas**, **empurrar preços diários**. Inclusive já é um caso de uso reconhecido pela plataforma — eles integram com PriceLabs e têm um motor próprio (PriceOptimizer).

Os três pontos de atrito reais são:

1. **Rate limit apertado** (5.000 req/h) — resolve com fila + agrupamento de dias contíguos.
2. **Reconciliação com edição manual** — resolve com regra de produto ("quem é dono do preço?").
3. **Multi-tenant por OAuth** — resolve desde o schema inicial (token por cliente).

Nenhum é bloqueador.

---

## 2. Mapeamento: modelo atual × Guesty

| Nosso modelo | Guesty | Dificuldade |
|---|---|---|
| `unidades` / `pb` | `listings` + `basePrice` | Baixa — `GET /listings` traz preço base, capacidade, minNights |
| `reserva_diarias` | `reservations` + webhook `reservation.new`/`reservation.updated` | Baixa — endpoint REST + webhook em tempo real |
| `ocupacao_portfolio` | derivada de `reservations` | Baixa — agregação continua do lado de cá |
| `d` (preço final) | `PUT /availability-pricing/api/calendar/listings/{id}` | **Média** — ver §4.1 |
| `expectativa_portfolio` | (não tem equivalente) | Fica do nosso lado — histórico + modelo |
| `regras_priori` / `regras_posteriori` | (não tem equivalente) | Lógica interna, não precisa viajar pro Guesty |

A tradução é direta: `listing_id` do Guesty vira `unidade_id`; `basePrice` entra em `pb`; `reservations` alimenta `reserva_diarias`; `d` sai pelo PUT do calendário.

---

## 3. Endpoints relevantes

### Autenticação
- **OAuth 2.0** — cliente pega `access_token` + `refresh_token`. Access expira; refresh é de longa duração.
- Uma conta Guesty por cliente final → tokens precisam ser armazenados por cliente desde o dia 1.

### Leitura
| Recurso | Método | Endpoint |
|---|---|---|
| Listagens | `GET` | `/listings` |
| Calendário (preço + disponibilidade) | `GET` | `/availability-pricing/api/calendar/listings/{id}?startDate=…&endDate=…` |
| Reservas | `GET` | `/reservations` |

### Escrita (push de preços)
| Recurso | Método | Endpoint |
|---|---|---|
| Calendário de uma listagem | `PUT` | `/availability-pricing/api/calendar/listings/{id}` |
| Calendário de múltiplas listagens | `PUT` | `/availability-pricing/api/calendar/listings` |

Campos do body:
- **Obrigatórios**: `startDate`, `endDate` (YYYY-MM-DD).
- **Opcionais**: `price`, `status` (`available`/`unavailable`), `minNights`, `note`, `cta`/`ctd` (closed to arrival/departure), `blockReason`.
- Flags de reset: `isBasePrice`, `isBaseMinNights`.

### Webhooks
- `reservation.new` — nova reserva entra.
- `reservation.updated` — alteração / cancelamento.
- Outros (não prioritários agora): `calendar`, `messages`, `payments`, `guests`, `listings`.

Payload traz código da reserva, check-in/out, número de noites, hóspede, valores.

---

## 4. Pontos de atrito técnicos

### 4.1. PUT de calendário é por intervalo, não por dia

O body aceita **um único `price`** aplicado ao intervalo `[startDate, endDate]`. Então se cada dia tem um preço diferente — que é nosso caso normal — um mês vira N requests.

**Mitigação — run-length encoding**: antes de despachar, agrupar dias contíguos com mesmo preço. Pra regras a priori com pouca variação ajuda muito; pra ocupação real que muda todo dia, menos.

Exemplo:
```
Dias 01-03 → R$ 300 (1 request)
Dias 04-05 → R$ 320 (1 request)
Dia  06    → R$ 315 (1 request)
Dias 07-10 → R$ 300 (1 request)
```

### 4.2. Rate limit apertado

| Intervalo | Limite |
|---|---|
| 1 segundo | 15 requests |
| 1 minuto | 120 requests |
| 1 hora | **5.000 requests** |

Resposta ao estourar: `HTTP 429` + header `Retry-After` (segundos).

**Conta de guardanapo**: 30 listings × 365 dias = 10.950 updates/dia em full refresh. Cabe com agrupamento + janela de 2 horas, mas não sobra muito. Implicações:
- Fila com token bucket local (respeitar limite antes de mandar).
- Retry com backoff exponencial em 429.
- "Só empurra o que mudou" (diff contra último estado conhecido).

Headers de monitoramento: `X-RateLimit-Remaining-<intervalo>` — usar pra pausar proativamente antes de bater.

### 4.3. Moeda é do listing, não do request

O campo `price` não aceita `currency`. É sempre na moeda cadastrada no próprio listing. Se um listing estiver em USD e mandarmos 300 pensando em BRL, **não há erro** — vira US$ 300. Validação precisa ficar do nosso lado: ao conectar uma listagem, registrar a moeda e abortar push se diferente de BRL.

### 4.4. Reconciliação com edição manual

Se alguém do operacional editar o preço direto no painel Guesty, o próximo push do motor sobrescreve (ou vice-versa, dependendo da ordem).

Decisão de **produto** (não técnica) necessária antes de codar:
- **Opção A**: motor é dono soberano. Edições manuais são desencorajadas / bloqueadas.
- **Opção B**: flag por dia "travado manualmente". Motor respeita.
- **Opção C**: última escrita vence (caótico — não recomendo).

### 4.5. Multi-tenant desde o início

Cada cliente final (Cyclinn, e futuros) tem sua própria conta Guesty. Schema precisa de:

```
integracao_guesty_conta(
  conta_id, nome, access_token, refresh_token,
  expires_at, moeda_padrao, ativa, criado_em
)
```

E toda chamada à API precisa carregar `conta_id`. Se hoje só existir Cyclinn, tudo bem, mas não assumir conta única no código.

### 4.6. Fuso horário

Reservas e calendário vêm em timezone do listing. Brasil inteiro é BRT, mas Guesty é internacional — se algum dia expandir, cuidado com UTC vs BRT em `reserva_diarias.data` e `calendar.startDate`.

### 4.7. Conflito com PriceOptimizer da própria Guesty

Se o cliente tiver o PriceOptimizer do Guesty ativo, **dois motores vão brigar pelo preço**. Parte do onboarding precisa ser: desativar o PriceOptimizer por listing antes de ligar o nosso.

---

## 5. Arquitetura proposta

```
┌──────────────┐          ┌───────────────┐     ┌───────┐     ┌────────────┐
│ Motor atual  │  →  d →  │ Adapter Guesty│  →  │ Fila  │  →  │ Guesty API │
│ (pb → pi → d)│          │ (auth/retry)  │     └───┬───┘     └────────────┘
└──────────────┘          └───────────────┘         │                ↓
       ↑                          ↑                 │         rate limit /
       │                          │                 │         HTTP 429
       │                          └─────────────────┘         retry/backoff
       │
       │  ┌─────────────────┐     ┌─────────────────┐
       └──│ Recomputa d     │ ←── │ Webhook receiver│ ←── reservation.new/updated
          │ + enfileira push│     │ /webhooks/guesty│
          └─────────────────┘     └─────────────────┘
```

Módulos:
- **`integrations/guesty/`** — lib isolada. Cliente HTTP, OAuth + refresh automático, `push_calendar(listing_id, days[])` com RLE.
- **Fila de push** — tabela `push_queue` em DuckDB (não precisa de Redis/Kafka agora). Worker drena respeitando rate limit. Retry on 429.
- **Receiver de webhook** — endpoint `/webhooks/guesty/reservation` que valida assinatura, atualiza `reserva_diarias`, dispara recomputação.
- **Scheduler** — job diário de "full refresh" de janela futura (ex: próximos 180 dias) + incremental quando regra muda.

---

## 6. Roadmap de MVP de integração

Ordem importa — os primeiros passos validam a hipótese mais barata.

| # | Etapa | Esforço | O que valida |
|---|---|---|---|
| 1 | Trial/sandbox Guesty com 2-3 listings fake | 0,5d | Ambiente acessível, credenciais |
| 2 | OAuth flow + storage de token (`integracao_guesty_conta`) | 1-2d | Auth funciona end-to-end |
| 3 | **Read-only**: importar `listings` → `pb`/`unidades` e `reservations` → `reserva_diarias` | 2-3d | **Modelagem atual aguenta dados reais** (etapa mais valiosa) |
| 4 | **Push manual**: botão "empurrar preços dos próximos 7 dias" em 1 listing | 1-2d | Loop completo motor → Guesty funciona |
| 5 | Webhook receiver pra `reservation.new`/`updated` | 1-2d | Eventos em tempo real |
| 6 | Scheduler + fila pra push automático + refresh diário | 2-3d | Produção |

**Não começar por 4-6.** O valor está em **1-3**: eles expõem todos os gaps de modelagem (categorias de listing, políticas de moeda, timezones, campos que não mapeiam) antes de escrever código de escrita.

---

## 7. Perguntas a responder com o cliente antes de começar

- **Quem é dono do preço?** (ver §4.4)
- **Quantas listings, quantas contas Guesty?** (dimensiona rate limit)
- **Todos os listings estão em BRL?** (§4.3)
- **PriceOptimizer do Guesty está ativado?** (§4.7)
- **Pode mandar daily rate diferente do basePrice sem quebrar contrato com a Guesty?** (geralmente sim, mas vale confirmar)
- **Há canais de distribuição (Booking, Airbnb) conectados via Guesty?** (preço empurrado pro Guesty propaga pra eles — bom entender a cadeia)

---

## 8. Riscos não-técnicos

- **Lock-in na Guesty.** Se o cliente trocar de PMS, a camada de integração é 100% descartada. Mitigação: adapter bem isolado (`integrations/guesty/` sem vazar pro resto).
- **Mudança breaking na API.** Guesty já teve migrações (v2 → v3). Monitorar changelog.
- **PriceOptimizer comercial deles pode virar concorrência.** Nosso diferencial é regras explícitas e auditáveis (motor caixa branca vs caixa preta deles).

---

## 9. Referências

- [Guesty Open API — documentação geral](https://open-api-docs.guesty.com/)
- [PUT calendar (listing único)](https://open-api-docs.guesty.com/reference/put_availability-pricing-api-calendar-listings-id)
- [GET calendar](https://open-api-docs.guesty.com/reference/get_availability-pricing-api-calendar-listings-id)
- [Rate limits](https://open-api-docs.guesty.com/docs/rate-limits)
- [Webhooks de reservas](https://open-api-docs.guesty.com/docs/webhooks-reservations)
- [PriceOptimizer (motor próprio da Guesty)](https://help.guesty.com/hc/en-gb/articles/9359361374493-Activating-Guesty-PriceOptimizer-on-a-listing)
- [Integração PriceLabs (concorrente de referência como pricing externo)](https://help.guesty.com/hc/en-gb/articles/9358205175453-PriceLabs)
