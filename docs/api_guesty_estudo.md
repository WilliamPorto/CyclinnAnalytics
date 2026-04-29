# Estudo da API do Guesty — mapeamento contra o simulador

> Estudo aprofundado da Guesty Open API pra identificar exatamente o que o simulador atual pode ler/escrever na Guesty, quais conceitos do nosso modelo têm equivalente direto, e quais precisam de adaptação.
>
> **Data do estudo:** abril/2026.
> **Complementa:** [analise_integracao_guesty.md](analise_integracao_guesty.md) (visão de arquitetura/atritos).
> **Foco aqui:** detalhamento técnico endpoint-a-endpoint.

---

## 1. Autenticação e ambiente

### 1.1. OAuth 2.0 client_credentials

- **Token endpoint**: `POST https://open-api.guesty.com/oauth2/token`
- **Grant type**: `client_credentials`
- **Scope**: `open-api`
- **Token expira em**: 24 horas
- **Limite**: máximo de **5 tokens novos por 24h por clientId** (estrela vermelha — força cache de token).
- **Estratégia recomendada**: cachear o token e renovar 30-60 minutos antes de expirar.
- **Auth header**: `Authorization: Bearer <access_token>`.

### 1.2. Como cliente final autoriza nossa plataforma

Existem **2 modelos distintos** (importante pra produto):

#### Modelo A — Cliente gera credenciais e passa pra nós
- Cliente vai em **Integrations > OAuth Applications** no painel Guesty.
- Gera Client ID + Client Secret.
- Compartilha conosco (canal seguro).
- Nós usamos essas credenciais pra acessar a conta dele.
- **Implicação**: 1 conexão = 1 par de credenciais. Multi-tenant via tabela `integracao_guesty_conta` no nosso DB.

#### Modelo B — Marketplace App (parceiro oficial)
- Cyclinn vira parceiro oficial Guesty Marketplace (aprovação manual da Guesty).
- Aparece na vitrine de integrações deles.
- Cliente "instala" o app no painel dele com 1 clique (OAuth flow padrão).
- Nós herdamos credenciais por cliente automaticamente.
- **Implicação**: muito melhor distribuição, mas exige aprovação Guesty + processo comercial.

**Recomendação pra MVP**: Modelo A (mais rápido). Migrar pra Modelo B quando tiver 5-10 clientes pra justificar parceria oficial.

### 1.3. Sandbox

- **Existe** sandbox pra Booking Engine API (URL: `booking-sandbox.guesty.com/api/`).
- **Tem custo**: pode haver "token fee" — precisa requisitar via Customer Success Manager.
- Pra Open API (pricing/calendar), **não há menção explícita de sandbox**. Provavelmente cliente precisa criar conta de teste com dados fake.
- **Atritos**: sem sandbox grátis pra Open API significa que demos precisam de uma conta Cyclinn de homologação ou cliente disposto a virar early adopter.

---

## 2. Listings — equivalente do nosso `cadastro.unidades`

### 2.1. Endpoint principal

**`GET /v1/listings`** (paginado, max 100 por página)

| Query param | Uso |
|---|---|
| `ids` | Filtrar por IDs específicos (csv) |
| `nids` | Excluir IDs |
| `viewId` | Aplicar view salva (filtros + fields + sort) |
| `q` | Search em title, internalNote, address.full |
| `city` | Filtro geográfico |
| `active` | Boolean |
| `pmsActive` | Limitar a listings com PMS ativo |
| `listed` | Listed/unlisted |
| `available` | Objeto `{checkIn, checkOut, minOccupancy}` pra disponibilidade |
| `tags` | Filtro por tags |
| `fields` | Seleção de campos (separados por espaço) |
| `sort` | Default `title` ascendente |
| `limit` | Max 100, default 25 |
| `skip` | Offset |
| `filters` | Array of objects, sintaxe `{operator, field, value}` (filtros estruturados) |

### 2.2. Campos relevantes do listing object

```json
{
  "_id": "abc123",
  "title": "Apt Berrini 12B",
  "nickname": "Berrini-12B",
  "type": "SINGLE",        // SINGLE | MTL | MTL_CHILD
  "mtl": { "p": "parent_id" },  // só em MTL_CHILD
  "active": true,
  "listed": true,
  "accommodates": 4,
  "bedrooms": 2,
  "bathrooms": 1,
  "address": {
    "full": "...",
    "city": "São Paulo",
    "country": "Brazil"
  },
  "tags": ["premium", "berrini", "investidor-x"],
  "amenities": [...],
  "prices": {
    "basePrice": 350.0,
    "basePriceUSD": 70.0,
    "currency": "BRL",
    "weekendBasePrice": 420.0,
    "weeklyPriceFactor": 0.95,    // desconto: 0.95 = 5% off
    "monthlyPriceFactor": 0.85,
    "guestsIncludedInRegularFee": 2,
    "extraPersonFee": 50.0,
    "cleaningFee": 100.0,
    "securityDepositFee": 500.0
  },
  "customFields": [...],
  "calendarRules": {...},
  "createdAt": "..."
}
```

### 2.3. Multi-unit (MTL) — importante!

Guesty distingue **3 tipos de listing**:

| Tipo | Significado |
|---|---|
| `SINGLE` | Imóvel único, sem sub-unidades. Caso típico. |
| `MTL` | "Multi-listing" pai. Representa um cluster (ex: edifício com várias unidades idênticas). |
| `MTL_CHILD` | Sub-unidade individual dentro de um MTL. Tem `mtl.p` apontando pro pai. |

**Implicação pro motor**: o conceito Cyclinn de "prédio com várias unidades" mapeia bem em MTL/MTL_CHILD. **Mas isso é decisão do cliente**: ele pode ter cadastrado cada apartamento como SINGLE separado, ou agrupado como MTL com filhos. Adapter precisa lidar com ambos.

**Allotment**: em MTL, disponibilidade é por **número de unidades disponíveis** (allotment), não pelo `status`. Quando tem 5 sub-unidades e 3 reservadas, allotment=2 (ainda dá pra reservar).

### 2.4. Mapeamento contra nosso schema

| Conceito Cyclinn | Onde no Guesty |
|---|---|
| `unidade_id` | `listing._id` |
| `codigo_externo` (label) | `listing.nickname` ou `title` |
| `predio_id` | **Não nativo.** Precisa: (a) usar custom fields, (b) usar tags, (c) usar address.street, ou (d) ignorar e listar como flat |
| `regiao_id` | **Não nativo.** Mesmo: custom fields, tags, ou address.city/neighborhood |
| `segmento_id` | **Não nativo.** Tags ou custom fields |
| `pb` (preço base) | `listing.prices.basePrice` |
| moeda | `listing.prices.currency` |
| capacidade | `listing.accommodates` |

**Decisão importante**: prédio/região/segmento **não existem nativamente** no Guesty. Vamos precisar **importar via custom fields ou tags**. Cliente que já estrutura assim no painel resolve direto; cliente que não estrutura precisa fazer setup inicial. Outra opção: cadastro paralelo nosso (vinculando `listing_id` → grupos definidos do nosso lado).

**Recomendação**: criar tabela `mapeamento_unidade(listing_id, regiao_id, predio_id, segmento_id)` no nosso DB, populada por:
1. Auto-detecção via custom fields/tags se disponível
2. UI nossa pra cliente atribuir manualmente
3. CSV upload em massa

---

## 3. Reservations — equivalente do nosso `reservas.reserva_diarias`

### 3.1. Endpoint principal

**`GET /v1/reservations`** (paginado, max 100)

Sintaxe de **filtros estruturados** (importante saber):

```
filters=[
  {"operator": "$between", "field": "checkInDateLocalized", "from": "2026-04-01", "to": "2026-12-31"},
  {"operator": "$in", "field": "status", "value": ["confirmed", "reserved"]},
  {"operator": "$eq", "field": "listingId", "value": "abc123"}
]
&sort=_id&limit=100&skip=0
```

### 3.2. Campos do reservation object

```json
{
  "_id": "res_xyz",
  "confirmationCode": "GY-4QLqteQL",
  "accountId": "...",
  "listingId": "abc123",
  "guestId": "...",
  "guestsCount": 3,
  "checkInDateLocalized": "2026-05-12",   // YYYY-MM-DD no fuso do listing
  "checkOutDateLocalized": "2026-05-15",
  "nightsCount": 3,
  "status": "confirmed",                   // confirmed | reserved | canceled | awaiting_payment
  "source": "Airbnb",                      // OTA ou website/manual
  "channel": "...",                        // similar a source mas mais granular
  "money": {
    "fareAccommodation": 1050.0,
    "fareCleaning": 100.0,
    "totalTaxes": 0,
    "subTotalPrice": 1150.0,
    "totalPaid": 1150.0,
    "totalRefunded": 0,
    "balanceDue": 0,
    "hostPayout": 920.0,
    "hostPayoutUsd": 184.0,
    "hostServiceFee": 230.0,
    "payments": [...],
    "invoiceItems": [...]
  },
  "createdAt": "...",
  "expiresAt": null
}
```

### 3.3. Status possíveis

- `confirmed` — reserva confirmada (paga ou aguardando dia)
- `reserved` — reservada (provavelmente OTA antes de finalizar)
- `canceled` — cancelada
- `awaiting_payment` — aguardando pagamento

**Pra nosso motor**: filtrar por `status IN ('confirmed', 'reserved')` pra computar ocupação real.

### 3.4. Sources/channels possíveis

Os principais:
- `Airbnb`
- `Booking.com`
- Website bookings (próprio site Cyclinn)
- `manual` (entrada manual no painel)
- Outras OTAs via integração

**Pra nosso motor**: pode ser interessante segmentar ocupação por canal — preço pode reagir diferente se 80% das reservas vêm do Booking (cliente direto) vs Airbnb (margem menor).

### 3.5. Fluxo de explosão em diárias (importante)

**Guesty não retorna `reserva_diarias`** (1 linha por noite). Retorna a reserva como objeto único com `checkIn`/`checkOut`. **Nossa pipeline precisa fazer a explosão** local — exatamente o que [scripts/load_real_data.py:328](scripts/load_real_data.py#L328) já faz hoje.

```python
for i in range(nights_count):
    rd_rows.append({
        "unidade_id": map(listingId),
        "data": checkInDateLocalized + i,
        ...
    })
```

### 3.6. Mapeamento contra nosso schema

| Conceito Cyclinn | Guesty |
|---|---|
| `reservas.reservas` | `GET /v1/reservations` |
| `reserva_diarias` | Derivado (explodir por noite) |
| `accommodation_fare` (nossa coluna) | `money.fareAccommodation` |
| `number_of_nights` | `nightsCount` |
| `status='confirmed'` | `status='confirmed'` (mesmo nome) |
| `data_checkin` | `checkInDateLocalized` |
| `canal` | `source` (mais alto-nível) ou `channel` |

---

## 4. Calendar — read e write de preço diário

### 4.1. GET — leitura

**`GET /v1/availability-pricing/api/calendar/listings/{id}?startDate=YYYY-MM-DD&endDate=YYYY-MM-DD`**

Resposta por dia:

```json
{
  "date": "2026-05-12",
  "listingId": "abc123",
  "currency": "BRL",
  "price": 387.50,
  "isBasePrice": false,           // true se está usando o basePrice do listing
  "minNights": 2,
  "isBaseMinNights": false,
  "status": "available",          // ou "unavailable"
  "blocks": {                     // múltiplos block types simultâneos
    "m": false,                   // manual block
    "r": false,                   // reserved
    "b": true,                    // booked
    "bd": false,                  // blocked by default
    "sr": false,                  // smart calendar rule
    "abl": false,                 // annual booking limit
    "a": false,                   // allotment block (MTL)
    "bw": false,                  // booking window
    "o": false,                   // owner reservation
    "pt": false,                  // preparation time
    "ic": false,                  // iCal imported
    "an": false                   // advance notice
  },
  "blockRefs": [...],             // refs específicas dos blocks
  "cta": false,                   // closed to arrival
  "ctd": false,                   // closed to departure
  "rulesApplied": [               // explicação de smart rules ativas
    {"name": "repeated days (tuesday)", "factor": 1.10},
    {"name": "duration 2 days 18%", "factor": 1.18}
  ]
}
```

**Insights importantes**:
- O Guesty **já tem motor de regras próprio** (smart rules / PriceOptimizer) que aplica fatores ao basePrice. O `price` retornado já passou por essas regras.
- `rulesApplied` mostra quais regras Guesty atuaram naquele dia. **Bom pra debug**: se nosso preço enviado for sobrescrito por uma smart rule deles, dá pra ver.
- `blocks.b` = booked (reserva ativa). Cruzar com `status` ajuda a determinar ocupação real **sem ter que ler reservations** (mais barato em rate limit, talvez).

### 4.2. PUT — escrita (single listing)

**`PUT /v1/availability-pricing/api/calendar/listings/{id}`**

Body:
```json
{
  "startDate": "2026-05-01",     // obrigatório
  "endDate": "2026-05-31",       // obrigatório
  "price": 387.50,                // moeda do listing
  "status": "available",          // available | unavailable
  "isBasePrice": false,           // true reseta pra basePrice
  "minNights": 2,
  "isBaseMinNights": false,
  "note": "atualizado por Cyclinn pricing engine",
  "cta": false,                   // closed to arrival
  "ctd": false,                   // closed to departure
  "blockReason": "Other",         // só relevante se status=unavailable
  "useChildValues": true          // MTL: aplicar nos filhos também?
}
```

**Limites confirmados**:
- Range máximo de **730 dias** (2 anos) por chamada
- O `price` é **um valor único pro range** — não array por dia
- Recomendação Guesty: **um listing por request** (mesmo tendo bulk endpoint, eles preferem)
- Reset pra base price: `isBasePrice: true` (sem precisar enviar `price`)

### 4.3. PUT — escrita (multiple listings)

**`PUT /v1/availability-pricing/api/calendar/listings`**

Existe mas Guesty **explicitamente recomenda usar single** (provavelmente por consistência transacional). Vamos usar single.

### 4.4. Block types — guia operacional

Como tratar cada block type quando estamos publicando preço:

| Code | O que é | Pricing engine deveria... |
|---|---|---|
| `m` | Bloqueio manual (operacional travou) | **Pular** — respeitar |
| `r` | Reservada (em processo) | **Pular** — preço será definido pela reserva |
| `b` | Booked | **Pular** — já vendido |
| `bd` | Blocked by default | **Pular** — owner não quer vender |
| `sr` | Smart rule block | Investigar — se for nossa regra antiga, sobrescrever |
| `abl` | Annual booking limit | **Pular** — atingiu limite |
| `a` | Allotment (MTL) | **Pular** — sub-unidades esgotadas |
| `bw` | Booking window | **Pular** — fora do horizonte de venda |
| `o` | Owner reservation | **Pular** — proprietário usando |
| `pt` | Preparation time | **Pular** — buffer entre reservas |
| `ic` | iCal imported | **Pular** — vem de outro canal |
| `an` | Advance notice | **Pular** — proteção de janela mínima |

**Política recomendada**: por default, qualquer bloco ativo → **pular o dia**. Operacional pode habilitar override só pra `sr` se for caso de migrar de PriceOptimizer pra Cyclinn.

---

## 5. Webhooks — eventos em tempo real

### 5.1. Eventos disponíveis

Apenas **2 eventos de reserva**:

- `reservation.new` — nova reserva criada/importada
- `reservation.updated` — qualquer alteração (incluindo cancelamento)

**Insight crítico**: Guesty **não tem evento separado** pra cancelamento. Cancelamento vem como `reservation.updated` com `status: canceled`. Nossa lógica precisa olhar `reservationBefore.status` vs `reservation.status`.

### 5.2. Payloads

**`reservation.new`**:
```json
{
  "event": "reservation.new",
  "reservation": { /* objeto completo */ }
}
```

**`reservation.updated`**:
```json
{
  "event": "reservation.updated",
  "reservation": { /* estado atual */ },
  "reservationBefore": { /* estado anterior */ }
}
```

### 5.3. Configuração

- **Endpoint pra registrar**: `POST /webhooks` (referenciado mas docs não detalham profundamente)
- **Não documentado** no que pesquisei: retry, timeout, HMAC signature.
- **Ação**: ao começar a integração real, validar com suporte Guesty:
  - Tem signing secret pra HMAC?
  - Quantas tentativas em caso de 5xx do nosso lado?
  - Timeout pra resposta nossa?

### 5.4. Eventos não disponíveis (mas que seriam úteis)

- `calendar.updated` — quando preço/disponibilidade muda manualmente. **Existe mas em outro doc** (Webhooks: Calendar).
- `listing.updated` — quando metadata do listing muda.
- `reservation.canceled` — explicitamente. Não tem; deduzir via update.

---

## 6. Custom fields, tags e extensibilidade

### 6.1. Custom fields

- **Endpoint**: `GET /v1/accounts/{id}/custom-fields` (lista todos os custom fields da conta) e `GET /v1/listings/{id}/custom-fields` (custom fields de um listing).
- **CRUD**: Há `POST /v1/accounts/{id}/custom-fields` pra criar.
- **Aplicáveis em**: Listings e Reservations (talvez Guests também).
- **Tipos**: text, number, boolean, date (provável — não 100% confirmado nas docs públicas).

**Caso de uso pro motor**: criar custom fields no listing tipo `cyclinn_predio_id`, `cyclinn_regiao_id`, `cyclinn_segmento_id`. Isso permite mapear nosso modelo dentro do próprio Guesty, em vez de manter tabela separada.

### 6.2. Tags

- Já documentado em listing object como array de strings.
- Filtro nativo via `tags` query param em `GET /listings`.
- Mais flexível que custom fields (não estruturado), mas menos explícito.

### 6.3. Recomendação

**Pra MVP**: usar **tags** pra mapeamento (mais simples).
- Tags: `regiao:berrini`, `predio:edificio-x`, `segmento:premium`.
- Parser nosso lê tag e popula nossa tabela de mapeamento.

**Pra v2**: migrar pra custom fields estruturados (mais robusto, melhor UI no Guesty).

---

## 7. Analytics — o que tem e o que NÃO tem

### 7.1. Dashboards no painel Guesty (não API)

Existem dashboards de Advanced Analytics no painel:
- Occupancy
- ADR (Average Daily Rate)
- RevPAR
- Pace Report (booking pace por janela: 7d, 30d, 60d, 90d)

**Importante**: pelo que pesquisei, **esses dashboards são UI-only**. Não há endpoint REST pra puxar os números agregados.

### 7.2. Implicação pro motor

Quando precisarmos calcular **expectativa** baseada em histórico (substituir a fórmula sintética atual), **vamos derivar do zero** a partir de `GET /reservations` (histórico) + `GET /listings`. Não dá pra "puxar a expectativa pronta" da Guesty.

Isso na prática:
- Nossa pipeline puxa todas as reservations dos últimos N anos
- Calcula ocupação observada por (regiao, dow, mês)
- Aplica suavização ou modelo
- Salva em `expectativa_regiao` (do nosso lado)

Mais trabalho que se eles tivessem `GET /analytics/occupancy?groupBy=...`, mas é o caminho.

---

## 8. Rate limits — confirmado

| Janela | Limite |
|---|---|
| 1 segundo | 15 requests |
| 1 minuto | 120 requests |
| 1 hora | **5.000 requests** |

**Resposta ao estourar**: HTTP 429 + header `Retry-After`.

**Headers de monitoramento** disponíveis nas respostas:
- `X-RateLimit-Limit-<intervalo>`
- `X-RateLimit-Remaining-<intervalo>`

**Conta de guardanapo pro Cyclinn** (233 unidades × 366 dias):
- Full refresh PUT calendar = 233 requests (1 por listing) por dia → ~8.000/mês.
- GET reservations diário (paginado): ~10 páginas → 10 requests/dia.
- Cabe folgado.

**Pra cliente de hotelaria com 1.000+ unidades**: pode estourar 5.000/h em full refresh. Solução: **agrupar por dias contíguos com mesmo preço** (run-length encoding) — um listing pode mandar 366 dias com 1 PUT se o preço for igual em vários dias seguidos.

---

## 9. Mapeamento completo: simulador ↔ Guesty

### 9.1. Read (importar do Guesty)

| Função do simulador | Endpoint Guesty | Periodicidade |
|---|---|---|
| Cadastro de unidades | `GET /listings` | 1x setup + sync diária (mudanças) |
| Preço base (`pb`) | `listing.prices.basePrice` | 1x setup + on listing update |
| Mapeamento prédio/região/segmento | tags ou custom fields no listing | 1x setup + manual |
| Reservas históricas | `GET /reservations?filters=[checkIn, status]` | 1x bulk + webhook reservation.new |
| Cancelamentos | `GET /reservations` + comparar status | webhook reservation.updated |
| Disponibilidade atual | `GET /calendar` (opcional, se queremos respeitar blocks) | on-demand |

### 9.2. Write (publicar no Guesty)

| Função do simulador | Endpoint Guesty | Cuidados |
|---|---|---|
| Publicar `d` (preço final) | `PUT /calendar/listings/{id}` | RLE de dias contíguos; pular dias bloqueados; rate limit |
| Publicar minNights | `PUT /calendar` com `minNights` | Geralmente fixo, raramente muda |
| Reverter pra base | `PUT /calendar` com `isBasePrice: true` | Caso especial: "parar de gerenciar X" |

### 9.3. Realtime (webhooks)

| Trigger | Webhook Guesty | Ação nossa |
|---|---|---|
| Nova reserva | `reservation.new` | Atualizar `reserva_diarias`, recomputar ocupação, recomputar `d` no horizonte afetado, enfileirar push |
| Cancelamento | `reservation.updated` (status: canceled) | Mesma coisa, mas reduz ocupação |
| Manual override de preço no painel | (não temos webhook fácil) | Polling ocasional do calendar pra detectar drift |

---

## 10. Lacunas e questões em aberto

Coisas que **não consegui confirmar** na pesquisa pública e precisam validar com Guesty antes de codar:

1. **HMAC dos webhooks** — tem? Como validar autenticidade?
2. **Retry policy de webhook** — quantas tentativas em caso de erro nosso? Timeout?
3. **Webhooks de calendar** — existe `calendar.updated`? Pra detectar override manual sem polling?
4. **Sandbox da Open API** — confirmamos só pra Booking Engine. Open API tem ambiente de teste?
5. **Token rate limit do oauth2** — 5 tokens/24h por clientId. Como funciona se temos 100 clientes (cada um com clientId diferente, suponho)?
6. **MTL — atualização de preço em pai propaga pros filhos automaticamente?** Ou precisa `useChildValues: true`?
7. **Custom fields — tipos suportados** (string/number/boolean/date) e como criar via API.
8. **Paginação de listings em conta com 1.000+ unidades** — `skip` perde performance? Tem cursor-based?

Validar essas 8 questões com **suporte técnico da Guesty** antes de comprometer arquitetura.

---

## 11. Próximos passos práticos

1. **Pedir credenciais** no Guesty (cliente Cyclinn cria Client ID + Secret no painel deles).
2. **Implementar `integrations/guesty/`** (lib isolada):
   - `client.py` — auth (cache de token), rate limit, retry/backoff
   - `listings.py` — read + map pra nosso schema
   - `reservations.py` — read + paginate + explode em diárias
   - `calendar.py` — read + write
   - `webhooks.py` — receiver + dispatcher
3. **Endpoint nosso `POST /integracao/guesty/sincronizar-listings`** (one-shot):
   - Lê todas as listings com paginação
   - Atualiza `cadastro.unidades` e `preco_base.precos_base`
   - Mapeia tags → `regiao_id`, `predio_id`, `segmento_id`
   - Reporta divergências (listings sem mapeamento, etc.)
4. **Endpoint `POST /integracao/guesty/sincronizar-reservas`** (one-shot bulk + delta):
   - Bulk inicial: lê reservations dos últimos 24 meses
   - Delta diária: usa filter por `lastUpdatedAt` pra incrementais
   - Popula `reserva_diarias` e dispara recomputação
5. **Webhook receiver**: substitui o mock de "Publicar no Guesty" por envio real, mantendo a UI igual (nada muda pro usuário final).
6. **Validar 8 lacunas** (§10) com suporte Guesty antes da etapa 2.

---

## 12. Referências

- [Guesty Open API — root](https://open-api-docs.guesty.com/)
- [Authentication](https://open-api-docs.guesty.com/docs/authentication)
- [Quick start guide](https://open-api-docs.guesty.com/docs/quick-start-guide)
- [Searching listings](https://open-api-docs.guesty.com/docs/searching-for-available-listings-and-all-listings)
- [Listing financials](https://open-api-docs.guesty.com/docs/listing-financials)
- [How to search reservations](https://open-api-docs.guesty.com/docs/how-to-search-for-reservations)
- [Calendar block types](https://open-api-docs.guesty.com/docs/calendar-block-types)
- [GET calendar (single)](https://open-api-docs.guesty.com/reference/get_availability-pricing-api-calendar-listings-id)
- [PUT calendar (single)](https://open-api-docs.guesty.com/reference/put_availability-pricing-api-calendar-listings-id)
- [PUT calendar (multiple)](https://open-api-docs.guesty.com/reference/put_availability-pricing-api-calendar-listings)
- [Webhooks: Reservations](https://open-api-docs.guesty.com/docs/webhooks-reservations)
- [Webhooks: Calendar](https://open-api-docs.guesty.com/docs/webhooks-calendar)
- [Custom fields per account](https://open-api-docs.guesty.com/reference/get_accounts-id-custom-fields)
- [Marketplace partners (revenue management)](https://www.guesty.com/blog/january-marketplace-integrations-spotlight-revenue-management/) — concorrentes diretos: Wheelhouse, PriceLabs, Buoy, RoomPriceGenie
- [Sandbox (Booking Engine)](https://booking-api-docs.guesty.com/docs/api-sandbox-environment)
- [Marketplace partners API key](https://help.guesty.com/hc/en-gb/articles/9371576143389-Guesty-API-key-for-Marketplace-partners)
