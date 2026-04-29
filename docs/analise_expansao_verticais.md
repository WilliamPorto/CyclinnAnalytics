# Análise — Expansão do motor pra outros verticais

> Análise estratégica de quais outras verticais (além de locação curta, aviação e ônibus) podem usar o mesmo motor de pricing dinâmico. Contexto: avaliar se o produto atual da Cyclinn pode evoluir pra plataforma horizontal de revenue management.
>
> **Data da análise:** abril/2026.
> **Status:** documento de orientação estratégica, não plano de implementação. Revisitar quando houver decisão de expandir além de locação.

---

## 1. Critério de fit

Pra um vertical encaixar no motor com adaptação **mínima**, ele precisa cumprir 4 características. Quanto mais, melhor:

1. **Inventário perecível** — depois de uma data/hora, o item vale zero (ou cai muito).
2. **Capacidade limitada** — você não pode vender mais do que tem.
3. **Demanda variável** — por dia, hora, sazonalidade, evento.
4. **Preço é decisão livre** — o operador pode mudar sem regulação que trave.

Os 4 critérios juntos definem **revenue management** — disciplina acadêmica/industrial que tem 40 anos e começou na aviação. Hotelaria, parking, eventos, restaurantes premium e muitos outros vieram depois.

---

## 2. Verticais agrupados por aderência

### Tier 1 — fit excelente (quase plug-and-play)

Mesma estrutura conceitual da locação curta. Adaptação principal: nomenclatura + integração com ERP do setor.

| Vertical | Modelo | Observação |
|---|---|---|
| **Hotéis tradicionais** | quarto × diária | Idêntico à locação. ERPs BR: Omnibees, Hits, ASI, Apollus |
| **Cruzeiros** | cabine × data de embarque | Igual, com horizonte mais longo (1-2 anos) |
| **Pousadas / B&Bs** | quarto × diária | Mid-market subserved por ferramentas grandes |
| **Coworking / salas de reunião** | espaço × hora ou dia | WeWork, Cubo, Regus + nicho pequeno fragmentado |
| **Estacionamento** | vaga × hora ou dia | Multipark, Estapar (urbano e aeroporto). Yield management embrionário no BR |
| **Restaurantes premium (slot)** | mesa × turno | Tagme, Get In começando a cogitar dinamismo |
| **Cinemas** | poltrona × sessão | Cinemark/Cinépolis fazem in-house — entrada difícil |
| **Espaços de evento (casamento, festa)** | espaço × data | Mercado fragmentado, vendas super manuais |

### Tier 2 — fit bom (adaptação moderada)

Esquema parecido, mas com 1 dimensão extra ou alguma especificidade que demanda refactor pequeno.

| Vertical | O que muda |
|---|---|
| **Locação de carros** | Adiciona dimensão "categoria" (econômico, SUV, etc). Movida/Localiza dominam — mid-market e agências menores são oportunidade |
| **Aluguel de equipamentos** | Frota de itens (festas, ferramentas, ski, surf). Capacidade discreta por item |
| **Trens intermunicipais** | Rota × horário × classe. Mais estático que avião, menos que ônibus |
| **Atrações turísticas / parques** | Entrada × dia. Pricing fixo por temporada hoje |
| **Yacht / charter de barco** | Embarcação × dia. Mercado luxo |
| **Salões / spas** | Slot × profissional. Capacidade humana, não física |
| **Quadras esportivas** | Quadra × horário (golfe, tênis, soçaite). Apps tipo BookMyMatch, AppCity |
| **Ingressos de eventos** | Setor × evento. Vendas em "lotes" hoje, dinamismo raro |

### Tier 3 — fit moderado (refatoração significativa)

| Vertical | Por que custa mais |
|---|---|
| **Voos** | Dimensão extra (fare class / RBD), regras tarifárias complexas, gigantes (PROS, Sabre AirVision, Amadeus) |
| **Frete / logística** | Schema 4-5D (origem, destino, tipo de carga, modalidade) |
| **Energia (mercado livre)** | Pricing horário, regulação, lastro físico |
| **Cloud spot pricing** | Compute como inventário perecível — mas dominado por AWS/Azure/GCP |
| **Programmatic ads** | Lógica de leilão, não rule-based |

### Tier 4 — fit ruim (modelo errado)

Não cabe e não vale forçar.

| Vertical | Por que não cabe |
|---|---|
| **SaaS / streaming** | Receita recorrente, não perecível por data |
| **Aluguel longo (residencial)** | Decisão é mensal/anual, não diária |
| **Retail físico** | Pricing não é date-bound (exceto perecíveis específicos) |
| **Educação online** | Inventário não-finito (curso vende infinitamente) |

---

## 3. Recomendações estratégicas (top 4 pra expansão)

Depois de Cyclinn (locação), se a decisão for expandir pra plataforma horizontal, esses são os caminhos com melhor risco/retorno no Brasil:

### #1 — Hotelaria tradicional *(menor risco)*

- **Modelo**: idêntico à locação curta. Apenas ERPs diferentes (Omnibees, Hits, ASI).
- **Mercado BR**: ~10.000 hotéis, maioria mid-market sem ferramenta dinâmica boa.
- **Concorrência**: D-EDGE, RateGain, IDeaS — caros, focados em rede grande. Mid-market é underserved.
- **Ticket estimado**: R$ 500-3.000/mês por propriedade.
- **Risco**: PriceLabs também cobre hotelaria, então briga direta.
- **Vantagem**: aproveita 90%+ do código atual.

### #2 — Estacionamento / parking *(maior upside)*

- **Modelo**: vaga × hora. Sazonalidade extrema (dia útil vs FDS), eventos (estádios, shows).
- **Mercado BR**: yield management embrionário. SP/Rio começando.
- **Players**: Multipark, Estapar, ParkBee, JCar Park, estacionamentos de aeroporto.
- **Concorrência direta no BR**: praticamente zero. Players gringos (SpotHero, Parkopedia) não fazem dinâmica.
- **Ticket estimado**: R$ 1.000-5.000/mês por estacionamento.
- **Vantagem**: rede de estacionamentos = um cliente, muitas unidades. Multiplica bem.
- **Risco**: ciclo de venda B2B mais longo, integração com sistema de cancela.

### #3 — Coworking + salas de reunião *(B2B, bom ARPU)*

- **Modelo**: sala × hora ou dia. Variabilidade alta (manhã vs sexta tarde).
- **Mercado BR**: cresceu pós-pandemia. WeWork, Cubo, Regus + centenas de pequenos.
- **Pricing hoje**: tabelado, sem dinamismo.
- **Ticket estimado**: R$ 800-3.000/mês por unidade.
- **Diferencial possível**: integração com OfficeRnD, Nexudus (PMS de coworking).
- **Risco**: mercado mais nichado que parking ou hotelaria.

### #4 — Atrações turísticas e parques *(mercado grande, primitivo)*

- **Modelo**: ingresso × dia. Capacidade fixa do parque.
- **Mercado BR**: volume turístico gigante. Bonito, Foz, Chapada, Beto Carrero, parques aquáticos, museus.
- **Pricing hoje**: tabelado por temporada (alta/baixa). Sofisticação baixa.
- **Ticket estimado**: R$ 500-2.000/mês por atração.
- **Risco**: clientes pequenos, ciclo "do dono", ticket menor.

---

## 4. Verticais a evitar

| Vertical | Por que evitar |
|---|---|
| **Aviação comercial** | Muito caro entrar, gigantes consolidados (PROS, Sabre, Amadeus) |
| **Cinema (rede)** | Times internos resolvem. Redes pequenas não pagam |
| **Locação de carros (top players)** | Movida/Localiza/Unidas têm pricing internos. Mid-market dominado por software gringo (Carla, RentalCars Pricing) |
| **Programmatic ads** | Lógica de leilão é incompatível com rule engine |
| **Aluguel longo residencial** | Decisão é mensal, não diária — modelo não cabe |

---

## 5. O padrão que emerge

Olhando os tier-1 + os 4 recomendados, o que conecta todos eles é:

> **Vender um pedaço de tempo num pedaço de espaço, com capacidade limitada e demanda variável.**

Esse é o nome técnico: **revenue management**. Já existe como disciplina há 40 anos (começou na aviação nos anos 1980). Ferramentas acessíveis pra mid-market são raras no Brasil.

**Posicionamento sugerido pra plataforma horizontal**:

> "PriceLabs do Brasil pra qualquer inventário que expira no tempo."

Comunica imediato pra quem conhece o setor (PriceLabs é referência em short-term rental), e amplia o escopo (não é só hospedagem).

---

## 6. Estimativa de reuso de código

Pegando o que existe hoje (~3 semanas em locação), aproximada por camada:

| Camada | Reuso direto entre verticais |
|---|---|
| Motor de regras (`pi`, `d`, fatores multiplicativos) | ~80% |
| Schema de regras (sazonalidade, dow, eventos, antecedência, ocupação) | ~85% |
| Auditoria + governança | ~95% |
| UI de regras (CRUD) | ~70% |
| Heatmap visualization | ~50% (precisa virar multidimensional) |
| Schema de dados (cadastro de unidades) | ~30% |
| Integração com fonte de booking | ~10% (Guesty ≠ Sabre ≠ ClickBus ≠ Estapar) |
| Push de preço | ~10% |

**Conclusão**: core ~70% reutilizável, bordas ~20%. Adicionar novo vertical depois do core abstrato deve custar 30-40% do esforço original.

---

## 7. Mudanças arquiteturais necessárias pra plataforma

Pra virar genérico, 4 mudanças principais:

1. **`unidade_id` deixa de ser primeira-classe**.
   Vira `pricing_unit_id` com dimensões dinâmicas. Schema: `pricing_unit(id, type, dimensions JSON, capacity, ...)`. Onde `dimensions` é `{unidade_id: 5}` pra locação e `{vaga: A12, periodo: hora}` pra parking.

2. **Matriz da UI vira multidimensional**.
   Hoje é (linha = unidade, coluna = data). Genérico: usuário escolhe quais 2 dimensões plotar, com filtros pelas outras.

3. **Adapter pattern pra integração**.
   Pasta `integrations/` com `guesty/`, `omnibees/`, `multipark/`, etc., cada uma implementando uma interface `read_inventory()`, `read_bookings()`, `push_prices()`.

4. **Regras com contexto de domínio**.
   Hoje a regra "ocupação portfolio" assume região. Genérico: "ocupação do agrupamento X", onde X é configurável (rota, base, marca, prédio, etc.).

---

## 8. Estratégia recomendada

### Caminho pragmático (recomendado)

1. **Termina Cyclinn** (locação). Valida modelo de negócio, gera receita, aprende.
2. **Não abstraia agora**. Regra de produto: 3 instâncias antes de generalizar. Você tem 1.
3. **Evita decisões irreversíveis**: nomes de tabela/coluna que sejam "só locação", acoplamento profundo com Guesty no core, etc. Manter `integrations/guesty/` isolado já é uma boa prática.
4. **Quando entrar no segundo vertical**, faz refactor pra core genérico ali — com 2 casos concretos você sabe o que abstrair de verdade. Sem isso, vira "abstração que não cabe".
5. **Aviação é "caso difícil"** — se for ambicioso, aprende muito mas custa caro. Hotelaria ou parking são mais próximos do MVP atual.

### Caminho ousado (não recomendado pra agora)

Construir abstrações desde já mirando 3 verticais.

- **Pros**: arquitetura limpa desde o começo.
- **Cons**: complexidade prematura, decisões sem dados, alto risco de over-engineering.

---

## 9. Perguntas em aberto pra revisitar

Antes de tomar a decisão de expandir, vale ter resposta pra:

1. **Cyclinn aceita ser case pra outras hospedagens (multi-tenant) ou serve só eles?** Se aceita, hotelaria é a entrada natural.
2. **Tem rede pessoal em algum dos verticais sugeridos?** Acesso a primeiro cliente vale mais que análise de mercado — quem entra com cliente fechado economiza 6 meses.
3. **TAM real de cada mercado no Brasil**: hoje é estimativa, vale validar com pesquisa específica antes de comprometer roadmap.
4. **Capacidade do time de bancar 2 verticais em paralelo, ou apenas serial?** Influencia se vai por hotelaria primeiro (migração suave) ou parking (mais salto, mais learning).
