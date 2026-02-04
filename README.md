# Projeto de Integração e Análise de Dados da ANS

Este projeto implementa um pipeline de ponta a ponta para coleta, tratamento e análise de dados públicos da Agência Nacional de Saúde Suplementar (ANS).

## Como Executar

Os scripts devem ser executados na ordem numérica de seus prefixos.

1.  **`1_1.py`**: Baixa os arquivos ZIP dos 3 últimos trimestres da ANS.
2.  **`1_2.py`**: Extrai os arquivos ZIP e remove os que não contêm dados de despesas.
3.  **`1_3.py`**: Consolida os dados de despesas, trata inconsistências e gera `consolidado_despesas.csv`.
4.  **`2_1.py`**: Valida o arquivo consolidado, separando dados válidos e inválidos.
5.  **`2_2.py`**: Enriquece os dados válidos com informações cadastrais das operadoras.
6.  **`2_3.py`**: Agrega os dados enriquecidos e gera o arquivo `despesas_agregadas.csv`.
7.  **`3_*.sql`**: Scripts para carregar os dados em um banco de dados e realizar análises.

---

## Documentação e Decisões Técnicas (Trade-offs)

### Parte 1: Integração e Consolidação

#### 1.2. Processamento de Arquivos
*   **Trade-off (Processamento Incremental vs. Em Memória):**
    *   **Escolha:** Processamento incremental para arquivos CSV/TXT, utilizando `chunksize` da biblioteca Pandas.
    *   **Justificativa:** Os arquivos de dados da ANS podem ser muito grandes. Carregá-los inteiramente na memória (`em memória`) poderia consumir todos os recursos da máquina e falhar. O processamento incremental (`incrementalmente`) lê o arquivo em pedaços, garantindo que o uso de memória permaneça baixo e estável, tornando a solução escalável e resiliente a grandes volumes de dados.

#### 1.3. Consolidação e Análise de Inconsistências
*   **Decisão de Design (Origem do CNPJ):**
    *   Os arquivos brutos das demonstrações contábeis (trimestres) não possuem a coluna `CNPJ`, identificando as operadoras apenas pelo `Registro ANS`. Como o requisito do teste exigia explicitamente a coluna `CNPJ` no arquivo consolidado, optou-se por utilizar o `Relatorio_cadop.csv` já nesta etapa para realizar o mapeamento `Registro ANS -> CNPJ`. Essa abordagem foi escolhida em detrimento de consultas unitárias à API (que poderiam apresentar instabilidade) ou da ausência dessa informação, garantindo a integridade do dataset desde o início.
*   **Análise Crítica (Tratamento de Inconsistências):**
    *   **CNPJs duplicados com razões sociais diferentes:** A fonte primária de verdade para `RazaoSocial` e `CNPJ` foi definida como o arquivo de cadastro (`Relatorio_cadop.csv`). Os dados financeiros são agrupados pelo `REG_ANS` e, ao final, enriquecidos com os dados do cadastro, garantindo consistência.
    *   **Valores zerados ou negativos:** Valores negativos foram mantidos, pois podem representar estornos ou ajustes contábeis legítimos. Valores zerados foram removidos na etapa final de consolidação para evitar poluir o dataset com registros sem informação financeira relevante.
    *   **Formatos de data inconsistentes:** O script adota uma abordagem dupla: primeiro, tenta extrair `Ano` e `Trimestre` do nome do arquivo (ex: `1T2023.zip`). Se isso falhar, ele busca por uma coluna de data dentro do próprio arquivo para determinar o período, garantindo maior robustez.

### Parte 2: Transformação e Validação

#### 2.1. Validação de Dados
*   **Trade-off (Tratamento de CNPJs Inválidos):**
    *   **Escolha:** Estratégia de "Quarentena".
    *   **Implementação:** Os registros que falham em qualquer uma das validações (CNPJ, Razão Social ou valor) são movidos para um arquivo separado (`relatorio_inconsistencias.csv`) com uma coluna adicional explicando o motivo da falha. Os registros válidos prosseguem no fluxo, salvos em `consolidado_validado.csv`.
    *   **Prós:** Não há perda de dados; a equipe de negócio ou dados pode analisar as inconsistências e decidir como corrigi-las na origem. O fluxo principal processa apenas dados de alta qualidade.
    *   **Contras:** Exige um processo manual ou semiautomático para lidar com os dados em quarentena.

#### 2.2. Enriquecimento de Dados
*   **Análise Crítica (Tratamento de Falhas no Join):**
    *   **Registros sem match no cadastro:** Foi utilizado um `LEFT JOIN` a partir dos dados de despesas. Isso garante que, mesmo que uma operadora não seja encontrada no arquivo de cadastro, seu dado financeiro não seja perdido. As colunas adicionais (`RegistroANS`, `Modalidade`, `UF`) são preenchidas com `N/A`.
    *   **CNPJs duplicados no cadastro:** Durante o carregamento do arquivo de cadastro, os CNPJs duplicados são removidos, mantendo-se apenas a primeira ocorrência. Isso evita a duplicação de linhas de despesa durante o join.
*   **Trade-off (Estratégia de Join):**
    *   **Escolha:** Processamento em memória com Pandas.
    *   **Justificativa:** O volume de dados agregado (após consolidação e validação) e o arquivo de cadastro são suficientemente pequenos para caberem confortavelmente na memória da maioria das máquinas modernas. Essa abordagem é mais simples de implementar e mais rápida em execução do que alternativas baseadas em banco de dados ou processamento distribuído para este volume de dados.

#### 2.3. Agregação
*   **Trade-off (Estratégia de Ordenação):**
    *   **Escolha:** Ordenação em memória (`sort_values` do Pandas).
    *   **Justificativa:** O DataFrame agregado final (agrupado por operadora/UF) é relativamente pequeno. A ordenação em memória é extremamente eficiente para este cenário e não requer a complexidade de uma solução de ordenação externa (external sort).

### Parte 3: Banco de Dados e Análise

#### 3.2. Estrutura do Banco (DDL)
*   **Trade-off (Normalização):**
    *   **Escolha:** Opção B (Tabelas Normalizadas).
    *   **Justificativa:** Separar `operadoras` (dados cadastrais) de `despesas` (dados transacionais) reduz a redundância, economiza espaço e melhora a integridade. Atualizar a UF de uma operadora, por exemplo, exige a modificação de uma única linha na tabela `operadoras`, em vez de milhões de linhas na tabela de despesas.
*   **Trade-off (Tipos de Dados):**
    *   **Valores Monetários:** `DECIMAL(18,2)` foi escolhido sobre `FLOAT` para evitar erros de arredondamento inerentes a tipos de ponto flutuante, garantindo a precisão exigida para dados financeiros.
    *   **Datas:** `DATE` foi escolhido sobre `VARCHAR` para permitir o uso de funções de data do SQL e garantir a ordenação correta. `TIMESTAMP` não foi necessário, pois não há informação de hora/minuto/segundo.

#### 3.3. Importação de Dados (CSV)
*   **Análise Crítica (Inconsistências na Importação):**
    *   O script `LOAD DATA` utiliza funções como `NULLIF` para tratar campos vazios, `REGEXP_REPLACE` para limpar strings em campos numéricos (como CNPJ) e `STR_TO_DATE` para converter formatos de data, garantindo uma carga de dados limpa e padronizada.

#### 3.4. Queries Analíticas
*   **Query 1 (Crescimento Percentual):** A query considera apenas operadoras com dados no primeiro e no último trimestre para garantir que o cálculo de crescimento seja matematicamente válido.
*   **Query 3 (Operadoras Acima da Média):** A abordagem com `CTEs` (Common Table Expressions) foi escolhida por sua legibilidade e performance. A média de cada trimestre é calculada uma única vez e reutilizada, o que é mais eficiente do que subqueries correlacionadas.
