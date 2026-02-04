-- 3.4. Desenvolva queries analíticas

-- ==============================================================================
-- QUERY 1: Top 5 Operadoras com Maior Crescimento Percentual de Despesas
-- ==============================================================================
-- Pergunta: Quais as 5 operadoras com maior crescimento percentual de despesas entre
-- o primeiro e o último trimestre analisado?
--
-- Desafio: Considere operadoras que podem não ter dados em todos os trimestres.
-- Solução:
--   1. Identificar dinamicamente o primeiro e o último período (Ano/Trimestre) disponíveis na base.
--   2. Filtrar apenas operadoras que possuem dados em AMBOS os períodos extremos.
--      Justificativa: O crescimento percentual exige um valor inicial e final. Se um deles for nulo
--      ou zero, o cálculo é matematicamente inválido ou infinito.
--   3. Calcular: ((Valor_Final - Valor_Inicial) / Valor_Inicial) * 100.

WITH Periodos AS (
    -- Identifica o menor e o maior período (combinando Ano e Trimestre para ordenação)
    SELECT
        MIN(ano * 10 + trimestre) as periodo_inicial_cod,
        MAX(ano * 10 + trimestre) as periodo_final_cod
    FROM despesas
),
DespesasExtremos AS (
    -- Seleciona despesas apenas dos períodos extremos
    SELECT
        d.cnpj,
        d.razao_social_informada,
        SUM(CASE WHEN (d.ano * 10 + d.trimestre) = p.periodo_inicial_cod THEN d.valor_despesas ELSE 0 END) as despesa_inicial,
        SUM(CASE WHEN (d.ano * 10 + d.trimestre) = p.periodo_final_cod THEN d.valor_despesas ELSE 0 END) as despesa_final
    FROM despesas d
    CROSS JOIN Periodos p
    WHERE (d.ano * 10 + d.trimestre) IN (p.periodo_inicial_cod, p.periodo_final_cod)
    GROUP BY d.cnpj, d.razao_social_informada
)
SELECT
    cnpj,
    razao_social_informada,
    despesa_inicial,
    despesa_final,
    ROUND(((despesa_final - despesa_inicial) / despesa_inicial) * 100, 2) as crescimento_percentual
FROM DespesasExtremos
WHERE despesa_inicial > 0 -- Evita divisão por zero e garante que existia no início
  AND despesa_final > 0   -- Garante que existe no final (opcional, dependendo da regra de negócio)
ORDER BY crescimento_percentual DESC
LIMIT 5;


-- ==============================================================================
-- QUERY 2: Distribuição de Despesas por UF
-- ==============================================================================
-- Pergunta: Liste os 5 estados com maiores despesas totais.
-- Desafio adicional: Calcule também a média de despesas por operadora em cada UF.

SELECT
    o.uf,
    SUM(d.valor_despesas) as total_despesas_uf,
    COUNT(DISTINCT d.cnpj) as qtd_operadoras,
    ROUND(SUM(d.valor_despesas) / COUNT(DISTINCT d.cnpj), 2) as media_por_operadora
FROM despesas d
JOIN operadoras o ON d.cnpj = o.cnpj
WHERE o.uf IS NOT NULL
GROUP BY o.uf
ORDER BY total_despesas_uf DESC
LIMIT 5;


-- ==============================================================================
-- QUERY 3: Operadoras com Despesas Acima da Média em >= 2 Trimestres
-- ==============================================================================
-- Pergunta: Quantas operadoras tiveram despesas acima da média geral em pelo menos
-- 2 dos 3 trimestres analisados?
--
-- Trade-off Técnico:
--   Abordagem Escolhida: CTEs com Window Functions (ou Subqueries agregadas).
--   Justificativa:
--     - Performance: Calcular a média por trimestre uma única vez.
--     - Legibilidade: Separar o cálculo da média da comparação individual.
--     - Manutenibilidade: Fácil adaptar para "N" trimestres sem criar N subqueries aninhadas.

WITH MediaPorTrimestre AS (
    -- Calcula a média geral de despesas para cada trimestre
    SELECT
        ano,
        trimestre,
        AVG(valor_despesas) as media_geral
    FROM despesas
    GROUP BY ano, trimestre
),
OperadorasAcimaMedia AS (
    -- Marca 1 se a operadora ficou acima da média naquele trimestre, 0 caso contrário
    SELECT
        d.cnpj,
        d.ano,
        d.trimestre,
        d.valor_despesas,
        m.media_geral,
        CASE WHEN d.valor_despesas > m.media_geral THEN 1 ELSE 0 END as acima_da_media
    FROM despesas d
    JOIN MediaPorTrimestre m ON d.ano = m.ano AND d.trimestre = m.trimestre
),
ContagemPorOperadora AS (
    -- Soma quantos trimestres cada operadora ficou acima da média
    SELECT
        cnpj,
        SUM(acima_da_media) as qtd_trimestres_acima
    FROM OperadorasAcimaMedia
    GROUP BY cnpj
)
-- Conta quantas operadoras satisfazem a condição (>= 2 trimestres)
SELECT
    COUNT(*) as qtd_operadoras_condicao
FROM ContagemPorOperadora
WHERE qtd_trimestres_acima >= 2;
