-- 3.3. Elabore queries para importar o conteúdo dos arquivos CSV

-- ==============================================================================
-- ESTRATÉGIA DE IMPORTAÇÃO E TRATAMENTO DE DADOS
-- ==============================================================================
--
-- 1. Encoding:
--    Os arquivos foram gerados em UTF-8 nos passos anteriores (Python).
--    O comando LOAD DATA INFILE deve especificar CHARACTER SET 'utf8mb4'.
--
-- 2. Tratamento de Inconsistências (Análise Crítica):
--    a) Valores NULL em campos obrigatórios:
--       - Abordagem: Utilizar variáveis de sessão (@var) para verificar e substituir por valores padrão
--         ou rejeitar a linha se for chave primária.
--    b) Strings em campos numéricos:
--       - Abordagem: O Python já fez uma pré-limpeza (ex: limpar_cnpj). No SQL,
--         usamos REPLACE para garantir remoção de aspas ou separadores residuais.
--    c) Datas em formatos inconsistentes:
--       - Abordagem: STR_TO_DATE para converter formatos brasileiros (DD/MM/YYYY) ou ISO.
--
-- 3. Caminhos dos Arquivos:
--    Assumindo que os arquivos estão acessíveis ao servidor de banco de dados.
--    Em um ambiente real, seria necessário o caminho absoluto do servidor.

-- ==============================================================================
-- IMPORTAÇÃO: OPERADORAS (Relatorio_cadop.csv)
-- ==============================================================================

LOAD DATA INFILE '/var/lib/mysql-files/Relatorio_cadop.csv'
INTO TABLE operadoras
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ';'
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES -- Ignora cabeçalho
(
    @registro_ans,
    @cnpj,
    @razao_social,
    @nome_fantasia,
    @modalidade,
    @logradouro,
    @numero,
    @complemento,
    @bairro,
    @cidade,
    @uf,
    @cep,
    @ddd,
    @telefone,
    @fax,
    @endereco_eletronico,
    @representante,
    @cargo_representante,
    @regiao_comercializacao,
    @data_registro_ans
)
SET
    registro_ans = NULLIF(@registro_ans, ''),
    -- Remove pontuação do CNPJ para padronizar
    cnpj = REGEXP_REPLACE(@cnpj, '[^0-9]', ''),
    razao_social = NULLIF(@razao_social, ''),
    nome_fantasia = NULLIF(@nome_fantasia, ''),
    modalidade = NULLIF(@modalidade, ''),
    logradouro = NULLIF(@logradouro, ''),
    numero = NULLIF(@numero, ''),
    complemento = NULLIF(@complemento, ''),
    bairro = NULLIF(@bairro, ''),
    cidade = NULLIF(@cidade, ''),
    uf = NULLIF(@uf, ''),
    cep = NULLIF(@cep, ''),
    ddd = NULLIF(@ddd, ''),
    telefone = NULLIF(@telefone, ''),
    fax = NULLIF(@fax, ''),
    endereco_eletronico = NULLIF(@endereco_eletronico, ''),
    representante = NULLIF(@representante, ''),
    cargo_representante = NULLIF(@cargo_representante, ''),
    regiao_comercializacao = NULLIF(@regiao_comercializacao, ''),
    -- Converte data YYYY-MM-DD (padrão do CSV gerado) para DATE
    data_registro_ans = STR_TO_DATE(NULLIF(@data_registro_ans, ''), '%Y-%m-%d');


-- ==============================================================================
-- IMPORTAÇÃO: DESPESAS CONSOLIDADAS (consolidado_despesas.csv)
-- ==============================================================================

LOAD DATA INFILE '/var/lib/mysql-files/consolidado_despesas.csv'
INTO TABLE despesas
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ';'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(
    @cnpj,
    @razao_social,
    @trimestre,
    @ano,
    @valor_despesas
)
SET
    cnpj = REGEXP_REPLACE(@cnpj, '[^0-9]', ''),
    razao_social_informada = NULLIF(@razao_social, ''),
    trimestre = CAST(@trimestre AS UNSIGNED),
    ano = CAST(@ano AS UNSIGNED),
    -- Garante que valor numérico seja interpretado corretamente
    valor_despesas = CAST(NULLIF(@valor_despesas, '') AS DECIMAL(18,2));


-- ==============================================================================
-- IMPORTAÇÃO: DESPESAS AGREGADAS (despesas_agregadas.csv)
-- ==============================================================================

LOAD DATA INFILE '/var/lib/mysql-files/despesas_agregadas.csv'
INTO TABLE despesas_agregadas
CHARACTER SET utf8mb4
FIELDS TERMINATED BY ';'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(
    @razao_social,
    @uf,
    @total_despesas,
    @media_trimestral,
    @desvio_padrao
)
SET
    razao_social = NULLIF(@razao_social, ''),
    uf = NULLIF(@uf, ''),
    total_despesas = CAST(NULLIF(@total_despesas, '') AS DECIMAL(18,2)),
    media_trimestral = CAST(NULLIF(@media_trimestral, '') AS DECIMAL(18,2)),
    desvio_padrao = CAST(NULLIF(@desvio_padrao, '') AS DECIMAL(18,2));
