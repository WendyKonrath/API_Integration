-- 3.2. Crie queries DDL para estruturar as tabelas necessárias

-- ==============================================================================
-- JUSTIFICATIVA TÉCNICA - MODELAGEM E TIPOS DE DADOS
-- ==============================================================================

-- 1. Normalização (Opção B: Tabelas Normalizadas)
-- ------------------------------------------------------------------------------
-- Escolha: Tabelas separadas para 'operadoras' e 'despesas'.
-- Justificativa:
--   - Integridade: Dados cadastrais (endereço, modalidade) são atributos da entidade Operadora,
--     não do evento de Despesa. Alterações cadastrais não devem exigir update em milhões de linhas de despesas.
--   - Eficiência: O arquivo de despesas cresce trimestralmente. Manter dados cadastrais repetidos
--     aumentaria desnecessariamente o armazenamento e I/O.
--   - Flexibilidade: Permite que uma operadora exista no cadastro mesmo sem despesas no período,
--     e facilita a inclusão de novas métricas financeiras sem alterar a estrutura cadastral.

-- 2. Tipos de Dados
-- ------------------------------------------------------------------------------
--   - Valores Monetários: DECIMAL(18,2)
--     Justificativa: Tipos de ponto flutuante (FLOAT/DOUBLE) introduzem erros de precisão em cálculos
--     financeiros (ex: soma de centavos). DECIMAL armazena o valor exato.
--
--   - Datas: DATE
--     Justificativa: Strings (VARCHAR) impedem o uso eficiente de índices e funções temporais
--     (DATEDIFF, EXTRACT). TIMESTAMP seria excessivo pois não precisamos de precisão de segundos/fuso
--     para dados trimestrais/diários de cadastro.

-- ==============================================================================
-- DDL - CRIAÇÃO DAS TABELAS
-- ==============================================================================

-- Tabela de Operadoras (Dados Cadastrais - Origem: Relatorio_cadop.csv)
CREATE TABLE IF NOT EXISTS operadoras (
    registro_ans VARCHAR(20) NOT NULL,
    cnpj VARCHAR(14) NOT NULL, -- Apenas números
    razao_social VARCHAR(255),
    nome_fantasia VARCHAR(255),
    modalidade VARCHAR(100),
    logradouro VARCHAR(255),
    numero VARCHAR(50),
    complemento VARCHAR(255),
    bairro VARCHAR(100),
    cidade VARCHAR(100),
    uf CHAR(2),
    cep VARCHAR(20),
    ddd VARCHAR(5),
    telefone VARCHAR(20),
    fax VARCHAR(20),
    endereco_eletronico VARCHAR(255),
    representante VARCHAR(255),
    cargo_representante VARCHAR(100),
    regiao_comercializacao VARCHAR(50),
    data_registro_ans DATE,

    PRIMARY KEY (registro_ans),
    -- Índice para garantir unicidade do CNPJ e performance no Join
    UNIQUE INDEX idx_operadoras_cnpj (cnpj),
    INDEX idx_operadoras_uf (uf)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Tabela de Despesas Consolidadas (Origem: consolidado_despesas.csv)
CREATE TABLE IF NOT EXISTS despesas (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    cnpj VARCHAR(14) NOT NULL,
    razao_social_informada VARCHAR(255), -- Pode divergir do cadastro oficial
    trimestre INT NOT NULL,
    ano INT NOT NULL,
    valor_despesas DECIMAL(18,2),

    -- Chave estrangeira para integridade (opcional dependendo da qualidade dos dados)
    -- CONSTRAINT fk_despesas_operadora FOREIGN KEY (cnpj) REFERENCES operadoras(cnpj),

    INDEX idx_despesas_cnpj (cnpj),
    INDEX idx_despesas_periodo (ano, trimestre)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Tabela de Despesas Agregadas (Origem: despesas_agregadas.csv)
-- Esta tabela serve como um Data Mart ou tabela de resultados pré-calculados
CREATE TABLE IF NOT EXISTS despesas_agregadas (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    razao_social VARCHAR(255),
    uf CHAR(2),
    total_despesas DECIMAL(18,2),
    media_trimestral DECIMAL(18,2),
    desvio_padrao DECIMAL(18,2),

    INDEX idx_agregadas_uf (uf)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
