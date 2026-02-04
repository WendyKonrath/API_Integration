import pandas as pd
import os
import re
import warnings
import requests

# Suprimir avisos de compatibilidade futura do pandas para manter o log limpo
warnings.simplefilter(action='ignore', category=FutureWarning)

# Configurações de Caminhos
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PASTA_EXTRAIDOS = os.path.join(BASE_DIR, "trimestres_baixados", "trimestres_extraidos")
PASTA_CADOP = os.path.join(BASE_DIR, "relatorio_cadop")
ARQUIVO_CADOP = os.path.join(PASTA_CADOP, "Relatorio_cadop.csv")
ARQUIVO_SAIDA_CSV = "consolidado_despesas.csv"

URL_CADOP = "https://dadosabertos.ans.gov.br/FTP/PDA/operadoras_de_plano_de_saude_ativas/Relatorio_cadop.csv"

# Colunas possíveis para normalização
COLS_DESCRICAO = ['DESCRICAO', 'DESC', 'EVENTO', 'HISTORICO', 'DETALHES', 'OBSERVACAO', 'CONTA']
COLS_REG_ANS = ['REG_ANS', 'REGISTRO', 'CODIGO', 'OPERADORA', 'CD_OPS']
COLS_VALOR_FINAL = ['VL_SALDO_FINAL', 'SALDO_FINAL', 'VALOR', 'VL_SALDO', 'SALDO']
COLS_VALOR_INICIAL = ['VL_SALDO_INICIAL', 'SALDO_INICIAL', 'VALOR_INICIAL', 'SALDO_ANTERIOR']
COLS_DATA = ['DATA', 'DT_REF', 'DATA_REFERENCIA', 'DT_REFERENCIA', 'DT_COMPETENCIA']


def baixar_cadop():
    """Baixa o arquivo de cadastro de operadoras se ele não existir."""
    if not os.path.exists(PASTA_CADOP):
        os.makedirs(PASTA_CADOP)

    if not os.path.exists(ARQUIVO_CADOP):
        print(f"Baixando CADOP de {URL_CADOP} para garantir a consolidação...")
        try:
            # Desabilita verificação SSL temporariamente se necessário
            resp = requests.get(URL_CADOP, verify=False)
            resp.raise_for_status()

            # Salva o arquivo
            with open(ARQUIVO_CADOP, 'wb') as f:
                f.write(resp.content)
            print("Download do CADOP concluído.")
        except Exception as e:
            print(f"Erro ao baixar CADOP: {e}")
            return False
    return True


def normalizar_colunas(df):
    """Normaliza os nomes das colunas para maiúsculo e remove espaços."""
    df.columns = [str(col).upper().strip() for col in df.columns]
    return df


def identificar_coluna(df, possiveis):
    """Retorna a primeira coluna encontrada na lista de possíveis."""
    for col in df.columns:
        if col in possiveis:
            return col
    return None


def carregar_cadop():
    """
    Carrega e trata o arquivo Relatorio_cadop.csv.
    Tratamento de Inconsistência: REG_ANS duplicados.
    """
    # Garante que o arquivo existe antes de tentar ler
    if not baixar_cadop():
        print("Aviso: Não foi possível obter o CADOP. O arquivo consolidado terá CNPJs vazios.")
        return None

    print("Carregando CADOP...")
    try:
        # Tenta ler com utf-8, se falhar tenta latin1
        try:
            df = pd.read_csv(ARQUIVO_CADOP, sep=';', quotechar='"', encoding='utf-8', dtype=str)
        except UnicodeDecodeError:
            df = pd.read_csv(ARQUIVO_CADOP, sep=';', quotechar='"', encoding='latin1', dtype=str)

        # Normaliza colunas
        df = normalizar_colunas(df)

        # Mapeia colunas esperadas
        col_reg = identificar_coluna(df, ['REGISTRO_OPERADORA', 'REGISTRO', 'REG_ANS'])
        col_cnpj = identificar_coluna(df, ['CNPJ'])
        col_razao = identificar_coluna(df, ['RAZAO_SOCIAL', 'RAZAOSOCIAL'])

        if not all([col_reg, col_cnpj, col_razao]):
            print("Erro: Colunas essenciais não encontradas no CADOP.")
            return None

        # Seleciona e renomeia
        df = df[[col_reg, col_cnpj, col_razao]].copy()
        df.columns = ['REG_ANS', 'CNPJ', 'RazaoSocial']

        # Limpeza do REG_ANS (remover aspas extras se houver, converter para int)
        df['REG_ANS'] = pd.to_numeric(df['REG_ANS'], errors='coerce')

        # Tratamento de Duplicatas
        duplicados = df.duplicated(subset=['REG_ANS'], keep='first').sum()
        if duplicados > 0:
            # print(f"Aviso: {duplicados} registros duplicados de REG_ANS removidos do CADOP.")
            df = df.drop_duplicates(subset=['REG_ANS'], keep='first')

        return df.set_index('REG_ANS')

    except Exception as e:
        print(f"Erro ao carregar CADOP: {e}")
        return None


def processar_arquivo_dados(caminho_arquivo, arquivo_nome):
    """
    Processa um único arquivo de dados, agregando despesas por REG_ANS.
    """
    # Extração de Trimestre e Ano do nome do arquivo
    trimestre = None
    ano = None
    match = re.search(r"(\d)T(\d{4})", arquivo_nome, re.IGNORECASE)
    if match:
        trimestre = int(match.group(1))
        ano = int(match.group(2))

    try:
        # Tenta ler CSV ou Excel
        if arquivo_nome.lower().endswith(('.csv', '.txt')):
            try:
                df = pd.read_csv(caminho_arquivo, sep=';', encoding='latin1', low_memory=False)
            except:
                df = pd.read_csv(caminho_arquivo, sep=',', encoding='utf-8', low_memory=False)
        elif arquivo_nome.lower().endswith(('.xlsx', '.xls')):
            df = pd.read_excel(caminho_arquivo)
        else:
            return None

        df = normalizar_colunas(df)

        # Identifica colunas dinamicamente
        col_reg = identificar_coluna(df, COLS_REG_ANS)
        col_desc = identificar_coluna(df, COLS_DESCRICAO)
        col_final = identificar_coluna(df, COLS_VALOR_FINAL)
        col_inicial = identificar_coluna(df, COLS_VALOR_INICIAL)
        col_data = identificar_coluna(df, COLS_DATA)

        if not all([col_reg, col_final, col_desc]):
            print(f"Colunas necessárias não encontradas em {arquivo_nome}")
            return None

        # Converte REG_ANS para numérico
        df[col_reg] = pd.to_numeric(df[col_reg], errors='coerce')

        # Função auxiliar para limpar e converter valores
        def limpar_valor(serie):
            if serie.dtype == object:
                return pd.to_numeric(serie.astype(str).str.replace('.', '', regex=False).str.replace(',', '.', regex=False), errors='coerce').fillna(0)
            return pd.to_numeric(serie, errors='coerce').fillna(0)

        # Calcula o valor do movimento
        val_final = limpar_valor(df[col_final])
        val_inicial = limpar_valor(df[col_inicial]) if col_inicial else 0
        df['VALOR_MOVIMENTO'] = val_final - val_inicial

        # Tenta extrair/refinar a data usando a coluna do arquivo
        if col_data:
            try:
                datas_validas = pd.to_datetime(df[col_data], errors='coerce').dropna()
                if not datas_validas.empty:
                    data_ref = datas_validas.max()
                    ano = data_ref.year
                    trimestre = (data_ref.month - 1) // 3 + 1
            except Exception:
                pass

        if trimestre is None or ano is None:
            print(f"Ignorando arquivo sem identificação de data: {arquivo_nome}")
            return None

        filtro_despesa = df[col_desc].astype(str).str.contains('eventos|sinistros', case=False, na=False)
        df_filtrado = df[filtro_despesa]

        if df_filtrado.empty:
            return None

        # Agrupa por REG_ANS
        agregado = df_filtrado.groupby(col_reg)['VALOR_MOVIMENTO'].sum().reset_index()
        agregado.columns = ['REG_ANS', 'ValorDespesas']

        agregado['Trimestre'] = trimestre
        agregado['Ano'] = ano

        return agregado

    except Exception as e:
        print(f"Erro ao processar {arquivo_nome}: {e}")
        return None


def main():
    print("Iniciando consolidação de despesas...")

    # 1. Carregar CADOP (Baixa se não existir)
    cadop = carregar_cadop()
    
    dados_consolidados = []

    # 2. Processar Arquivos Extraídos
    if not os.path.exists(PASTA_EXTRAIDOS):
        print(f"Pasta {PASTA_EXTRAIDOS} não encontrada.")
        print("Execute o script 1_2.py primeiro.")
        return

    arquivos = os.listdir(PASTA_EXTRAIDOS)
    print(f"Processando {len(arquivos)} arquivos...")

    for arquivo in arquivos:
        caminho = os.path.join(PASTA_EXTRAIDOS, arquivo)
        if os.path.isdir(caminho):
            continue

        df_agregado = processar_arquivo_dados(caminho, arquivo)
        if df_agregado is not None:
            dados_consolidados.append(df_agregado)

    if not dados_consolidados:
        print("Nenhum dado relevante encontrado para consolidação.")
        return

    # 3. Concatenar todos os dados
    df_final = pd.concat(dados_consolidados, ignore_index=True)

    # 4. Merge com CADOP
    if cadop is not None:
        df_final = df_final.join(cadop, on='REG_ANS', how='left')
    else:
        df_final['CNPJ'] = 'N/A'
        df_final['RazaoSocial'] = 'N/A'

    # Reordenar colunas
    colunas_finais = ['CNPJ', 'RazaoSocial', 'Trimestre', 'Ano', 'ValorDespesas']

    df_final['CNPJ'] = df_final['CNPJ'].fillna('N/A')
    df_final['RazaoSocial'] = df_final['RazaoSocial'].fillna('N/A')

    df_final = df_final[df_final['ValorDespesas'] != 0]
    df_final = df_final[colunas_finais]

    # 5. Salvar CSV
    print(f"Salvando {ARQUIVO_SAIDA_CSV}...")
    df_final.to_csv(ARQUIVO_SAIDA_CSV, index=False, sep=';', encoding='utf-8')

    print("Processo concluído com sucesso.")


if __name__ == "__main__":
    main()