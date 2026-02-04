import pandas as pd
import os
import re
import requests

# Configurações de Caminhos
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ARQUIVO_DADOS_VALIDADOS = os.path.join(BASE_DIR, "consolidado_validado.csv")
PASTA_CADOP = os.path.join(BASE_DIR, "relatorio_cadop")
ARQUIVO_CADOP = os.path.join(PASTA_CADOP, "Relatorio_cadop.csv")
ARQUIVO_SAIDA = os.path.join(BASE_DIR, "consolidado_enriquecido.csv")

URL_CADOP = "https://dadosabertos.ans.gov.br/FTP/PDA/operadoras_de_plano_de_saude_ativas/Relatorio_cadop.csv"


def baixar_cadop():
    """Baixa o arquivo de cadastro de operadoras se ele não existir."""
    if not os.path.exists(PASTA_CADOP):
        os.makedirs(PASTA_CADOP)
    
    if not os.path.exists(ARQUIVO_CADOP):
        print(f"Baixando CADOP de {URL_CADOP}...")
        try:
            # Desabilita verificação SSL temporariamente se necessário (comum em gov.br)
            resp = requests.get(URL_CADOP, verify=False)
            resp.raise_for_status()
            
            # Detecta encoding (geralmente é latin1 ou utf-8, vamos tentar salvar direto)
            with open(ARQUIVO_CADOP, 'wb') as f:
                f.write(resp.content)
            print("Download concluído.")
        except Exception as e:
            print(f"Erro ao baixar CADOP: {e}")
            return False
    return True


def limpar_cnpj(cnpj):
    """Remove caracteres não numéricos do CNPJ para garantir o match."""
    return re.sub(r'[^0-9]', '', str(cnpj))


def carregar_cadop_para_enriquecimento():
    """
    Carrega o CADOP selecionando apenas as colunas necessárias para o join.
    Trata duplicatas de CNPJ para evitar explosão de linhas no merge.
    """
    print("Carregando e preparando CADOP...")
    
    if not baixar_cadop():
        return None

    try:
        # Lê o CSV com separador ; e aspas
        # Tenta ler com utf-8, se falhar tenta latin1 (comum em arquivos do governo)
        try:
            df = pd.read_csv(ARQUIVO_CADOP, sep=';', quotechar='"', encoding='utf-8', dtype=str)
        except UnicodeDecodeError:
            df = pd.read_csv(ARQUIVO_CADOP, sep=';', quotechar='"', encoding='latin1', dtype=str)

        # Mapeamento de colunas solicitadas
        # Relatorio_cadop.csv possui: REGISTRO_OPERADORA, CNPJ, Modalidade, UF, etc.
        cols_map = {
            'CNPJ': 'CNPJ',
            'REGISTRO_OPERADORA': 'RegistroANS',
            'Modalidade': 'Modalidade',
            'UF': 'UF'
        }

        # Verifica se as colunas existem (normalizando nomes se necessário)
        df.columns = [c.strip() for c in df.columns]

        # Filtra apenas as colunas necessárias e renomeia
        df = df[list(cols_map.keys())].rename(columns=cols_map)

        # Limpeza da chave de join
        df['CNPJ'] = df['CNPJ'].apply(limpar_cnpj)

        # --- Análise Crítica: CNPJs Duplicados ---
        # Se houver mais de um registro para o mesmo CNPJ no CADOP, mantemos o primeiro.
        # Isso evita que uma linha de despesa se transforme em duas ou mais no relatório final.
        duplicados = df.duplicated(subset=['CNPJ']).sum()
        if duplicados > 0:
            print(f"Aviso: Removendo {duplicados} CNPJs duplicados do CADOP para garantir integridade do Join.")
            df = df.drop_duplicates(subset=['CNPJ'], keep='first')

        return df

    except Exception as e:
        print(f"Erro ao processar CADOP: {e}")
        return None


def main():
    print("Iniciando processo de enriquecimento de dados (Join)...")

    if not os.path.exists(ARQUIVO_DADOS_VALIDADOS):
        print(f"Arquivo de entrada {ARQUIVO_DADOS_VALIDADOS} não encontrado.")
        print("Por favor, execute o script 2_1.py primeiro.")
        return

    # 1. Carregar Dados Consolidados (Lado Esquerdo do Join)
    df_dados = pd.read_csv(ARQUIVO_DADOS_VALIDADOS, sep=';', encoding='utf-8', dtype={'CNPJ': str})
    df_dados['CNPJ'] = df_dados['CNPJ'].apply(limpar_cnpj)

    print(f"Registros financeiros carregados: {len(df_dados)}")

    # 2. Carregar Dados Cadastrais (Lado Direito do Join)
    df_cadop = carregar_cadop_para_enriquecimento()

    if df_cadop is None:
        return

    # 3. Realizar o Join (Merge)
    # Estratégia: Left Join
    # Justificativa: Prioridade para os dados financeiros. Se não houver cadastro,
    # mantemos o dado financeiro e marcamos o cadastro como não encontrado.
    df_final = pd.merge(df_dados, df_cadop, on='CNPJ', how='left')

    # 4. Tratamento de Registros sem Match
    cols_novas = ['RegistroANS', 'Modalidade', 'UF']
    for col in cols_novas:
        df_final[col] = df_final[col].fillna('N/A')

    # Estatísticas de Qualidade
    sem_match = df_final[df_final['RegistroANS'] == 'N/A'].shape[0]
    print(f"Registros enriquecidos com sucesso: {len(df_final) - sem_match}")
    print(f"Registros sem correspondência no cadastro (N/A): {sem_match}")

    # 5. Salvar Resultado
    df_final.to_csv(ARQUIVO_SAIDA, index=False, sep=';', encoding='utf-8')
    print(f"Arquivo final salvo em: {ARQUIVO_SAIDA}")


if __name__ == "__main__":
    main()