import pandas
import os
import zipfile

def validar_arquivos(pasta_destino):
    print("Iniciando validação de dados nos arquivos extraídos...")
    if not os.path.exists(pasta_destino):
        return

    for arquivo in os.listdir(pasta_destino):
        caminho_completo = os.path.join(pasta_destino, arquivo)
        if os.path.isdir(caminho_completo):
            continue

        manter = False
        try:
            df = None
            # Lista de possíveis nomes para a coluna de descrição (Normalização de estrutura)
            colunas_possiveis = ['DESCRICAO', 'DESC', 'EVENTO', 'HISTORICO', 'DETALHES', 'OBSERVACAO']
            
            if arquivo.lower().endswith(('.csv', '.txt')):
                # Processamento INCREMENTAL para CSV/TXT (evita estouro de memória)
                chunk_iter = None
                try:
                    chunk_iter = pandas.read_csv(caminho_completo, sep=';', encoding='latin1', chunksize=10000, low_memory=False)
                except:
                    chunk_iter = pandas.read_csv(caminho_completo, sep=',', chunksize=10000, low_memory=False)
                
                for chunk in chunk_iter:
                    # Normaliza colunas do pedaço atual
                    chunk.columns = [str(col).upper().strip() for col in chunk.columns]
                    
                    # Identifica automaticamente a coluna correta baseada na lista de sinônimos
                    coluna_alvo = next((col for col in chunk.columns if col in colunas_possiveis), None)
                    
                    if coluna_alvo:
                        if chunk[coluna_alvo].astype(str).str.contains('eventos|sinistros', case=False, na=False).any():
                            manter = True
                            break # Encontrou? Para de ler o arquivo (otimização)

            elif arquivo.lower().endswith(('.xlsx', '.xls')):
                # Excel geralmente cabe na memória, mas aplicamos a mesma lógica de colunas flexíveis
                df = pandas.read_excel(caminho_completo)
                df.columns = [str(col).upper().strip() for col in df.columns]
                
                coluna_alvo = next((col for col in df.columns if col in colunas_possiveis), None)
                
                if coluna_alvo:
                    if df[coluna_alvo].astype(str).str.contains('eventos|sinistros', case=False, na=False).any():
                        manter = True

        except Exception:
            pass

        if not manter:
            print(f"Removendo arquivo sem dados relevantes: {arquivo}")
            os.remove(caminho_completo)
        else:
            print(f"Arquivo validado: {arquivo}")

def extrair_e_limpar():
    # Define os caminhos das pastas
    pasta_origem = "trimestres_baixados"
    pasta_destino = os.path.join(pasta_origem, "trimestres_extraidos")

    # Cria a pasta de destino se ela não existir
    if not os.path.exists(pasta_destino):
        os.makedirs(pasta_destino)

    # Verifica se a pasta de origem existe antes de prosseguir
    if not os.path.exists(pasta_origem):
        print(f"A pasta '{pasta_origem}' não foi encontrada.")
        return

    # Itera sobre os arquivos na pasta de downloads
    for arquivo in os.listdir(pasta_origem):
        caminho_completo = os.path.join(pasta_origem, arquivo)

        # Ignora diretórios (como a própria pasta 'trimestres_extraidos')
        if os.path.isdir(caminho_completo):
            continue

        # Verifica se é um arquivo zip válido
        if zipfile.is_zipfile(caminho_completo):
            print(f"Extraindo: {arquivo}")
            with zipfile.ZipFile(caminho_completo, 'r') as zip_ref:
                zip_ref.extractall(pasta_destino)
            
            print(f"Removendo arquivo original: {arquivo}")
            os.remove(caminho_completo)

    validar_arquivos(pasta_destino)

if __name__ == "__main__":
    extrair_e_limpar()