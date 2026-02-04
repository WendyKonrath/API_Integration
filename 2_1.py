import pandas as pd
import os
import re

# Configurações de Caminhos
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ARQUIVO_ENTRADA = os.path.join(BASE_DIR, "consolidado_despesas.csv")
ARQUIVO_SAIDA_VALIDO = os.path.join(BASE_DIR, "consolidado_validado.csv")
ARQUIVO_SAIDA_ERROS = os.path.join(BASE_DIR, "relatorio_inconsistencias.csv")


def validar_cnpj(cnpj):
    # Remove caracteres não numéricos
    cnpj = re.sub(r'[^0-9]', '', str(cnpj))

    # Verifica tamanho e sequências inválidas conhecidas
    if len(cnpj) != 14 or len(set(cnpj)) == 1:
        return False

    # Cálculo do primeiro dígito verificador
    pesos_1 = [5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
    soma_1 = sum(int(cnpj[i]) * pesos_1[i] for i in range(12))
    resto_1 = soma_1 % 11
    digito_1 = 0 if resto_1 < 2 else 11 - resto_1

    if int(cnpj[12]) != digito_1:
        return False

    # Cálculo do segundo dígito verificador
    pesos_2 = [6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
    soma_2 = sum(int(cnpj[i]) * pesos_2[i] for i in range(13))
    resto_2 = soma_2 % 11
    digito_2 = 0 if resto_2 < 2 else 11 - resto_2

    if int(cnpj[13]) != digito_2:
        return False

    return True


def processar_validacao():
    print("Iniciando validação estrita de dados...")

    if not os.path.exists(ARQUIVO_ENTRADA):
        print(f"Arquivo de entrada não encontrado: {ARQUIVO_ENTRADA}")
        print("Execute o script 1_3.py primeiro.")
        return

    # Carrega o CSV consolidado
    # dtype=str para CNPJ para evitar perda de zeros à esquerda
    df = pd.read_csv(ARQUIVO_ENTRADA, sep=';', encoding='utf-8', dtype={'CNPJ': str})

    # Converte ValorDespesas para numérico
    df['ValorDespesas'] = pd.to_numeric(df['ValorDespesas'], errors='coerce')

    # Validação de CNPJ
    # Aplica a função validar_cnpj linha a linha
    mask_cnpj_valido = df['CNPJ'].apply(validar_cnpj)

    # Razão Social não vazia (e diferente de 'N/A' gerado no passo anterior)
    mask_razao_social = (df['RazaoSocial'].notna()) & \
                        (df['RazaoSocial'].str.strip() != '') & \
                        (df['RazaoSocial'] != 'N/A')

    # Valores Numéricos Positivos
    # Nota: O passo 1.3 permitia negativos (estornos), mas este requisito 2.1 exige positivos.
    mask_valor_positivo = df['ValorDespesas'] > 0

    # Define o que é válido (todas as regras devem ser True)
    mask_geral_valida = mask_cnpj_valido & mask_razao_social & mask_valor_positivo

    # Separa os DataFrames
    df_validos = df[mask_geral_valida].copy()
    df_erros = df[~mask_geral_valida].copy()

    # Adiciona motivo do erro no relatório de inconsistências
    if not df_erros.empty:
        df_erros['Motivo_Erro'] = ''
        df_erros.loc[~mask_cnpj_valido, 'Motivo_Erro'] += 'CNPJ Inválido; '
        df_erros.loc[~mask_razao_social, 'Motivo_Erro'] += 'Razão Social Vazia/Inválida; '
        df_erros.loc[~mask_valor_positivo, 'Motivo_Erro'] += 'Valor Não Positivo; '

    # Salvamento

    print(f"Total de registros processados: {len(df)}")
    print(f"Registros Válidos: {len(df_validos)}")
    print(f"Registros Inconsistentes: {len(df_erros)}")

    df_validos.to_csv(ARQUIVO_SAIDA_VALIDO, index=False, sep=';', encoding='utf-8')
    print(f"Arquivo validado salvo em: {ARQUIVO_SAIDA_VALIDO}")

    if not df_erros.empty:
        df_erros.to_csv(ARQUIVO_SAIDA_ERROS, index=False, sep=';', encoding='utf-8')
        print(f"Relatório de erros salvo em: {ARQUIVO_SAIDA_ERROS}")
    else:
        print("Nenhuma inconsistência encontrada.")


if __name__ == "__main__":
    processar_validacao()