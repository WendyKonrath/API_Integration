import pandas as pd
import os

# Configurações de Caminhos
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ARQUIVO_ENTRADA = os.path.join(BASE_DIR, "consolidado_enriquecido.csv")
ARQUIVO_SAIDA_CSV = os.path.join(BASE_DIR, "despesas_agregadas.csv")


def main():
    print("Iniciando agregação e análise estatística...")

    if not os.path.exists(ARQUIVO_ENTRADA):
        print(f"Arquivo de entrada {ARQUIVO_ENTRADA} não encontrado.")
        print("Por favor, execute o script 2_2.py primeiro.")
        return

    # 1. Carregar Dados Enriquecidos
    try:
        df = pd.read_csv(ARQUIVO_ENTRADA, sep=';', encoding='utf-8')
    except Exception as e:
        print(f"Erro ao ler o arquivo CSV: {e}")
        return

    # Garantir que ValorDespesas é numérico
    df['ValorDespesas'] = pd.to_numeric(df['ValorDespesas'], errors='coerce').fillna(0)

    print(f"Registros carregados: {len(df)}")

    # 2. Agregação por Razão Social e UF
    # Cálculos solicitados: Total, Média (por trimestre) e Desvio Padrão
    print("Calculando estatísticas...")

    agregado = df.groupby(['RazaoSocial', 'UF'])['ValorDespesas'].agg(
        TotalDespesas='sum',
        MediaTrimestral='mean',
        DesvioPadrao='std'
    ).reset_index()

    # Tratamento para Desvio Padrão NaN (ocorre quando há apenas 1 registro/trimestre para a operadora)
    # Preenchemos com 0.0 pois não há variação com um único dado.
    agregado['DesvioPadrao'] = agregado['DesvioPadrao'].fillna(0.0)

    # 3. Ordenação (Trade-off Técnico)
    # Estratégia: Ordenar por Total de Despesas Decrescente.
    # Justificativa: Destacar as operadoras com maior impacto financeiro no topo do relatório.
    # O uso de sort_values em memória é eficiente para este volume de dados.
    agregado = agregado.sort_values(by='TotalDespesas', ascending=False)

    # Formatação opcional para melhor leitura (arredondamento)
    agregado['TotalDespesas'] = agregado['TotalDespesas'].round(2)
    agregado['MediaTrimestral'] = agregado['MediaTrimestral'].round(2)
    agregado['DesvioPadrao'] = agregado['DesvioPadrao'].round(2)

    # 4. Salvar Resultado CSV
    agregado.to_csv(ARQUIVO_SAIDA_CSV, index=False, sep=';', encoding='utf-8')
    print(f"Arquivo CSV salvo em: {ARQUIVO_SAIDA_CSV}")

    print("-" * 50)
    print(f"Análise concluída.")


if __name__ == "__main__":
    main()