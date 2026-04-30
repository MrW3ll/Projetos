import pandas as pd
import math
import os

try:
    # Pasta de entrada (onde está o arquivo original)
    pasta_entrada = r"C:\Users\wconceicao\OneDrive - Grupo A Educação SA\Área de Trabalho\Arquivos - Disparos\RANGE\STAND BY"

    # Pasta de saída (onde vão ficar os arquivos divididos)
    pasta_saida = r"C:\Users\wconceicao\OneDrive - Grupo A Educação SA\Área de Trabalho\Arquivos - Disparos\RANGE"

    # Nome do arquivo original
    arquivo_nome = "cap_ops_psicologia_mesdocuidado.csv"  # <-- coloque o nome do arquivo aqui

    # Caminho completo do arquivo de entrada
    arquivo = os.path.join(pasta_entrada, arquivo_nome)

    # Quantidade de partes desejadas
    num_partes = 5  # <-- coloque a quantidade de partes aqui

    # Lê o CSV
    df = pd.read_csv(arquivo)

    # Total de linhas
    total_linhas = len(df)

    # Tamanho de cada parte
    linhas_por_parte = math.ceil(total_linhas / num_partes)

    # Nome base do arquivo (sem extensão)
    nome_base = os.path.splitext(os.path.basename(arquivo))[0]

    # Criar os arquivos divididos
    for i in range(num_partes):
        inicio = i * linhas_por_parte
        fim = inicio + linhas_por_parte

        parte_df = df.iloc[inicio:fim]
        num_linhas = len(parte_df)

        if num_linhas == 0:
            break
        else:
            # Nome do arquivo final
            nome_saida = f"{nome_base}_v{num_linhas}_pt{i+1}.csv"

            # Caminho completo para salvar
            caminho_saida = os.path.join(pasta_saida, nome_saida)

            parte_df.to_csv(caminho_saida, index=False, encoding="utf-8")

            print(f"Arquivo salvo: {caminho_saida}")

            

except Exception as e:
    print(f"Erro: {e}")
