import pandas as pd
import math
import os
import re

try:
    entrada = r'M:\Comercial\Call Center - OLOS\01.MAILINGS IMPORTADOS\SECAD\RANGE_VALIDACAO\Divididos\STAND BY'
    saida = r'M:\Comercial\Call Center - OLOS\01.MAILINGS IMPORTADOS\SECAD\RANGE_VALIDACAO\Divididos'
    arquivo_nome = '_1605_MEDICINA_BASE&LEADS_LEADS&ENGAJADOS_17032026_V5739.csv'
    arquivo = os.path.join(entrada,arquivo_nome)

    df = pd.read_csv(arquivo,encoding='latin-1',sep=';')
    total_linhas = len(df)
    volume_max = 400
    num_partes = int(len(df) / volume_max)
    linhas_por_parte = math.ceil(total_linhas/num_partes)
    nome_base = os.path.splitext(os.path.basename(arquivo))[0]
    nome_base = re.sub(r'V\d+$','V',nome_base)

    for i in range(num_partes):
        inicio = i * linhas_por_parte
        fim = inicio + linhas_por_parte

        parte_df = df.iloc[inicio:fim]
        num_linhas = len(parte_df)

        if num_linhas == 0:
            break
        else:
            nome_saida = f'{nome_base}{num_linhas}_pt{i+1}.csv'
            caminho_saida = os.path.join(saida,nome_saida)
            parte_df.to_csv(caminho_saida,index=False,encoding='latin-1',sep=';')
            print(f'Arquivo salvo: {caminho_saida}')
except Exception as e:
    print(f'Erro: {e}')