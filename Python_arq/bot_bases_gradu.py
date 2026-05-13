import pandas as pd
import numpy as  np
import datetime as dt
import engines as engs
import math

from sqlalchemy import text
from sqlalchemy import text


def atualizar_base(limite_contato=None,status=None ,num_partes=None, gerar_discador=False, gerar_disparo=False): #Função mãe ser para chamar demais funções de atualização de bases.

    base_dados = gerar_dados()

    if gerar_discador:
        gerar_bases_discador(base_dados,status=status, limite_contato=limite_contato,)

    if gerar_disparo:
        gerar_bases_disparo(base_dados,status=status, limite_contato=limite_contato,num_partes=num_partes)

def gerar_dados(): ##Atualizar a base de cliente para gerar os arquivos de disparo e discador
    try:
        print('Carregando base de dados...')
        eng = engs.get_engine()
        text_qry = text(engs.load_query('base_gradu.sql'))
        base_dados = pd.read_sql(text_qry, eng)
        return base_dados
    except Exception as e:
        print(f'Erro ao carregar base de dados: {str(e)}')    
        return None

dia = dt.datetime.now().day
mes = dt.datetime.now().month
ano = dt.datetime.now().year

def padronizar_bases_discador(base_discador): ##Padroniza a Base de dados para atuação no discador

    resultado = base_discador.copy()
    resultado.columns = (
        resultado.columns
        .str.strip()
        .str.lower()
    )

    base_rename = {
    'base_discador':{
    'ies':'IES',
    'cód_candidato':'COD_CANDIDATO',
    'nome':'NOME_CANDIDATO',
    'email':'EMAIL_CANDIDATO',
    'hubspotcontactid':'HUBSPOTCONTACTID',
    'cpf':'CPF_INSCRITO',
    'telefone':'CELULAR',
    'data hora inscrição':'DATA_INSCRICAO',
    'polo':'POLO',
    'curso':'Curso_Escolhido',
    'tipo ingresso':'TIPO_INGRESSO',
    'status':'ANO_MAIOR_NOTA',
    'data do lead':'MAIOR_NOTA',
    'data da tabulação':'DESCONTO',
    'last_tab':'FAIXA_DESCONTO'}
}

    resultado = resultado.rename(
        columns=base_rename['base_discador']
    )

    dropCols = ['qtd_hsm', 'qtd_call', 'ultima tabulação','last_ticket_tag']
    resultado = resultado.drop(columns=dropCols, errors='ignore')

    return resultado

def gerar_bases_discador(base_discador,status=None,limite_contato=None):  ##Gera a base para atuação no discador, segmentada por IES e status do candidato
    
    if status is not None:
        base_discador = base_discador[base_discador['status'].isin(status)]

    if limite_contato is None:
        limite_contato = base_discador['qtd_call'].max()
        print(f'Limite de contatos ajustado para {limite_contato} devido ao número máximo de chamadas na base de dados.')
    
    
    base_discador = base_discador[base_discador['qtd_hsm'] <= limite_contato]    


    cluster_discador = {
    'PUCPR':1679,
    'EADUNISINOS':1680,
    'FAESA':1681,
    'UCS':1681,
    'UNISAGRADO':1681,
    'UNIVALI':1681
    }


    base_discador = padronizar_bases_discador(base_discador)

    for ies, campaingid in cluster_discador.items():
        base_dados_filtrada = base_discador[(base_discador['IES'] == ies)]
        bases = {
            'Inscrito':'Inscrito',
            'Avaliado':'Avaliado',
            'Pré-Matriculado':'Pré-Matriculado',
        }
        for status in bases:
            base_status_filtrada = base_dados_filtrada[base_dados_filtrada['ANO_MAIOR_NOTA'] == status]
            rows = base_status_filtrada.shape[0]
            if rows > 10:
                nome_arquivo = fr'CAP_OPS_{campaingid}_{ies}_{ano}{mes}{dia}_{status}_Vol{rows}'
                caminho_arquivo = fr'C:\bases\olos\{nome_arquivo}.csv'
                print(f'Gerando arquivo: {ies} - {status}...')
                
                try:
                    base_status_filtrada.to_csv(caminho_arquivo.upper(), sep=";", index=False, encoding="utf-8-sig")
                    print(f'Arquivo gerado com sucesso: {caminho_arquivo}')
                except Exception as e:
                    print(f'Erro ao exportar arquivo: {caminho_arquivo} - {str(e)}')    

def gerar_bases_disparo(base_dados,status=None, limite_contato=None,num_partes=None): ##Gera a base de dados para realização de disparos de HSM

    cluster_disparo = [
        'PUCPR',
        'EADUNISINOS',
        'FAESA',
        'UCS',
        'UNISAGRADO',
        'UNIVALI'
    ]

    if status is not None:
        base_dados = base_dados[base_dados['status'].isin(status)]

    if limite_contato is None:
        limite_contato = base_dados['qtd_hsm'].max()
        print(f'Limite de contatos ajustado para {limite_contato} devido ao número máximo de HSMs na base de dados.')

    base_dados = base_dados[base_dados['qtd_hsm'] < limite_contato]
    base_dados = base_dados[base_dados['ies'].isin(cluster_disparo)]

    if num_partes is None: ##Definir o número de partes para dividir a base 
        num_partes = 4 


    for ies, grupo in base_dados.groupby('ies'):
        
        base_dados_filtrada = grupo[['celular','nome']].copy()
        base_dados_filtrada['nome'] = (
            base_dados_filtrada['nome']
                .fillna('')
                .str.split(' ')
                .str[0].str.title()
            )

        total_linhas = base_dados_filtrada.shape[0]

        if total_linhas >= 10:  ## Evitar gerar arquivos para bases muito pequenas

            nome_base = fr'CAP_OPS_GERAL_HSM_{dia}_{mes}_{ano}_{ies}'
            linhas_por_parte = math.ceil(total_linhas / num_partes)
            
            for i in range(num_partes):
                inicio = i * linhas_por_parte
                fim = inicio + linhas_por_parte
                parte_df = base_dados_filtrada.iloc[inicio:fim]
                num_linhas = len(parte_df)
                if num_linhas == 0:
                    continue
                else:
                    nome_arquivo = fr'{nome_base}_Vol{num_linhas}_Pt{i+1}'
                    caminho_arquivo = fr'C:\bases\disparos\graduacao\{nome_arquivo}.csv'
                    parte_df.to_csv(caminho_arquivo, index=False,sep=';', encoding='utf-8')
                    print(f'Gerando arquivo: {nome_arquivo} | Parte {i + 1} | {num_linhas} linhas.')




atualizar_base(limite_contato=10,num_partes=4, gerar_discador=False, gerar_disparo=True)