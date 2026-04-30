import pandas as pd
import numpy as  np
import datetime as dt
import engines as engs
import math

from sqlalchemy import text
from calendar import day_name
from sqlalchemy import text
from pathlib import Path


def atualizarBase(limite_contato=None, num_partes=None, gerarDiscador=False, gerarDisparo=False):
    base_dados = gerarDados()
    if gerarDiscador:
        gerarBasesDiscador(base_dados, limiteContato=limite_contato,)

    if gerarDisparo:
        gerarBasesDisparo(base_dados, limiteContato=limite_contato,num_partes=num_partes)



def gerarDados(): ##Atualizar a base de cliente para gerar os arquivos de disparo e discador
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


def padronizarBaseDiscador(base_discador): ##PAdroniza a Base de dados para atuação no discador

    resultado = base_discador.copy()
    resultado.columns = (
        resultado.columns
        .str.strip()
        .str.lower()
    )

    baseRename = {
    'base_discador':{
    'ies':'IES',
    'cód_candidato':'COD_CANDIDATO',
    'nome':'NOME_CANDIDATO',
    'email':'EMAIL_CANDIDATO',
    'hubspotcontactid':'HUBSPOTCONTACTID',
    'cpf':'CPF_INSCRITO',
    'telefone':'CELULAR',
    'Data hora inscrição':'DATA_INSCRICAO',
    'polo':'POLO',
    'curso':'Curso_Escolhido',
    'Tipo Ingresso':'TIPO_INGRESSO',
    'status':'ANO_MAIOR_NOTA',
    'Data do lead':'MAIOR_NOTA',
    'Data da Tabulação':'DESCONTO',
    'last_tab':'FAIXA_DESCONTO'}
}

    resultado = resultado.rename(
        columns=baseRename['base_discador']
    )

    dropCols = ['Qtd. HSM', 'qtd_call', 'Ultima Tabulação']
    resultado = resultado.drop(columns=dropCols, errors='ignore')

    return resultado

def gerarBasesDiscador(base_discador,limiteContato=None): ##Gera a base para atuação no discador, segmentada por IES e status do candidato

    if limiteContato is None:
        limiteContato = base_discador['qtd_call'].max()
        print(f'Limite de contatos ajustado para {limiteContato} devido ao número máximo de chamadas na base de dados.')
    
    
    base_discador = base_discador[base_discador['qtd_call'] <= limiteContato]    


    clusterDiscador = {
    'PUCPR':1679,
    'EADUNISINOS':1680,
    'FAESA':1681,
    'UCS':1681,
    'UNISAGRADO':1682,
    'UNIVALI':1682
    }


    base_discador = padronizarBaseDiscador(base_discador)

    for ies, campaingid in clusterDiscador.items():
        baseDadosFiltrada = base_discador[(base_discador['IES'] == ies)]
        bases = {
            'Inscrito':'Inscrito',
            'Avaliado':'Avaliado',
            'Pré-Matriculado':'Pré-Matriculado',
        }
        for status in bases:
            baseStatusFiltrada = baseDadosFiltrada[baseDadosFiltrada['ANO_MAIOR_NOTA'] == status]
            rows = baseStatusFiltrada.shape[0]
            if rows > 10:
                templateName = fr'CAP_OPS_{campaingid}_{ies}_{ano}{mes}{dia}_{status}_Vol{rows}'
                nomeArquivo = fr'C:\bases\olos\{templateName}.csv'
                print(f'Gerando arquivo: {ies} - {status}...')
                
                try:
                    baseStatusFiltrada.to_csv(nomeArquivo, index=False,sep=';')
                    print(f'Arquivo gerado com sucesso: {nomeArquivo}')
                except Exception as e:
                    print(f'Erro ao exportar arquivo: {nomeArquivo} - {str(e)}')    
                


def gerarBasesDisparo(base_dados, limiteContato=None,num_partes=None): ##Gera a base de dados para realização de disparos de HSM

    clusterDisparo = [
        'PUCPR',
        'EADUNISINOS',
        'FAESA',
        'UCS',
        'UNISAGRADO',
        'UNIVALI'
    ]


    if limiteContato is None:
        limiteContato = base_dados['Qtd. HSM'].max()
        print(f'Limite de contatos ajustado para {limiteContato} devido ao número máximo de HSMs na base de dados.')

    base_dados = base_dados[base_dados['Qtd. HSM'] <= limiteContato]
    base_dados = base_dados[base_dados['ies'].isin(clusterDisparo)]

    if num_partes is None: ##Definir o número de partes para dividir a base 
        num_partes = 4 


    for ies, grupo in base_dados.groupby('ies'):
        
        baseDadosFiltrada = grupo[['telefone','nome']].copy()
        baseDadosFiltrada['nome'] = (
            baseDadosFiltrada['nome']
                .fillna('')
                .str.split(' ')
                .str[0].str.title()
            )

        rows = baseDadosFiltrada.shape[0]

        if rows >= 10:  ## Evitar gerar arquivos para bases muito pequenas
            nomeBase = fr'CAP_OPS_GERAL_HSM_{dia}{mes}{ano}_{ies}'
            linhasPorParte = math.ceil(rows / num_partes)
            for i in range(num_partes):
                inicio = i * linhasPorParte
                fim = inicio + linhasPorParte
                parte_df = baseDadosFiltrada.iloc[inicio:fim]
                num_linhas = len(parte_df)
                if num_linhas == 0:
                    continue
                else:
                    nomeArquivo = fr'{nomeBase}_Vol{num_linhas}_Pt{i+1}'
                    caminhoArquivo = fr'C:\bases\disparos\graduacao\{nomeArquivo}.csv'
                    parte_df.to_csv(caminhoArquivo, index=False,sep=';', encoding='utf-8')
                    print(f'Gerando arquivo: {nomeBase} | Parte {i + 1} | {num_linhas} linhas.')






atualizarBase(limite_contato=15, num_partes=4, gerarDiscador=True, gerarDisparo=True)
