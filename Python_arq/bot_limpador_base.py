import pandas as pd
import numpy as np 
import datetime as dt
import warnings
import unicodedata
import engines

from sqlalchemy import text
from pathlib import Path

warnings.filterwarnings('ignore',category=FutureWarning)
eng = engines.get_engine()

queries ={
    'sispag':text(engines.load_query('qry_sispag_lim.sql')),
    'qtd_calls':text(engines.load_query('qry_qtd_call_lim.sql')),
    'tab_olos':text(engines.load_query('tab_olos_lim.sql')),
    'tab_blip':text(engines.load_query('tab_blip_lim.sql')),
    'qry_leadscore_olos':text(engines.load_query('score_olos_lim.sql')),
    'qry_leadscore_blip':text(engines.load_query('score_blip_lim.sql'))
}

bases_limpador = {}

try:
    for nome, query in queries.items():
        print(f'Carregando query: {nome}...')
        bases_limpador['nome'] = pd.read_sql(query, eng)

    blacklist = Path(r'C:\bases\Limpador\black_list_new.xlsx')    
    df_blacklist = pd.read_excel(blacklist,dtype=str)

    pasta = r"M:\Comercial\Call Center - OLOS\01.MAILINGS IMPORTADOS\SECAD\RANGE_VALIDACAO\upload"
    lista_dfs = []
    for arquivo in Path(pasta).glob('*'):
        if arquivo.name.startswith('.') or arquivo.name.startswith('~'):
            continue

        try:
            if arquivo.suffix.lower() in ['.csv','.txt']:
                df = pd.read_csv(arquivo,encoding='latin-1',sep=None,engine='python',on_bad_lines='skip')
            elif arquivo.suffix.lower() in ['.xlsx','.xls']:
                df = pd.read_excel(arquivo)
            else:
                continue

            df['Nome da Origem'] = arquivo.name

            lista_dfs.append(df)
        except Exception as e:
            print(f'Erro ao ler {arquivo.name}: {e}')
            continue

    try:
        rodando_atualmente = pd.concat(lista_dfs,ignore_index=True)
        colunas_remover = [
            'Id_do_cliente_', 'Nome', 'CPF', 'Fone_4', 'ORIGEM', 
            'ORIGEM_DO_LEAD2', 'PROFISSAO', 'ESPECIALIDADE', 'LEAD_SCORING', 
            'PRODUTO', 'A_LTIMO_REGISTRO', 'LOGRADOURO', 'BAIRRO', 
            'CIDADE', 'UF', 'CEP', 'DATA_NASCIMENTO', 'DADOS_DO_PAGAMENTO', 
            'MOTIVO_DE_CANCELAMENTO', 'URL_2', 'Fone_3'
        ]

        rod_atualmente = rodando_atualmente.drop(columns=colunas_remover,errors='ignore')
        rod_atualmente['Fone_1'] = rod_atualmente['Fone_1'].astype(str)
        rod_atualmente['Fone_1'] = rod_atualmente['Fone_1'].str.replace(r'\D','',regex=True)
        rod_atualmente = rod_atualmente.drop_duplicates(subset=['Fone_1'],keep='first')
        rod_atualmente = rod_atualmente.dropna(how='all')
        rod_atualmente.columns = rod_atualmente.columns.str.lower()
        rod_atualmente = rod_atualmente.reset_index(drop=True)
    except Exception as e:
        print(f'Erro ao processar base de rodando atualmente: {e}')
        rod_atualmente = None
except Exception as e:
    print(f'Erro... {e}')

def padrao_e_filtro(
    
):  
    pass

Depara_colunas = {
    'base_inativa':{
        'email':'email',
        'phone_1':'phone',
        'product':'program',
        'area':'area',
        'copy':'copy',
        'type':'type'
    },
    'base_ativa':{
        'email':'email',
        'phone':'phone',
        'area':'area',
        'copy':'copy'
    },
    'base_carrinho':{
        'Area':'area',
        'email':'email',
        'area':'area',
        'copy':'copy'
    },
    'base_hubspot':''
}

def filtrar_base():
    pass

def padronizar(df_base,mapa_colunas):
    df_padrao  =df.rename(columns=mapa_colunas)
    return df_padrao[list(mapa_colunas.values)]

def limpeza_minima(df_base):
    resultado = df_base.copy()

    resultado = resultado[
        resultado['email'].notna() &
        resultado['phone'].notna()
    ]

    resultado = resultado[
        (resultado['email'].str.strip() != '') &
        (resultado['phone'].str.strip() != '')
    ]

    resultado = padrao_email_phone(resultado)
    resultado = validar_num_movel(resultado)

    resultado['copy'] = resultado['email']+';'+resultado['phone']
    
    resultado = remover_duplicadas(resultado)

    return resultado


def remover_duplicadas(df_base,id_key='copy'):
    ## utilizado na função LIMPEZA_MININA() ## 
    return(
        df_base
        .sort_values(by=id_key)
        .drop_duplicates(subset=id_key,keep='first')
        .reset_index(drop=True)
    )

def padrao_email_phone(df_base):
    resultado = df_base.copy()

    resultado['email'] = (
        resultado['email'].str.strip().str.lower()
    )

    resultado['phone'] = (
        resultado['phone'].str.replace(r'\D','',regex=True)
    )
    return resultado

def validar_num_movel(df_base):
    resultado = df_base.copy()

    resultado['tel_movel'] = (
        resultado['phone'].str.len().eq(11) &
        resultado['phone'].str[2].eq('9')
    )
    resultado = resultado[resultado['tel_movel'] == True]

    return resultado

def verificar_blacklist(df_base,blacklist):
    resultado = df_base.copy()
    df_blacklist = blacklist.copy()
    
    df_blacklist['telefone'] = (
        df_blacklist['telefone'].astype(str).str.replace(r'\D','',regex=True)    
    )

    resultado['in_blacklist'] = resultado['phone'].isin(df_blacklist['telefone'])
    resultado = resultado[resultado['in_blacklist'] == False]

    return resultado


def rod_atual(df_base,rod_atual):
    resultado = df_base.copy()
    df_rodando_atualmente = rod_atual

    resultado['rodando_atualmente'] = resultado['phone'].isin(df_rodando_atualmente['fone_1'])
    resultado = resultado[resultado['rodando_atualmente'] == False]

    return resultado

def lead_score_olos():
    pass

def lead_score_blip():
    pass

def qtd_calls():
    pass

def ultima_compra():
    pass

def ultimo_contato_olos():
    pass

def ultimo_contato_blip():
    pass

def exportar_bases():
    pass

def filtro_final():
    pass