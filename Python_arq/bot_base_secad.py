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


## importação e leitura de bases para limpeza ## 
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
        bases_limpador[nome] = pd.read_sql(query, eng)

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

## importação e leitura de bases para limpeza ## 


### Leitura e importação da base crua ####

Caminho_builder = Path(r'C:\Users\wconceicao\OneDrive - Grupo A Educação SA\Área de Trabalho\Construtores\base_builder_V2.xlsm')
sheets_builder = [
    'base_carrinho',
    'base_inativa',
    'base_ATIVA'
]

bases = {}

for sheet in sheets_builder:
    df = pd.read_excel(
        Caminho_builder,
        sheet_name=sheet,
        dtype=str,
        engine='openpyxl'
    )

    bases[sheet] = df
    print(f'Sheet "{sheet}" carregada: {df.shape}')

df_inativa = bases['base_inativa']
df_ativa = bases['base_ATIVA']
df_carrinho = bases['base_carrinho']

def diagnostico_copy(df,nome):
    print(f'\n📊 {nome}')
    print('Linhas totais        :', df.shape[0])
    print('Copy nulos           :', df['copy'].isna().sum())
    print('Copy vazios ("")     :', (df['copy'].str.strip() == '').sum())
    print('Copy duplicados      :', df['copy'].duplicated().sum())

diagnostico_copy(df_inativa, 'Base Inativa')
diagnostico_copy(df_ativa, 'Base Ativa')
diagnostico_copy(df_carrinho, 'Base Carrinho')    

df_inativa = bases['base_inativa']
df_inativa = df_inativa.sort_values(by=['copy'],ascending=True)
df_inativa = df_inativa.drop_duplicates(subset='copy',keep='first')
df_inativa = df_inativa.reset_index(drop=True)

df_ativa = bases['base_ATIVA']
df_ativa = df_ativa.sort_values(by=['copy'], ascending=True)
df_ativa = df_ativa.drop_duplicates(subset='copy',keep='first')
df_ativa = df_ativa.reset_index(drop=True)

df_carrinho = bases['base_carrinho']
df_carrinho = df_carrinho.sort_values(by=['copy'],ascending=True)
df_carrinho = df_carrinho.drop_duplicates(subset='copy',keep='first')
df_carrinho = df_carrinho.reset_index(drop=True)

### Leitura e importação da base crua ####

## FUNÇÕES - Inicio ## 
def padrao_e_filtro(
        df_base,
        mapa_colunas,
        df_rodando_atualmente,
        df_leadscore_olos,
        df_leadscore_blip,
        
        
        area=None,
        base_type=None,
        program=None,
        df_blacklist = None,
        
    ):

    if base_type is not None:
        resultado = padronizar(df_base,mapa_colunas)
    
    resultado = filtrar_base(resultado, area=area, base_type=base_type, program=program)
    resultado = limpeza_minima(resultado)
    resultado = lead_score_olos(resultado,df_leadscore_olos)
    resultado = lead_score_blip(resultado,df_leadscore_blip)

    if df_blacklist is not None:
        resultado = verificar_blacklist(resultado, df_blacklist)

    resultado = rod_atual(resultado, df_rodando_atualmente)

    if base_type is not None:
        resultado = filtro_final(resultado,base_type=base_type)
    return resultado



def padronizar(df_base,mapa_colunas):
    df_padrao = df_base.rename(columns=mapa_colunas)
    return df_padrao[list(mapa_colunas.values)]

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

def filtrar_base(df_base, area=None, base_type=None, program=None):
    resultado = df_base.copy()

    if area is not None:
        areas = area if isinstance(area, list) else [area]
        resultado = resultado[
            resultado['area']
            .str.upper()
            .isin([a.upper() for a in areas])
        ]
    
    if base_type is not None:
        types = base_type if isinstance(base_type, list) else [base_type]
        resultado = resultado[
            resultado['type']
            .str.upper()
            .isin([t.upper() for t in types])
        ]

    if program is not None:
        programs = program if isinstance(program, list) else [program]
        resultado = resultado[
            resultado['program']
            .str.upper()
            .isin([p.upper() for p in programs])
        ]        

    return resultado

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

def lead_score_olos(df_base,df_leadscore_olos):
    resultado = df_base.copy()
    leadscore = df_leadscore_olos.copy()

    leadscore['phone_number'] = (
        leadscore['phone_number'].astype(str).str.replace(r'\D','',regex=True)
    )
    
    resultado['leadscore_olos'] = resultado['phone'].isin(leadscore['phone_number'])
    resultado = resultado[resultado['leadscore_olos'] == False]

    return resultado

def lead_score_blip(df_base,df_leadscore_blip):
    resultado = df_base.copy()
    leadscore = df_leadscore_blip.copy()

    leadscore['phone_number'] = (
        leadscore['phone_number'].astype(str).str.replace(r'\D','',regex=True)
    )

    resultado['leadscore_blip'] = resultado['phone'].isin(leadscore['phone_number'])
    resultado = resultado[resultado['leadscore_blip'] == False]

    return resultado

def qtd_calls(df_base,qtd_calls,limit=10):
    resultado = df_base.copy()
    qtd = qtd_calls.copy()

    qtd['phone_number'] = (
        qtd['phone_number'].astype(str).str.replace(r'\D','',regex=True)
    )
    
    resultado = resultado.merge(
        qtd,
        left_on='phone', 
        right_on='phone_number', 
        how='left'
    )

    resultado['call_count'] = resultado['call_count'].fillna(0).astype(int)
    resultado = resultado[resultado['call_count'] < limit]

    resultado = resultado.drop(columns=['phone_number','call_count'])
    
    return resultado

def ultima_compra(df_base,sispag):

    resultado = df_base.copy()
    df_sispag = sispag.copy()

    df_sispag['id_copy'] = df_sispag['email'] + ';' + df_sispag['celular']
    resultado['ultima_compra'] = resultado['copy'].isin(df_sispag['id_copy'])
    resultado['ultima_compra'] = resultado[resultado['ultima_compra'] == False]
    resultado = resultado.drop(columns=['ultima_compra'])

    return resultado


def ultimo_contato_olos(df_base,df_tab_olos):
    
    resultado = df_base.copy()
    df_olos = df_tab_olos.copy()

    df_olos['customer_id'] = (
        df_olos['customer_id'].str.strip().str.lower()
    )

    df_olos['phone'] = (
        df_olos['phone'].str.replace(r'\D','',regex=True)
    )

    df_olos['id_copy'] = df_olos['customer_id'] + ';' + df_olos['phone']

    resultado = resultado.merge(
        df_olos[['id_copy','status_retorno']],
        left_on='copy',
        right_on = 'id_copy',
        how='left'
    )

    return resultado

def ultimo_contato_blip(df_base,df_tab_blip):
    
    resultado = df_base.copy()
    df_blip = df_tab_blip.copy()

    df_blip['contact_id_trimmed'] = (
        df_blip['contact_id_trimmed'].str.replace(r'\D','',regex=True)
    )

    resultado = resultado.merge(
        df_blip[['contact_id_trimmed','status_retorno']],
        left_on='phone',
        right_on = 'contact_id_trimmed',
        how='left'
    )

    return resultado

def exportar_bases():
    pass

def filtro_final(df_base,base_type):

    colunas_bases = {
        
        'base_ativa':{
            'Id_do_cliente_': lambda df: df['program'],
            'Nome': lambda df: df['name'],
            'CPF': lambda df: df['cpf'],
            'E_mail': lambda df: df['email'],
            'Fone_1': lambda df: df['phone'],
            'Fone_2': lambda df: df['phone_alt'],
            'Fone_3': None,
            'Fone_4': None,
            'ORIGEM': 'BASE ATIVA',
            'ORIGEM_DO_LEAD2': lambda df: 'BASE ATIVA ' + df['program'],
            'PROFISSAO': lambda df: df['area'],
            'ESPECIALIDADE': lambda df: df['expertise'],
            'LEAD_SCORING': None,
            'PRODUTO': lambda df: df['program'],
            'A_LTIMO_REGISTRO': None,
            'LOGRADOURO': lambda df: df['address'],
            'BAIRRO': None,
            'CIDADE': lambda df: df['city'],
            'UF': None,
            'CEP': None,
            'DATA_NASCIMENTO': lambda df: df['birth_date'],
            'DADOS_DO_PAGAMENTO': lambda df: df['payment_type'],
            'MOTIVO_DE_CANCELAMENTO': lambda df: df['invoice_type'],
            'DATA_IMPORTA__O': None,
            'URL_2': lambda df: df['end_date']
        },

        'base_inativa':{
            'Id_do_cliente_':lambda df: df['client_id'],
            'Nome':lambda df: df['name'],
            'CPF':lambda df: df['cpf'],
            'E_mail':lambda df: df['email'],
            'Fone_1':lambda df: df['phone'],
            'Fone_2':lambda df: df['phone_2'],
            'Fone_3':None,
            'Fone_4':None,
            'ORIGEM':lambda df: df['type'],
            'ORIGEM_DO_LEAD2':lambda df: df['type'] + ' ' + df['program'],
            'PROFISSAO':lambda df: df['area'],
            'ESPECIALIDADE':None,
            'LEAD_SCORING':None,
            'PRODUTO':lambda df: df['program'],
            'A_LTIMO_REGISTRO':lambda df: df['cancellation_date'],
            'LOGRADOURO':lambda df: df['address'],
            'BAIRRO':None,
            'CIDADE':lambda df: df['city'],
            'UF':lambda df: df['state'],
            'CEP':None,
            'DATA_NASCIMENTO':None,
            'DADOS_DO_PAGAMENTO':None,
            'MOTIVO_DE_CANCELAMENTO':lambda df: df['kind'],
            'DATA_IMPORTA__O':None,
            'URL_2':None
        },

        'base_carrinho':{
            'Id_do_cliente_': lambda df: df['requestid'],
            'Nome': lambda df: df['nome'],
            'CPF': None,
            'E_mail': lambda df: df['email'],
            'Fone_1': lambda df: df['phone'],
            'Fone_2': None,
            'Fone_3': None,
            'Fone_4': None,
            'ORIGEM': 'Carrinho Abandonado',
            'ORIGEM_DO_LEAD2': lambda df: df['programa'],
            'PROFISSAO': None,
            'ESPECIALIDADE': None,
            'LEAD_SCORING': None,
            'PRODUTO': lambda df: df['programa'],
            'A_LTIMO_REGISTRO': lambda df: df['data'],
            'LOGRADOURO': None,
            'BAIRRO': None,
            'CIDADE': None,
            'UF': None,
            'CEP': None,
            'DATA_NASCIMENTO': None,
            'DADOS_DO_PAGAMENTO': None,
            'MOTIVO_DE_CANCELAMENTO': None,
            'DATA_IMPORTA__O': None,
            'URL_2': lambda df: df['phone'] + ';' + df['nome'] + ';' + df['produtos']
            
        },

        'container_base':{

        },

        'bases_hubspot':{
            
        }
    }


    resultado = pd.DataFrame({
        col_destino: col_origem(df_base) if callable(col_origem) else col_origem
        for col_destino, col_origem in colunas_bases[base_type].items()
    })

    resultado['DATA_IMPORTA__O'] = dt.date.today()

    return resultado


## FUNÇÕES - Fim ##


print(bases_limpador)