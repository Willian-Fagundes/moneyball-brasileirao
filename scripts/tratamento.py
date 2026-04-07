import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import sqlite3
import re
import unicodedata

conn = sqlite3.connect('/workspaces/moneyball-brasileirao/databases/brasileirao.db')

df_a_inicio = pd.read_csv('/workspaces/moneyball-brasileirao/data/valores_a_inicio.csv', sep = ',')
df_b_inicio = pd.read_csv('/workspaces/moneyball-brasileirao/data/valores_b_inicio.csv', sep = ',')
df_a_fim = pd.read_csv('/workspaces/moneyball-brasileirao/data/valores_c_fim.csv', sep = ',')
df_b_fim = pd.read_csv('/workspaces/moneyball-brasileirao/data/valores_d_fim.csv', sep = ',')
df_class = pd.read_csv('/workspaces/moneyball-brasileirao/data/classClubes.csv', sep = ',')
df_geral = pd.read_csv('/workspaces/moneyball-brasileirao/data/valores_geral.csv', sep = ',')

#Funções auxiliares

import unicodedata

def limpar_texto(texto):
    if not isinstance(texto, str):
        return str(texto)
    
    texto = unicodedata.normalize('NFKD', texto)
    texto = "".join([c for c in texto if not unicodedata.combining(c)])
    texto = texto.lower()
    
    return texto

def processar_dataframes(df, coluna):
    df[coluna] = df[coluna].apply(limpar_texto)
    df[coluna] = df[coluna].str.replace(r'[^\w\s.]', '', regex=True)
    df[coluna] = df[coluna].str.replace(r'\s+', ' ', regex=True).str.strip()
    
    return df
def remove_null(df):
    df = df.iloc[:-1]
    df = df.drop(df.columns[0], axis=1)
    df = df.dropna(axis = 1, how = 'all')
    return df

def rename_colunas_inicio(df):
    df.columns = ['clube', 'liga', 'valor_inicio', 'numero_jogadores_inicio', 'valor_atual','numero_jogadores_atual', 'diferenca', '%']
    return df

def rename_colunas_fim(df):
    df.columns = ['clube', 'liga', 'valor_final', 'numero_jogadores_final', 'valor_atual','numero_jogadores_atual', 'diferenca', '%']
    return df

def normalizador_numericos(df):
    for coluna in df:
        if df[coluna].dtype == 'str' or df[coluna].dtype == 'object':
            mask_tem_numero = df[coluna].str.contains(r'\d', na=False)
            
            if mask_tem_numero.any():
                milhar = df[coluna].str.contains(r'k|mil', case=False, na=False)
                
                df[coluna] = df[coluna].str.replace(r'\.$', '', regex=True)
                
                df[coluna] = df[coluna].str.replace(r'[^\d.-]', '', regex=True)
                
                valores_numericos = pd.to_numeric(df[coluna], errors='coerce')
                
                df[coluna] = np.where(
                    milhar,
                    valores_numericos / 1000, 
                    valores_numericos
                )
            
    return df

def salvar_db(dfs):
    conn = sqlite3.connect('/workspaces/moneyball-brasileirao/databases/brasileirao.db')
    
    for nome_tabela, df in dfs.items():  
        df.to_sql(name=nome_tabela, con=conn, if_exists='replace', index=False)    
    conn.close()

#Ajustes das tabelas antes de carregar no db
df_a_inicio = remove_null(df_a_inicio)
df_b_inicio = remove_null(df_b_inicio)
df_a_fim = remove_null(df_a_fim)
df_b_fim = remove_null(df_b_fim)
df_a_inicio = rename_colunas_inicio(df_a_inicio)
df_b_inicio = rename_colunas_inicio(df_b_inicio)
df_a_fim = rename_colunas_fim(df_a_fim)
df_b_fim = rename_colunas_fim(df_b_fim)

df1_inicio = df_a_inicio[df_a_inicio['liga'] == 'Série A']
df2_inicio = df_b_inicio[df_b_inicio['liga'] == 'Série A']

df_inicio = pd.concat([df1_inicio, df2_inicio], ignore_index=True)

df1_final = df_a_fim[df_a_fim['liga'] == 'Série A']
df2_final = df_b_fim[df_b_fim['liga'] == 'Série A']

df_fim = pd.concat([df1_final, df2_final], ignore_index=True)

df_geral_1 = df_geral.iloc[:-1]
df_geral_2 = df_geral_1.dropna(axis= 1, how = 'all')
df_geral_2.columns = ['clube', 'numero_jogadores', 'media_idade', 'estrangeiros', 'media_valor_mercado_', 'valor_mercado_total']

df_class_1 = df_class.dropna(axis= 1, how = 'all')
df_class_2 = df_class_1.drop(df_class_1.columns[[0, 2]], axis=1)
df_class_2.columns = ['clube', 'v', 'e', 'd', 'gols', 'sg', 'pontos']

df_inicio = processar_dataframes(df_inicio, 'clube')
df_fim = processar_dataframes(df_fim, 'clube')
df_inicio = processar_dataframes(df_inicio, 'liga')
df_fim = processar_dataframes(df_fim, 'liga')
df_geral_2 = processar_dataframes(df_geral_2, 'clube')
df_class_2 = processar_dataframes(df_class_2, 'clube')

lista_df = [df_inicio, df_fim, df_geral_2]

lista_df_db = {'inicio_ano' : df_inicio,
               'fim_ano' : df_fim,
               'investimento_geral' : df_geral_2,
               'classificacao_final' : df_class_2}
for df in lista_df:
    normalizador_numericos(df)
# carregar tabelas em um sql para fazer analises posteriores
salvar_db(lista_df_db)

