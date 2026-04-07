from functools import reduce
from thefuzz import process

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

import sqlite3
import re
import unicodedata

#Funções auxiliares

def abrir_csv(caminho):
    df = pd.read_csv(caminho, sep = ',')
    return df

def rename_dados_clubes(df):
    df.columns = ['clube', 'numero_jogadores', 'media_idade', 'estrangeiros', 'media_valor_mercado_', 'valor_mercado_total']
    return df

def rename_class_clubes(df):
    df.columns = ['clube', 'rodadas','v', 'e', 'd', 'gols', 'sg', 'pontos']
    return df

def remove_null(df):
    df = df.iloc[:-1]
    df = df.drop(df.columns[0], axis=1)
    df = df.dropna(axis = 1, how = 'all')
    return df

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


def padronizar(nome_atual, nomes_padrao):
    melhor_match, score = process.extractOne(nome_atual, nomes_padrao)
    return melhor_match if score > 80 else nome_atual

def pipeline_tratamento(data_inicio, data_fim):
    for data in range(data_inicio, data_fim + 1):
        data_class = abrir_csv(f'/workspaces/moneyball-brasileirao/data/class_clubes_{data}.csv')
        data_clubes = abrir_csv(f'/workspaces/moneyball-brasileirao/data/dados_clubes_{data}.csv')

        data_class = remove_null(data_class)
        data_clubes = remove_null(data_clubes)

        data_class = rename_class_clubes(data_class)
        data_clubes = rename_dados_clubes(data_clubes)

        data_class = processar_dataframes(data_class, 'clube')
        data_clubes = processar_dataframes(data_clubes, 'clube')

        data_class[['gols_pro', 'gols_contra']] = (
            data_class['gols'].str.split(':', expand=True).astype(int)
        )
        data_class = data_class.drop(columns=['gols'])

       
        nomes_padrao = data_class['clube'].tolist()
        data_clubes['clube'] = data_clubes['clube'].apply(
            lambda nome: padronizar(nome, nomes_padrao)
        )

        data_clubes = normalizador_numericos(data_clubes)

        lista_df_db = {
            f'dados_clubes_{data}': data_clubes,
            f'dados_class_{data}': data_class,
        }

        salvar_db(lista_df_db)

if __name__ == "__main__":
    pipeline_tratamento(2015, 2025)