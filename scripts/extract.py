#Este arquivo busca os dados consolidados dentro do transfermkt que autoriza de forma limitada alguns scrapings
#Exporta os dados em csv
#Os dados do transfermkt tem um problema ja que para eu conseguir os dados de valoração dos clubes da temporada 25 com os clubes participantes foi necessario buscar eles
#em outra tabela isso sera tratado mais a frente
#preciso filtrar os dados apenas para o ano de 2025, mas o transfermkt n tem filtros para isso, vou buscar manualmente no tratamento dos dados
#
import pandas as pd
import requests

url_valores_a_inicio_ano = 'https://www.transfermarkt.com.br/campeonato-brasileiro-serie-a/marktwerteverein/wettbewerb/BRA1/plus/1?stichtag=2025-01-01'
url_valores_b_inicio_ano = 'https://www.transfermarkt.com.br/campeonato-brasileiro-serie-a/marktwerteverein/wettbewerb/BRA2/plus/1?stichtag=2025-01-01'
url_valores_a_fim_ano = 'https://www.transfermarkt.com.br/campeonato-brasileiro-serie-a/marktwerteverein/wettbewerb/BRA1/stichtag/2025-12-15/plus/1'
url_valores_b_fim_ano = 'https://www.transfermarkt.com.br/campeonato-brasileiro-serie-b/marktwerteverein/wettbewerb/BRA2/stichtag/2025-12-15/plus/1'
url_valores_geral = 'https://www.transfermarkt.com/campeonato-brasileiro-serie-a/startseite/wettbewerb/BRA1/plus/?saison_id=2024'

url_classificacao = 'https://www.transfermarkt.com.br/campeonato-brasileiro-serie-a/tabelle/wettbewerb/BRA1?saison_id=2024'
headers = {'User-Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36'}

response_valores = requests.get(url = url_valores_a_inicio_ano, headers = headers)
print(response_valores)
response_valores_b = requests.get(url = url_valores_b_inicio_ano, headers = headers)
print(response_valores)
response_clubes = requests.get(url = url_classificacao, headers = headers)
print(response_clubes)

tables_valores = pd.read_html(url_valores_a_inicio_ano)
tables_valores_b = pd.read_html(url_valores_b_inicio_ano)
tables_valores_c = pd.read_html(url_valores_a_fim_ano)
tables_valores_d = pd.read_html(url_valores_b_fim_ano)
tables_valores_geral = pd.read_html(url_valores_geral)

df_valores_clubes_a = tables_valores[1]
df_valores_clubes_b = tables_valores_b[1]
df_valores_clubes_c = tables_valores_c[1]
df_valores_clubes_d = tables_valores_d[1]
df_valores_geral = tables_valores_geral[1]

tables_class = pd.read_html(url_classificacao)
df_class_clubes = tables_class[1]

df_valores_clubes_a.to_csv('/workspaces/moneyball-brasileirao/data/valores_a_inicio.csv', index = False)
df_valores_clubes_b.to_csv('/workspaces/moneyball-brasileirao/data/valores_b_inicio.csv', index = False)
df_valores_clubes_c.to_csv('/workspaces/moneyball-brasileirao/data/valores_c_fim.csv', index = False)
df_valores_clubes_d.to_csv('/workspaces/moneyball-brasileirao/data/valores_d_fim.csv', index = False)
df_valores_geral.to_csv('/workspaces/moneyball-brasileirao/data/valores_geral.csv', index = False)

df_class_clubes.to_csv('/workspaces/moneyball-brasileirao/data/classClubes.csv', index = False)



