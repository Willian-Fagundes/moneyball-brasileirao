#Este arquivo busca os dados consolidados dentro do transfermkt que autoriza de forma limitada alguns scrapings
#Exporta os dados em csv
#Os dados do transfermkt tem um problema ja que para eu conseguir os dados de valoração dos clubes da temporada 25 com os clubes participantes foi necessario buscar eles
#em outra tabela isso sera tratado mais a frente
#preciso filtrar os dados apenas para o ano de 2025, mas o transfermkt n tem filtros para isso, vou buscar manualmente no tratamento dos dados
#Adicionei o user agent para evitar bloqueio do site, mas mesmo assim o site bloqueia o acesso, por isso busquei os dados manualmente e exportei para csv, o codigo de extração esta aqui apenas para registro do processo
#Agora que ja montei o pipeline de tratamento posso fazer um novo tipo de extraçao por ano dentro do transfermkt para fazer um modelo de previsão de resultados e uma 
# analise mais aprofundada sobre os impactos dos valores investidos ano a ano dentro dos clubes
import pandas as pd
import requests

def extract_data():
    for data in range(2015, 2026):
        url_valores = f'https://www.transfermarkt.com.br/campeonato-brasileiro-serie-a/startseite/wettbewerb/BRA1/plus/?saison_id={data}'
        url_classificacao = f'https://www.transfermarkt.com.br/campeonato-brasileiro-serie-a/tabelle/wettbewerb/BRA1/saison_id/{data}'
        headers = {'User-Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36'}
        response_valores = requests.get(url = url_valores, headers = headers)
        response_classificacao = requests.get(url = url_classificacao, headers = headers)
        print(f"Status code for valores {data}: {response_valores.status_code}")
        print(f"Status code for classificacao {data}: {response_classificacao.status_code}")
        if response_valores.status_code == 200 and response_classificacao.status_code == 200:
            tables_valores = pd.read_html(url_valores)
            tables_classificacao = pd.read_html(url_classificacao)
            df_valores_clubes = tables_valores[1]
            df_class_clubes = tables_classificacao[1]
            df_valores_clubes.to_csv(f'/workspaces/moneyball-brasileirao/data/dados_clubes_{data}.csv', index = False)
            df_class_clubes.to_csv(f'/workspaces/moneyball-brasileirao/data/class_clubes_{data}.csv', index = False)
        else:
            print(f"Failed to retrieve data for {data}. Status code: {response_valores.status_code} for valores, {response_classificacao.status_code} for classificacao.")
        
if __name__ == "__main__":
    extract_data()



