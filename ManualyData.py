import pandas as pd
import requests
from io import StringIO
import os 

# Base da dados do GitHub
#https://github.com/diegonerii/Dados-Abertos-Setor-Eletrico-Brasileiro/blob/main/dadosAbertosSetorEletrico/app.ipynb

# URL From CCEE

url_pld_2023 = r'https://pda-download.ccee.org.br/e-qPA419SneVTnOQ04kEYA/content'
url_pld_2024 = r'https://pda-download.ccee.org.br/HlhpSDOXS_C0P7cmvJZl5A/content'

url_geracao_2023 = r'https://pda-download.ccee.org.br/rl-kcVbiRYetJRW1xNO47w/content'
url_geracao_2024 = r'https://pda-download.ccee.org.br/H1uCbMTQS6uf3-5Dy7xkng/content'

url_list = [url_pld_2023,url_pld_2024,url_geracao_2023,url_geracao_2024]

def fetch_csv_from_api(url):
    """
    Faz uma requisição GET para a URL da API e retorna o conteúdo CSV como DataFrame
    
    Parâmetros:
    url (str): URL da API que retorna um arquivo CSV
    
    Retorna:
    pd.DataFrame: DataFrame contendo os dados do CSV
    """
    try:
        # Faz a requisição GET para a API
        response = requests.get(url)
        response.raise_for_status()  # Verifica se houve erro na requisição
        
        # Usa StringIO para ler o conteúdo CSV diretamente
        csv_data = StringIO(response.text)
        
        # Lê o CSV como DataFrame
        df = pd.read_csv(csv_data, sep =";")
        

        
        
        return df
    
    except requests.exceptions.RequestException as e:
        print(f"Erro na requisição à API: {e}")
        return None
    except pd.errors.EmptyDataError:
        print("O arquivo CSV retornado está vazio.")
        return None
    except Exception as e:
        print(f"Erro inesperado: {e}")
        return None
    


    if __name__ == "main":
        df = fetch_csv_from_api(url_list[-1])
        print(df)
        print(df['FONTE_PRIMARIA'].unique())
        df_final = df[df['FONTE_PRIMARIA'].isin(['Eólica', 'Solar Fotovoltaica'])]
        df_final.groupby(["FONTE_PRIMARIA","SUBMERCADO","PERIODO_COMERCIALIZACAO","MES_REFERENCIA"]).sum()