import asyncio
import httpx
import pandas as pd
from datetime import datetime
from io import BytesIO
from typing import List, Union, Optional

class ONSHourlyGeneration:

    def __init__(self):
        """
        Initializes the ONSHourlyGeneration class to fetch hourly generation data from ONS.
        """
        self._cache = {}

    def _get_url(self, year: int, month: Optional[int] = None) -> str:
        """
        Constructs the URL for the specified year and month.
        """
        if not (2000 <= year <= datetime.now().year):
            raise ValueError(f"Year {year} must be between 2000 and the current year.")
        
        base_url = r'https://ons-aws-prod-opendata.s3.amazonaws.com/dataset/geracao_usina_2_ho/GERACAO_USINA-2_'
        
        if year >= 2022:
            if month is None:
                raise ValueError("Month must be specified for years >= 2022.")
            if not (1 <= month <= 12):
                raise ValueError(f"Month {month} must be between 1 and 12.")
            return f"{base_url}{year}_{str(month).zfill(2)}.parquet"
        else:
            return f"{base_url}{year}.parquet"

    async def _fetch_and_cache_data(self, client: httpx.AsyncClient, url: str) -> pd.DataFrame:
        """
        Asynchronously fetches a single Parquet file and caches it.
        """
        if url in self._cache:
            print(f"[{url}] Data found in cache. Skipping download.")
            return self._cache[url]

        try:
            response = await client.get(url, timeout=30)
            response.raise_for_status()
            data = BytesIO(response.content)
            df = pd.read_parquet(data)
            self._cache[url] = df
            print(f"[{url}] Downloaded and cached.")
            return df
        except httpx.HTTPStatusError as e:
            print(f"[{url}] failed: HTTP error - {e}")
        except httpx.RequestError as e:
            print(f"[{url}] failed: Request error - {e}")
        except Exception as e:
            print(f"[{url}] failed: Unexpected error - {e}")
        
        return pd.DataFrame()

    async def get_generation_data(self, years: List[int], months: Optional[List[int]] = None) -> pd.DataFrame:
        """
        Asynchronously downloads all generation data for the specified years/months
        and combines them into a single DataFrame.
        """
        urls = []
        current_year = datetime.now().year
        current_month = datetime.now().month

        for year in sorted(years):
            if year < 2022: # Arquivos anuais
                urls.append(self._get_url(year))
            elif months is not None: # Arquivos mensais, apenas se a lista de meses for fornecida
                for month in sorted(months):
                    # Para de adicionar tarefas para meses futuros do ano corrente
                    if year == current_year and month > current_month:
                        break
                    urls.append(self._get_url(year, month))

        if not urls:
            print("No URLs to fetch. Check your year/month selection.")
            return pd.DataFrame()

        async with httpx.AsyncClient() as client:
            tasks = [self._fetch_and_cache_data(client, url) for url in urls]
            results = await asyncio.gather(*tasks)
        
        return pd.concat(results, ignore_index=True)
    
    def _data_filter(self, df: pd.DataFrame) -> pd.DataFrame:
        
        if df.empty:
            print("DataFrame is empty. Returning an empty DataFrame.")
            return df
        
        df_filter = df.loc[df["nom_tipocombustivel"].isin(["Eólica", "Fotovoltaica"])]
        df_filter = df_filter[["din_instante", "nom_subsistema", "id_estado", "nom_tipocombustivel", "val_geracao"]]
        df_filter["val_geracao"] = pd.to_numeric(df_filter["val_geracao"], errors='coerce')

        df_filtered = df_filter.groupby(["din_instante", "nom_subsistema", "id_estado", "nom_tipocombustivel"]).sum().reset_index()
        df_filtered["date_part"] = df_filtered["din_instante"].dt.date
        df_filtered["time_part"] = df_filtered["din_instante"].dt.time

        return df_filtered


if __name__ == "__main__":
    
    async def main():
        ons_gen = ONSHourlyGeneration()
        try:
            # Exemplo 1: Download de anos e meses específicos
            print("--- Primeira chamada: 2021 e 2024 (meses 1 e 2) ---")
            df1 = await ons_gen.get_generation_data(years=[2021, 2024], months=[1, 2])
            print(f"\nDownload completo! Total de {len(df1)} registros na primeira chamada.")
            
            # Exemplo 2: Chamada com meses diferentes
            print("\n--- Segunda chamada: 2021 e 2024 (meses 1, 3 e 4) ---")
            df2 = await ons_gen.get_generation_data(years=[2021, 2024], months=[1, 3, 4])
            print(f"\nDownload completo! Total de {len(df2)} registros na segunda chamada.")

            print(f"Menor data do DF: {df1['din_instante'].min()}")
            print(f"Maior data do DF: {df1['din_instante'].max()}")
            
        except ValueError as e:
            print(f"Erro na entrada de dados: {e}")

    asyncio.run(main())