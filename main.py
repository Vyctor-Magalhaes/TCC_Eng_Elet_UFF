from datetime import datetime
import pandas as pd
from OpenDataSEB import ElectricSectorOpenData
from ONS_Hourly_Generation import ONSHourlyGeneration
import matplotlib.pyplot as plt
import asyncio
import os



class HistoricalDataProcessor:

    def __init__(self, electric_sector_client_ccee, electric_sector_client_ons, ons_hourly_generation_client):

        self.ccee_client = electric_sector_client_ccee
        self.ons_client = electric_sector_client_ons
        self.ons_generation_client = ons_hourly_generation_client

    def historical_hourly_pld_processing(self):

        hourly_pld_raw = self.ccee_client.download_full_product_data("pld_horario")

        hourly_pld = hourly_pld_raw.copy()

        hourly_pld['MES_REFERENCIA'] = pd.to_datetime(hourly_pld['MES_REFERENCIA'], format='%Y%m')

        dates = {
            'year': hourly_pld['MES_REFERENCIA'].dt.year,
            'month': hourly_pld['MES_REFERENCIA'].dt.month,
            'day': hourly_pld['DIA'],
            'hour': hourly_pld['HORA']
        }

        hourly_pld['date'] = pd.to_datetime(dates) # type: ignore

        hourly_pld.set_index('date', inplace=True)

        hourly_pld.drop(columns=['MES_REFERENCIA', 'DIA', 'HORA','_id','PERIODO_COMERCIALIZACAO'], inplace=True)

        hourly_pld.rename(columns={"SUBMERCADO": "submarket","PLD_HORA": "Hourly_PLD"}, inplace=True)

        # hourly_pld.drop_duplicates(inplace=True)

        submarket_map = {
        'NORDESTE': 'NE',
        'NORTE': 'N',
        'SUL': 'S',
        'SUDESTE': 'SE'
        }

        hourly_pld['submarket'] = hourly_pld['submarket'].map(submarket_map)

        hourly_pld = hourly_pld.loc[hourly_pld.index < '2025-07-01']

        if hourly_pld is None or hourly_pld.empty:

            print("Hourly PLD DataFrame is empty after processing. Returning an empty DataFrame.")

        return hourly_pld
    

    def download_hourly_generation(self, start_date: str = '2010-01-01', end_date: str = '2025-07-01'):

        date_range = pd.date_range(start=start_date, end=end_date, freq='MS')

        years_gen = date_range.year.unique().tolist()

        months_gen = date_range.month.unique().tolist()

        async def main():

            try:
                hourly_generation_downloading = await self.ons_generation_client.get_generation_data(years=years_gen, months=months_gen)
                
                return hourly_generation_downloading

            except ValueError as e:
                print(f"Error in download data: {e}")

        hourly_generation_raw = asyncio.run(main())

        if hourly_generation_raw is None or hourly_generation_raw.empty:

            print("Hourly Generation DataFrame is empty after processing. Returning an empty DataFrame.")

        return hourly_generation_raw
    

    def historical_hourly_generation_processing(self, clean_version: bool = True, start_date: str = '2010-01-01', end_date: str = '2025-07-01'):

        """ Start and end date included"""

        start_date = pd.to_datetime(start_date) # type: ignore
        end_date = pd.to_datetime(end_date) # type: ignore

        hourly_generation_raw = self.download_hourly_generation(start_date,end_date)
        
        hourly_generation = hourly_generation_raw.copy() # type: ignore

        # power_plant_type = [
        #     'TIPO I', 
        #     'TIPO II-A', 
        #     'TIPO II-B', 
        #     'TIPO II-C'
        # ]

        # hourly_generation = hourly_generation.query( "cod_modalidadeoperacao in @power_plant_type")
        
        if clean_version:
            drop_cols = ['nom_subsistema', 'nom_estado', #'cod_modalidadeoperacao',
                                            'nom_tipocombustivel','nom_usina','id_ons','id_estado','ceg']
        
        else:
            drop_cols = ['nom_subsistema', 'nom_estado', #'cod_modalidadeoperacao',
                                            'nom_tipocombustivel','nom_usina','id_ons']
        
        rename_cols = {"din_instante": "date","id_subsistema": "submarket",
                                            "val_geracao": "generation_MWh", 'nom_tipousina': "gen_technology"}

        hourly_generation = (
            hourly_generation
            .drop(columns=drop_cols)          
            .rename(columns=rename_cols)     
        )

        hourly_generation.set_index('date', inplace=True)

        hourly_generation = hourly_generation.loc[(hourly_generation.index >= start_date) & (hourly_generation.index <= end_date)]

        if hourly_generation is None or hourly_generation.empty:

            print("Hourly Generation DataFrame is empty after processing. Returning an empty DataFrame.")

        return hourly_generation


    def hourly_data_treatment(self,hourly_generation: pd.DataFrame = pd.DataFrame(), hourly_prices: pd.DataFrame = pd.DataFrame()):

        total_generation = pd.DataFrame()
        generation_RE = pd.DataFrame()

        try:
            generation = hourly_generation.copy()   

            # power_plant_type = [
            # 'TIPO I', 
            # 'TIPO II-A', 
            # 'TIPO II-B', 
            # 'TIPO II-C'
            # ]

            # generation = generation.query( "cod_modalidadeoperacao in @power_plant_type")

            generation.drop(columns='cod_modalidadeoperacao', inplace=True)

            generation['generation_MWh'] = pd.to_numeric(generation['generation_MWh'], errors='coerce')

            grouped_by_tech = generation.groupby(
                            [generation.index, 'submarket', 'gen_technology']
                        ).sum()


            total_generation = grouped_by_tech.groupby(level=['date', 'submarket']).sum()
            # total_generation.reset_index('submarket', inplace=True)
            total_generation.rename(columns={"generation_MWh": "total_generation_MWh"}, inplace=True)

            wind_generation = grouped_by_tech.query("gen_technology == 'EOLIELÉTRICA'")
            wind_generation = wind_generation.droplevel('gen_technology')
            # wind_generation.reset_index('submarket', inplace=True)
            wind_generation.rename(columns={"generation_MWh": "wind_generation_MWh"}, inplace=True)

            solar_generation = grouped_by_tech.query("gen_technology == 'FOTOVOLTAICA'")
            solar_generation = solar_generation.droplevel('gen_technology')
            # solar_generation.reset_index('submarket', inplace=True)
            solar_generation.rename(columns={"generation_MWh": "solar_generation_MWh"}, inplace=True)

            generation_RE = wind_generation.join(solar_generation, how='outer')

        except:
            if hourly_generation is None or hourly_generation.empty:
                print("Hourly Generation DataFrame is empty. Trying to continue the process using only prices.")
            
            else:
                print("Error in processing hourly generation data. Returning an empty DataFrame.")
                return pd.DataFrame()           


        try:

            prices = hourly_prices.copy()

            prices = prices.loc[(prices.index >= total_generation.index.min()[0]) & (prices.index <= total_generation.index.max()[0])]

            prices = prices.groupby([prices.index, 'submarket']
                                        ).sum()

            price_gen = prices.join(total_generation, how='outer')

            hourly_data = price_gen.join(generation_RE, how='outer').fillna(0)

        except:
            if hourly_prices is None or hourly_prices.empty:
                print("Hourly Prices DataFrame is empty. Returning only generation data.")
                return total_generation, generation_RE, pd.DataFrame()

            else:
                print("Error in processing hourly prices data. Returning an empty DataFrame.")
                return pd.DataFrame()

        if hourly_data is None or hourly_data.empty:

            print("Hourly Data DataFrame is empty after processing. Returning an empty DataFrame.")

        return total_generation, generation_RE, hourly_data
    
    
    
class CaptureIndicators:

    # def __init__(self, historical_data_processor_client, future_data_processor_client):
    def __init__(self, historical_data_processor_client):
        self.historical_data_processor = historical_data_processor_client
        # self.future_data_processor = future_data_processor_client


    def capture_prices_calculate(self,hourly_data_raw: pd.DataFrame, start_date: str = '2010-01-01', end_date: str = '2025-07-01'):

        hourly_data = hourly_data_raw.query(
                                        "@start_date <= date <= @end_date"
                                        )
        
        hourly_data["cap_pric_wind"] = hourly_data['wind_generation_MWh'] * hourly_data['Hourly_PLD']
        wind_cap_prices = hourly_data['cap_pric_wind'].groupby(['submarket']).sum()/hourly_data['wind_generation_MWh'].groupby(['submarket']).sum()


        hourly_data["cap_pric_sol"] = hourly_data['solar_generation_MWh'] * hourly_data['Hourly_PLD']
        solar_cap_prices = hourly_data['cap_pric_sol'].groupby(['submarket']).sum()/hourly_data['solar_generation_MWh'].groupby(['submarket']).sum()

        return wind_cap_prices, solar_cap_prices, hourly_data
    

    def capture_rate_calculate(self, hourly_data_raw: pd.DataFrame, start_date: str = '2010-01-01', end_date: str = '2025-07-01'):

        wind_cap_prices, solar_cap_prices, hourly_data = self.capture_prices_calculate(hourly_data_raw, start_date, end_date)

        base_prices = hourly_data['Hourly_PLD'].groupby(['submarket']).mean()

        solar_cap_rate = solar_cap_prices/base_prices
        wind_cap_rate = wind_cap_prices/base_prices

        return wind_cap_rate, solar_cap_rate, wind_cap_prices, solar_cap_prices


if __name__ == "__main__":

    electric_sector_client_ccee = ElectricSectorOpenData("ccee")
    electric_sector_client_ons = ElectricSectorOpenData("ons")
    ons_generation_client = ONSHourlyGeneration()

    start_date='2024-01-01'
    end_date='2024-12-31'

    historical_data_processor = HistoricalDataProcessor(electric_sector_client_ccee, electric_sector_client_ons, ons_generation_client)

    historical_hourly_price = historical_data_processor.historical_hourly_pld_processing()

    print(historical_hourly_price)

    historical_hourly_generation = historical_data_processor.historical_hourly_generation_processing(start_date=start_date, end_date=end_date)

    print(historical_hourly_generation)

    hourly_data = historical_data_processor.hourly_data_treatment(historical_hourly_generation, historical_hourly_price)

    capture_indicators = CaptureIndicators(historical_data_processor)

    wind_cap_rate, solar_cap_rate, wind_cap_prices, solar_cap_prices = capture_indicators.capture_rate_calculate(hourly_data, start_date=start_date, end_date= end_date)

    print(wind_cap_rate, solar_cap_rate, wind_cap_prices, solar_cap_prices)


    print("End of Main Processing.")