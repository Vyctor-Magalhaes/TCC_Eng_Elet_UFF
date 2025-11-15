import pandas as pd
from typing import Tuple, Dict, Any
from matplotlib import cm
import os
from main import ElectricSectorOpenData, ONSHourlyGeneration, HistoricalDataProcessor, general_input
from NEWAVE_Outputs_Data import NewaveDataProcessor


class EnergyAnalysisService:

    def __init__(self, 
                 historical_data_processor: HistoricalDataProcessor, 
                 newave_processor: NewaveDataProcessor):

        self.historical_data_processor = historical_data_processor
        self.newave_processor = newave_processor

        self.total_shape: pd.DataFrame = pd.DataFrame()
        self.wind_shape: pd.DataFrame = pd.DataFrame()
        self.solar_shape: pd.DataFrame = pd.DataFrame()
        self.total_avg: pd.DataFrame = pd.DataFrame()
        self.wind_avg: pd.DataFrame = pd.DataFrame()
        self.solar_avg: pd.DataFrame = pd.DataFrame()
        self.historical_hourly_price: pd.DataFrame = pd.DataFrame()
        self.avg_historical_shape: pd.DataFrame = pd.DataFrame()
        self.avg_historical_prices: pd.DataFrame = pd.DataFrame()
        self.future_prices: pd.DataFrame = pd.DataFrame()

    def _calculate_monthly_avg_and_shape(self, filtered_series: pd.Series) -> Tuple[pd.DataFrame, pd.DataFrame]:

        pivoted_data = filtered_series.unstack('submarket')
        
        avg_hourly_data = pivoted_data.groupby(
            [pivoted_data.index.year, pivoted_data.index.month, pivoted_data.index.hour] # type: ignore
        ).mean()
        
        avg_hourly_data.index.names = ['year', 'month', 'hour']
        
        avg_hourly_shape = avg_hourly_data / avg_hourly_data.groupby(level=['year', 'month']).mean()

        avg_hourly_shape_final = avg_hourly_shape.copy()
        avg_hourly_shape_final = avg_hourly_shape_final / avg_hourly_shape_final.groupby(level=['year', 'month']).sum()
        
        return avg_hourly_shape_final, avg_hourly_data

    def _apply_monthly_shape(self, monthly_gen_nw: pd.DataFrame, final_shape: pd.DataFrame) -> pd.DataFrame:

        monthly_gen = monthly_gen_nw.copy()
        monthly_gen['year'] = monthly_gen.index.year # type: ignore
        monthly_gen['month'] = monthly_gen.index.month # type: ignore
        monthly_gen = monthly_gen.rename(columns={'Submarket': 'submarket'})

        lookup_gen = monthly_gen[['year', 'month', 'submarket', 'generation_MWh']]

        gen_shape = final_shape.copy()
        
        df_shape_long = gen_shape.stack().reset_index()
        df_shape_long = df_shape_long.rename(columns={
            'level_2': 'submarket', 
            0: 'hourly_profile'
        })
        
        df_final = lookup_gen.merge(
            df_shape_long,
            on=['month', 'submarket'],
            how='left'
        )

        df_final['hourly_generation'] = df_final['generation_MWh'] * df_final['hourly_profile']
        
        df_final['day'] = 1
        
        df_final['date'] = pd.to_datetime(df_final[['year', 'month', 'day', 'hour']])
        
        final_gen = df_final.set_index('date')[[
            'submarket', 'hourly_generation'
        ]]

        return final_gen.sort_index()

    def calculate_generation_monthly_shapes(self, start_date: str, end_date: str) -> Tuple[pd.DataFrame, ...]:

        historical_hourly_generation = self.historical_data_processor.historical_hourly_generation_processing(
            start_date=start_date, 
            end_date=end_date
        )
        total_generation, generation_RE, hourly_data = self.historical_data_processor.hourly_data_treatment(historical_hourly_generation)

        total_generation_filtered = total_generation.query( # type: ignore
            "@start_date <= date <= @end_date"
        )
        generation_RE_filtered = generation_RE.query( # type: ignore
            "@start_date <= date <= @end_date"
        )

        (self.total_shape, self.total_avg) = self._calculate_monthly_avg_and_shape(total_generation_filtered) # type: ignore

        gen_wind = generation_RE_filtered['wind_generation_MWh']
        (self.wind_shape, self.wind_avg) = self._calculate_monthly_avg_and_shape(gen_wind)

        gen_solar = generation_RE_filtered['solar_generation_MWh']
        (self.solar_shape, self.solar_avg) = self._calculate_monthly_avg_and_shape(gen_solar)        

        return (
            self.total_shape, self.wind_shape, self.solar_shape,
            self.total_avg, self.wind_avg, self.solar_avg
        )
    

    def calculate_final_monthly_generation(self, solar_shape: pd.DataFrame, wind_shape: pd.DataFrame, start_date: str = '2024-01-01' , end_date: str = '2024-12-31') -> pd.DataFrame:

        if solar_shape.empty or wind_shape.empty:
            (total_shape, wind_shape, solar_shape, total_avg, wind_avg, solar_avg) = self.calculate_generation_monthly_shapes( start_date=start_date, end_date=end_date )         

        self.newave_processor.process_all_data()
            
        eol_nw_gen = self.newave_processor.re_generation_data.query("Tecnology == 'EOL' ").drop("Tecnology", axis = 1)
        solar_nw_gen = self.newave_processor.re_generation_data.query("Tecnology == 'UFV' ").drop("Tecnology", axis = 1)

        final_solar_shape = solar_shape.reset_index().groupby(by = ['month','hour']).mean().drop('year',axis = 1)
        final_wind_shape = wind_shape.reset_index().groupby(by = ['month','hour']).mean().drop('year',axis = 1)

        monthly_wind_gen = self._apply_monthly_shape(eol_nw_gen, final_wind_shape)
        monthly_solar_gen = self._apply_monthly_shape(solar_nw_gen, final_solar_shape)

        monthly_solar_gen = monthly_solar_gen.set_index('submarket', append=True).rename(columns = {'hourly_generation':'solar_generation_MWh'})
        monthly_wind_gen = monthly_wind_gen.set_index('submarket', append=True).rename(columns = {'hourly_generation': 'wind_generation_MWh'})

        future_hourly_re_gen = pd.concat([monthly_wind_gen.sort_index(),monthly_solar_gen.sort_index()], axis = 1)

        return future_hourly_re_gen


    def calculate_price_historical_shape(self, start_date: str, end_date: str) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:

        historical_hourly_price = self.historical_data_processor.historical_hourly_pld_processing()
        historical_hourly_price = historical_hourly_price[(historical_hourly_price.index >= start_date) & (historical_hourly_price.index <= end_date)]

        price_aggregated = historical_hourly_price.groupby([historical_hourly_price.index, 'submarket'])['Hourly_PLD'].mean()

        pivoted_prices = price_aggregated.unstack('submarket')
        avg_historical_prices = pivoted_prices.groupby(pivoted_prices.index.hour).mean()
        avg_historical_prices.index.name = None
        avg_historical_shape = avg_historical_prices / avg_historical_prices.mean()

        self.historical_hourly_price = pivoted_prices
        self.avg_historical_shape = avg_historical_shape
        self.avg_historical_prices = avg_historical_prices

        return self.historical_hourly_price, self.avg_historical_shape, self.avg_historical_prices
    


    def consolidate_future_price_scenarios(self,path_scenarios:str = r'C:\Code_TCC_UFF\TCC_Eng_Elet_UFF\cenarios_horarios_finais') -> pd.DataFrame:

        list_of_processed_dfs = []
        
        for pqt in os.listdir(path_scenarios):
            file_path = os.path.join(path_scenarios, pqt)
            df_price = pd.read_parquet(file_path)

            df_price_filtered = df_price.query('day == 1').copy()

            df_price_filtered['date'] = pd.to_datetime(
            df_price_filtered[['year', 'month', 'day', 'hour']]
            )

            df_processed = df_price_filtered.set_index('date').drop(
            columns=['year', 'month', 'day', 'hour']
            )

            list_of_processed_dfs.append(df_processed)

            print(f"Processed file: {pqt}")

        self.future_prices = pd.concat(list_of_processed_dfs)

        return self.future_prices

if __name__ == "__main__":

    electric_sector_client_ccee = ElectricSectorOpenData("ccee")
    electric_sector_client_ons = ElectricSectorOpenData("ons")
    ons_generation_client = ONSHourlyGeneration()
    processor = NewaveDataProcessor(newave_csv_path=general_input.newave_csv, re_excel_path=general_input.re_excel)
    
    start_date='2024-01-01'
    end_date='2024-12-31'

    historical_data_processor = HistoricalDataProcessor(electric_sector_client_ccee, electric_sector_client_ons, ons_generation_client)

    analysis_service = EnergyAnalysisService(
        historical_data_processor=historical_data_processor,
        newave_processor=processor
    )
    
    historical_hourly_price, avg_historical_shape, avg_historical_prices = analysis_service.calculate_price_historical_shape(
        start_date=start_date, 
        end_date=end_date
    )
    
    (total_shape, wind_shape, solar_shape,
    total_avg, wind_avg, solar_avg) = analysis_service.calculate_generation_monthly_shapes(
        start_date=start_date, 
        end_date=end_date
    )

    future_hourly_re_gen = analysis_service.calculate_final_monthly_generation(solar_shape = solar_shape, wind_shape = wind_shape, start_date=start_date, end_date=end_date)

    future_prices = analysis_service.consolidate_future_price_scenarios()

    print(future_hourly_re_gen)

    print("End")