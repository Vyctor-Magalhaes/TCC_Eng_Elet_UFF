import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path
from typing import List
import general_input
from NEWAVE_Outputs_Data import NewaveDataProcessor

class ScenarioGenerator:
    
       def __init__(self, 
                     scenarios_n: int, 
                     base_scenario: List[float], 
                     average_scenario: List[float], 
                     duck_curve_scenario: List[float], 
                     canyon_curve_scenario: List[float]):
              """
              Initializes the scenario generator with base parameters and data.
              """
              self.scenarios_n = scenarios_n
              self.total_intervals = self.scenarios_n - 1
              self.hours_n = 24 
              self.hours_range = np.array(range(self.hours_n))
              self.columns_name = np.array(range(1, self.scenarios_n + 1, 1)).astype(str)
              self.index_range = self.hours_range.astype(str)

              self.base_scenario = base_scenario
              self.average_scenario_full = average_scenario
              self.duck_curve_scenario = duck_curve_scenario
              self.canyon_curve_scenario = canyon_curve_scenario
              
              self.anchor_list = [
              self.base_scenario, 
              self.average_scenario_full, 
              self.duck_curve_scenario, 
              self.canyon_curve_scenario
              ]

       def generate_scenarios(self) -> pd.DataFrame:
              """
              Generates the interpolated scenarios DataFrame.
              """

              dist_1_2 = np.sum(np.abs(np.asarray(self.average_scenario_full) - np.asarray(self.base_scenario)))
              dist_2_3 = np.sum(np.abs(np.asarray(self.duck_curve_scenario) - np.asarray(self.average_scenario_full)))
              dist_3_4 = np.sum(np.abs(np.asarray(self.canyon_curve_scenario) - np.asarray(self.duck_curve_scenario)))
              total_dist = dist_1_2 + dist_2_3 + dist_3_4

              cum_frac_1 = 0.0
              cum_frac_2 = dist_1_2 / total_dist
              cum_frac_3 = (dist_1_2 + dist_2_3) / total_dist
              cum_frac_4 = 1.0

              anchor_indices = [
              0, 
              int(round(cum_frac_2 * self.total_intervals)),
              int(round(cum_frac_3 * self.total_intervals)),
              self.total_intervals 
              ]
              
              scenarios = pd.DataFrame(index=self.index_range,
                                   columns=self.columns_name,
                                   dtype=float)

              for i, anchor_col_data in enumerate(self.anchor_list):
                     col_index_pos = anchor_indices[i]
                     scenarios.iloc[:, col_index_pos] = np.asarray(anchor_col_data)
              
              scenarios.interpolate(method='linear', axis=1, inplace=True)

              return scenarios

       def plot_scenarios(self, scenarios: pd.DataFrame):
              """
              Plots the generated scenarios.
              """

              anchor_indices = [1, 2, 10, 21]
              anchor_list_name = [
              'Base', 
              'Média', 
              'Duck Curve', 
              'Canyon Curve'
              ]
              map_anchor = dict(zip(anchor_indices, anchor_list_name))

              plt.style.use('seaborn-v0_8-whitegrid')
              fig, ax = plt.subplots(figsize=(10, 6))

              for col in scenarios.columns:
                     col_int = int(col) 

                     if col_int in map_anchor:
                            
                            label_name = map_anchor[col_int] 
                            ax.plot(scenarios.index.astype(int), 
                                   scenarios[col], 
                                   label=label_name, 
                                   linewidth=1.5, 
                                   zorder=10)
                     else:
                            ax.plot(scenarios.index.astype(int), 
                                   scenarios[col], 
                                   alpha=0.7,
                                   linewidth=1.0,
                                   color='gray',
                                   zorder=2)
              
              plt.title('Evolução dos Cenários')
              plt.xlabel('Hora')
              plt.ylabel('PU (%)')
              plt.legend(title='Cenários', bbox_to_anchor=(1.05, 1), loc='upper left')
              plt.tight_layout()
              plt.show()

       def hourly_price_scenario_optimized(self, start_date) -> None: # Retun None due to chunked processing
              """
              Converts PU scenarios to hourly price scenarios using chunked processing
              by price scenario (scenario_nw) and saves each to Parquet to avoid MemoryErrors.
              """

              scenarios = self.generate_scenarios() 
              processor = NewaveDataProcessor(
                     newave_csv_path=general_input.newave_csv, 
                     re_excel_path=general_input.re_excel, 
                     start_date=start_date
              )
              processor.process_all_data()

              price_lookup = processor.pld_data.loc[
                     processor.pld_data['submarket'] == 'SE/CO'
              ].copy()

              price_lookup['year'] = price_lookup.index.year.astype(int)    # type: ignore
              price_lookup['month'] = price_lookup.index.month.astype(int)  # type: ignore

              price_lookup['scenario_nw'] = price_lookup['scenario_nw'].astype(int)
              price_lookup['pld_nw'] = price_lookup['pld_nw'].astype('float32')
              
              price_lookup = price_lookup[
                     ['year', 'month', 'scenario_nw', 'pld_nw']
              ].drop_duplicates()

              first_date = processor.pld_data.index.min()
              last_date = pd.to_datetime(f'{processor.pld_data.index.max().year}-{processor.pld_data.index.max().month}-01') + pd.DateOffset(months=1) - pd.DateOffset(hours=1)
              full_date = pd.date_range(start=first_date, end=last_date, freq='h')

              hourly_df = pd.DataFrame(index=full_date)
              hourly_df['year'] = hourly_df.index.year.astype('int16')  # type: ignore
              hourly_df['month'] = hourly_df.index.month.astype('int8') # type: ignore
              hourly_df['day'] = hourly_df.index.day.astype('int8')     # type: ignore
              hourly_df['hour'] = hourly_df.index.hour.astype('int8')   # type: ignore
              
              scenarios['hour'] = scenarios.index.astype('int8')
              
              hourly_df = hourly_df.merge(scenarios, on='hour', how='left')

              profile_base_long = hourly_df.melt(
                     id_vars=['year', 'month', 'day', 'hour'], 
                     var_name='simulated_scenario', 
                     value_name='hourly_profile'
              )
              profile_base_long['simulated_scenario'] = profile_base_long['simulated_scenario'].astype('int8')
              profile_base_long['hourly_profile'] = profile_base_long['hourly_profile'].astype('float32')

              
              price_scenarios_list = price_lookup['scenario_nw'].unique()
              
              output_directory = "cenarios_horarios_finais"
              Path(output_directory).mkdir(exist_ok=True)
              
              print(f"Starting chunked processing for {len(price_scenarios_list)} price scenarios...")

              for price_id in price_scenarios_list:
                     
                     current_price_scenario = price_lookup[price_lookup['scenario_nw'] == price_id]

                     temp_df = profile_base_long.merge(
                     current_price_scenario,
                     on=['year', 'month'],
                     how='left' )

                     temp_df['hourly_price'] = temp_df['pld_nw'] * temp_df['hourly_profile']

                     temp_df['hourly_price'] = temp_df['hourly_price'].clip(
                     lower=general_input.MONTHLY_PLD_LIMITS['min'][0],
                     upper=general_input.MONTHLY_PLD_LIMITS['max'][0]
                     )

                     final_chunk = temp_df[[
                     'year', 'month', 'day', 'hour', 
                     'scenario_nw', 'simulated_scenario', 
                     'hourly_price'
                     ]].copy()

                     output_filename = f"{output_directory}/price_scenario_{price_id}.parquet"
                     final_chunk.to_parquet(output_filename, index=False)

                     if int(price_id) % 100 == 0: 
                            print(f"Processed and saved: price scenario {price_id}/{len(price_scenarios_list)}")
              
              return None

if __name__ == "__main__":

       generator = ScenarioGenerator(
              scenarios_n=general_input.scenarios_n,
              base_scenario=general_input.base_scenario, # type: ignore
              average_scenario=general_input.average_scenario_full,
              duck_curve_scenario=general_input.duck_curve_scenario,
              canyon_curve_scenario=general_input.canyon_curve_scenario
       )

       scenarios = generator.generate_scenarios()

       generator.plot_scenarios(scenarios)
       #     print(scenarios)

       start_date='2026-01-01'
       # hourly_price_scenarios = generator.hourly_price_scenario(start_date=start_date)
       # print(hourly_price_scenarios.head())
       hourly_price_scenario_optimized = generator.hourly_price_scenario_optimized(start_date=start_date)
       # print(hourly_price_scenario_optimized.head())

       print("End of Scenario Generation Module.")








