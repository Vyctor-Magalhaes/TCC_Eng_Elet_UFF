import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
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


#AJUSTAR, ALGO ERRADO
       # def hourly_price_scenario(self, start_date) -> pd.DataFrame:
       #        """
       #        Converts the PU scenarios to hourly price scenarios based on a given monthly PLD scenarios from the Newave model.
       #        """

       #        scenarios = self.generate_scenarios()

       #        processor = NewaveDataProcessor(newave_csv_path=general_input.newave_csv, re_excel_path=general_input.re_excel, start_date=start_date)
       #        processor.process_all_data()

       #        newave_pld = processor.pld_data.copy()


       #        first_date = newave_pld.index.min()
       #        last_date = pd.to_datetime(f'{newave_pld.index.max().year}-{newave_pld.index.max().month}-31 23:00:00')
       #        full_date = pd.date_range(start=first_date, end=last_date, freq='h')

       #        newave_pld_modified = newave_pld.copy()
       #        newave_pld_modified['year'] = newave_pld_modified.index.year.astype(int)
       #        newave_pld_modified['month'] = newave_pld_modified.index.month.astype(int)
       #        newave_pld_modified['day'] = newave_pld_modified.index.day.astype(int)

       #        # Using only SE/CO submarket for hourly price scenarios (load center from Brazil)
       #        newave_pld_modified = newave_pld_modified.loc[newave_pld_modified['submarket']=='SE/CO'].copy()
       #        newave_pld_modified.drop(columns=['submarket'], inplace=True)
       #        newave_pld_modified['scenario_nw'] = newave_pld_modified['scenario_nw'].astype(int)
       #        newave_pld_modified['pld_nw'] = newave_pld_modified['pld_nw'].astype(float)

       #        hourly_df = pd.DataFrame(index=full_date)
       #        hourly_df['year'] = hourly_df.index.year.astype(int)
       #        hourly_df['month'] = hourly_df.index.month.astype(int)
       #        hourly_df['day'] = hourly_df.index.day.astype(int)
       #        hourly_df['hour'] = hourly_df.index.hour.astype(int)

       #        scenarios['hour'] = scenarios.index
       #        scenarios['hour'] = scenarios['hour'].astype(int)

       #        hourly_df = hourly_df.merge(scenarios, on='hour', how='left')

       #        print("melting hourly df...")

       #        hourly_df_final = hourly_df.melt(
       #            id_vars=['year', 'month', 'day', 'hour'], 
       #            var_name='simulated_scenario',            
       #            value_name='hourly_profile'               
       #        )

       #        hourly_df_final['simulated_scenario'] = hourly_df_final['simulated_scenario'].astype(int)
       #        hourly_df_final['hourly_profile'] = hourly_df_final['hourly_profile'].astype(float)

       #        print("merging final df...")

       #        hourly_df_final_filter = hourly_df_final[
       #            ['year', 'month', 'hour', 'simulated_scenario','hourly_profile']
       #        ].drop_duplicates()

       #        final_df = newave_pld_modified.merge(
       #            hourly_df_final_filter,
       #            on=['year', 'month'],
       #            how='left'
       #        )
       #        print("building final df...")
       #        filter_df = ['year', 'month', 'day', 'hour']

       #        final_df['date'] = pd.to_datetime(
       #            final_df[filter_df]
       #        )

       #        final_df = final_df.set_index('date').drop(
       #            columns=[*filter_df]
       #        )
       #        print("calculating hourly price...")

       #        final_df['hourly_price'] = final_df['pld_nw'] * final_df['hourly_profile']

       #        final_df['hourly_price'] = final_df['hourly_price'].clip(
       #            lower=general_input.MONTHLY_PLD_LIMITS['min'][0],
       #            upper=general_input.MONTHLY_PLD_LIMITS['max'][0]
       #        )

       #        return final_df
       

       # def hourly_price_scenario_optimized(self, start_date) -> pd.DataFrame:
       #        """
       #        Converts PU scenarios to hourly price scenarios using chunked processing
       #        to avoid MemoryErrors.
       #        """
              
       #        # --- 1. Carregar e Preparar Dados (Operações Leves) ---
       #        print("Loading base data...")
       #        scenarios = self.generate_scenarios() # 24x21 DF
       #        processor = NewaveDataProcessor(
       #               newave_csv_path=general_input.newave_csv, 
       #               re_excel_path=general_input.re_excel, 
       #               start_date=start_date
       #        )
       #        processor.process_all_data()

       #        # --- 2. Preparar Tabela de Preços (96k rows, otimizada) ---
       #        # Filtra o submercado ANTES de qualquer operação pesada
       #        price_lookup = processor.pld_data.loc[
       #               processor.pld_data['submarket'] == 'SE/CO'
       #        ].copy()

       #        price_lookup['year'] = price_lookup.index.year.astype('int16')
       #        price_lookup['month'] = price_lookup.index.month.astype('int8')
       #        price_lookup['day'] = price_lookup.index.day.astype('int8') # Para o índice final
       #        price_lookup['scenario_nw'] = price_lookup['scenario_nw'].astype('int16')
       #        price_lookup['pld_nw'] = price_lookup['pld_nw'].astype('float32')
              
       #        # Seleciona apenas as colunas necessárias para o merge
       #        price_lookup = price_lookup[
       #               ['year', 'month', 'day', 'scenario_nw', 'pld_nw']
       #        ]

       #        # --- 3. Preparar Tabela de Perfis (504 rows, otimizada) ---
       #        # Não precisamos dos 24k de linhas (que incluía ano/mês).
       #        # Precisamos apenas do "molde" de 504 linhas (21 cenários * 24 horas).
              
       #        scenarios['hour'] = scenarios.index.astype('int8')
       #        profile_lookup = scenarios.melt(
       #               id_vars=['hour'], 
       #               var_name='simulated_scenario', 
       #               value_name='hourly_profile'
       #        ) # 504 linhas
              
       #        profile_lookup['simulated_scenario'] = profile_lookup['simulated_scenario'].astype('int8')
       #        profile_lookup['hourly_profile'] = profile_lookup['hourly_profile'].astype('float32')

              
       #        # --- 4. Processamento em Lotes (Chunking) ---
       #        print("Starting chunked processing by month...")
              
       #        # Pega os meses únicos (48) para iterar
       #        months_to_process = price_lookup[['year', 'month', 'day']].drop_duplicates()
              
       #        list_of_chunks = [] # Armazena os resultados de cada mês
              
       #        for _, row in months_to_process.iterrows():
       #               y = row['year']
       #               m = row['month']
       #               d = row['day'] # Geralmente 1, vindo do price_lookup
                     
       #               # a. Pega os 2000 preços deste mês
       #               prices_chunk = price_lookup[
       #               (price_lookup['year'] == y) & (price_lookup['month'] == m)
       #               ]
                     
       #               # b. Pega os 504 perfis (a tabela de perfis é constante, usamos .copy())
       #               profiles_chunk = profile_lookup.copy()
                     
       #               # c. Faz o Cross-Join (Produto Cartesiano) - *APENAS* para este mês
       #               #    (Cria ~1M de linhas em memória temporariamente)
       #               chunk_df = prices_chunk.merge(profiles_chunk, how='cross')

       #               # d. Calcula o preço horário
       #               chunk_df['hourly_price'] = chunk_df['pld_nw'] * chunk_df['hourly_profile']
                     
       #               # e. Aplica o CLIP
       #               chunk_df['hourly_price'] = chunk_df['hourly_price'].clip(
       #               lower=general_input.MONTHLY_PLD_LIMITS['min'][0],
       #               upper=general_input.MONTHLY_PLD_LIMITS['max'][0]
       #               )
                     
       #               # f. Cria o DatetimeIndex (muito mais rápido em 1M de linhas)
       #               chunk_df['date'] = pd.to_datetime(
       #               chunk_df[['year', 'month', 'day', 'hour']]
       #               )
                     
       #               # g. Limpa o chunk antes de salvar
       #               chunk_df = chunk_df.set_index('date')[[
       #               'scenario_nw', 'simulated_scenario', 'hourly_price'
       #               ]]
                     
       #               list_of_chunks.append(chunk_df)
       #               print(f"Processed chunk: {y}-{m}")

       #        # --- 5. Montagem Final ---
       #        print("Concatenating all chunks...")
       #        final_df = pd.concat(list_of_chunks)
              
       #        print("Process complete.")
       #        return final_df





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
       print(hourly_price_scenario_optimized.head())

       print("End of Scenario Generation Module.")








