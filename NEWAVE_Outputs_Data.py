import pandas as pd
from datetime import datetime


monthly_pld_limits = {'max':[751.73], 'min':[58.60]} # Máximos e mínimos regulatórios PLD mensais 2025
hourly_pld_limits = {'max':[1542.23], 'min':[58.60]} # Máximos e mínimos regulatórios PLD horários 2025


# RAW DATA NEWAVE
newave_data = pd.read_csv(r"Data\dados_nwlistop062025_totais.csv", sep=',', decimal='.')

newave_data['generation'] = newave_data['vl_hidro_generation'] + newave_data['vl_thermal_generation']

newave_data['year'] = newave_data['nu_period_day'].apply(lambda x: int(str(x)[:4]))

newave_data['month'] = newave_data['nu_period_day'].apply(lambda x: int(str(x)[-4:-2]))

newave_data['date'] = newave_data.apply(lambda row: datetime(year=row['year'], month=row['month'], day=1), axis=1)

newave_data.set_index('date', inplace=True)

newave_data.drop(columns=['Unnamed: 0','cd_price_model','PAT','vl_hidro_generation','vl_thermal_generation','month','year','nu_period_day',
                    'vl_earm_mwmed', 'vl_earm', 'vl_inflow_energy', 'cd_price_model'], inplace=True)

newave_data.rename(columns={'cd_serie':'scenario_nw','cd_subsystem':'submarket','vl_cmo':'pld_nw', 'generation': 'generation_MWm'}, inplace=True)



#PLD
newave_pld = newave_data[['scenario_nw', 'submarket', 'pld_nw']].copy()

newave_pld = newave_pld.loc[newave_pld.index>= '2026-01-01']

newave_pld['pld_nw'] = newave_pld['pld_nw'].clip(lower=monthly_pld_limits['min'][0], upper=monthly_pld_limits['max'][0])



#Generation Simulated Power Plant
newave_generation = newave_data[['scenario_nw', 'submarket', 'generation_MWm']].copy()

newave_generation = newave_generation.loc[newave_generation.index>= '2026-01-01']

newave_generation['t_hour'] = newave_generation.index.days_in_month*24 # type: ignore

newave_generation['generation_MWh'] = newave_generation['generation_MWm'] * newave_generation['t_hour']

newave_generation.drop(columns=['t_hour','generation_MWm'], inplace=True)


#Generation Non Simulated Power Plant
newave_RE_raw = pd.read_excel(r"C:\Code_TCC_UFF\TCC_Eng_Elet_UFF\Data\Generation_NEWAVE_EOL_UFV.xlsx",sheet_name="generation_MWh_vf")

newave_RE = newave_RE_raw.iloc[1:,:].copy()

newave_RE.rename(columns = {'Unnamed: 0':'date','EOL':'EOL_SE','UFV':'UFV_SE',
                                                'EOL.1':'EOL_S','UFV.1':'UFV_S',
                                                'EOL.2':'EOL_NE','UFV.2':'UFV_NE',
                                                'EOL.3':'EOL_N','UFV.3':'UFV_N',
                                                }, inplace=True)

newave_RE['date'] = pd.to_datetime(newave_RE['date'], format='%Y-%m-%d')

newave_RE.set_index('date', inplace=True)

newave_RE = newave_RE.loc[newave_RE.index>= '2026-01-01']

newave_RE.columns = newave_RE.columns.str.split('_', expand=True)

newave_RE.columns.names = ['Tecnology', 'Submarket']


newave_RE = newave_RE.stack(level=['Tecnology', 'Submarket'])

newave_generation_RE = newave_RE.reset_index(name='generation_MWh') # type: ignore
newave_generation_RE.set_index('date', inplace=True)

print('Fim')