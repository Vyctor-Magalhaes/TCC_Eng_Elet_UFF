import pandas as pd
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional
import general_input

class NewaveDataProcessor:
    """
    Processes raw NEWAVE data and renewable energy (RE) generation data.
    """

    def __init__(self, newave_csv_path: Path, re_excel_path: Path, start_date: str = '2026-01-01'):
        """
        Initializes the data processor.

        Args:
            newave_csv_path (Path): Path to the CSV file 'dados_nwlistop...'.
            re_excel_path (Path): Path to the Excel file 'Generation_NEWAVE_EOL_UFV.xlsx'.
            start_date (str): Start date to filter the data (format 'YYYY-MM-DD').
        """
        self.newave_csv_path = newave_csv_path
        self.re_excel_path = re_excel_path
        self.start_date = start_date

        # Attributes to store the resulting DataFrames
        self.raw_data: pd.DataFrame = pd.DataFrame()
        self.pld_data: pd.DataFrame = pd.DataFrame()
        self.simulated_generation_data: pd.DataFrame = pd.DataFrame()
        self.re_generation_data: pd.DataFrame = pd.DataFrame()

    def _load_and_preprocess_newave_raw(self) -> pd.DataFrame:
        """Loads and preprocesses the main NEWAVE CSV file."""
        
        data = pd.read_csv(self.newave_csv_path, sep=',', decimal='.')

        data['generation'] = data['vl_hidro_generation'] + data['vl_thermal_generation']
        data['year'] = data['nu_period_day'].apply(lambda x: int(str(x)[:4]))
        data['month'] = data['nu_period_day'].apply(lambda x: int(str(x)[-4:-2]))
        data['date'] = data.apply(lambda row: datetime(year=row['year'], month=row['month'], day=1), axis=1)
        data.set_index('date', inplace=True)

        cols_to_drop = [
            'Unnamed: 0', 'cd_price_model', 'PAT', 'vl_hidro_generation',
            'vl_thermal_generation', 'month', 'year', 'nu_period_day',
            'vl_earm_mwmed', 'vl_earm', 'vl_inflow_energy', 'cd_price_model'
        ]
        
        data.drop(columns=cols_to_drop, inplace=True)

        data.rename(columns={
            'cd_serie': 'scenario_nw',
            'cd_subsystem': 'submarket',
            'vl_cmo': 'pld_nw',
            'generation': 'generation_MWm'
        }, inplace=True)

        return data

    def _process_pld(self) -> pd.DataFrame:
        """Extracts and processes PLD data from the raw DataFrame."""
        pld_data = self.raw_data[['scenario_nw', 'submarket', 'pld_nw']].copy()
        pld_data = pld_data.loc[pld_data.index >= self.start_date]
        
        pld_data['pld_nw'] = pld_data['pld_nw'].clip(
            lower=general_input.MONTHLY_PLD_LIMITS['min'][0],
            upper=general_input.MONTHLY_PLD_LIMITS['max'][0]
        )
        return pld_data

    def _process_simulated_generation(self) -> pd.DataFrame:
        """Extracts and processes simulated generation (MWh) from the raw DataFrame."""
        gen_data = self.raw_data[['scenario_nw', 'submarket', 'generation_MWm']].copy()
        gen_data = gen_data.loc[gen_data.index >= self.start_date]
        
        if not isinstance(gen_data.index, pd.DatetimeIndex):
            raise ValueError("Index of gen_data must be a DatetimeIndex.")
        
        else:
            gen_data['t_hour'] = gen_data.index.days_in_month * 24
            gen_data['generation_MWh'] = gen_data['generation_MWm'] * gen_data['t_hour']
            gen_data.drop(columns=['t_hour', 'generation_MWm'], inplace=True)
            
        return gen_data

    def _load_and_process_re_generation(self) -> pd.DataFrame:
        """Loads and processes RE (non-simulated) generation data from Excel."""
        
        re_raw = pd.read_excel(self.re_excel_path, sheet_name="generation_MWh_vf")

        re_data = re_raw.iloc[1:, :].copy()
        re_data.rename(columns={
            'Unnamed: 0': 'date', 'EOL': 'EOL_SE', 'UFV': 'UFV_SE',
            'EOL.1': 'EOL_S', 'UFV.1': 'UFV_S',
            'EOL.2': 'EOL_NE', 'UFV.2': 'UFV_NE',
            'EOL.3': 'EOL_N', 'UFV.3': 'UFV_N',
        }, inplace=True)

        re_data['date'] = pd.to_datetime(re_data['date'], format='%Y-%m-%d')
        re_data.set_index('date', inplace=True)
        re_data = re_data.loc[re_data.index >= self.start_date]

        re_data.columns = re_data.columns.str.split('_', expand=True)
        re_data.columns.names = ['Tecnology', 'Submarket']
        
        re_data_stacked = re_data.stack(level=['Tecnology', 'Submarket'], future_stack=True)
        
        final_re_gen = re_data_stacked.reset_index(name='generation_MWh') # type: ignore
        final_re_gen.set_index('date', inplace=True)
        
        return final_re_gen

    def process_all_data(self):
        """
        Main method that orchestrates the entire processing pipeline.
        """
        print("Starting data processing...")
        
        self.raw_data = self._load_and_preprocess_newave_raw()
        self.pld_data = self._process_pld()
        self.simulated_generation_data = self._process_simulated_generation()

        self.re_generation_data = self._load_and_process_re_generation()
        
        print("Data processing complete.")



if __name__ == "__main__":

    processor = NewaveDataProcessor(newave_csv_path=general_input.newave_csv, re_excel_path=general_input.re_excel, start_date='2026-01-01')
    processor.process_all_data()

    print("PLD Data Sample:")
    print(processor.pld_data.head())

    print("\nSimulated Generation Data Sample:")
    print(processor.simulated_generation_data.head())

    print("\nRE Generation Data Sample:")
    print(processor.re_generation_data.head())