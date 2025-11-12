from matplotlib import cm
from main import *


def generation_historical_shape(historical_data_processor, start_date: str = '2010-01-01', end_date: str = '2025-06-30'):


    def _calculate_avg_and_shape(filtered_series: pd.Series) -> pd.DataFrame:
        
        pivoted_data = filtered_series.unstack('submarket')
        
        avg_hourly_data = pivoted_data.groupby(pivoted_data.index.hour).mean()
        avg_hourly_data.index.name = None
        
        avg_hourly_shape = avg_hourly_data / avg_hourly_data.mean()
        
        return avg_hourly_shape, avg_hourly_data

    historical_hourly_generation = historical_data_processor.historical_hourly_generation_processing(start_date=start_date, end_date=end_date)
    total_generation, generation_RE, hourly_data = historical_data_processor.hourly_data_treatment(historical_hourly_generation)

    total_generation_filtered = total_generation.query(
        "@start_date <= date <= @end_date"
    )
    
    generation_RE_filtered = generation_RE.query(
        "@start_date <= date <= @end_date"
    )

    (avg_historical_gen_total_shape, 
     avg_historical_gen_total) = _calculate_avg_and_shape(total_generation_filtered)

    gen_wind = generation_RE_filtered['wind_generation_MWh']
    (avg_historical_gen_wind_shape, 
     avg_historical_gen_wind) = _calculate_avg_and_shape(gen_wind)

    gen_solar = generation_RE_filtered['solar_generation_MWh']
    (avg_historical_gen_solar_shape, 
     avg_historical_gen_solar) = _calculate_avg_and_shape(gen_solar)

    return (
        avg_historical_gen_total_shape, 
        avg_historical_gen_wind_shape, 
        avg_historical_gen_solar_shape, 
        avg_historical_gen_total, 
        avg_historical_gen_wind, 
        avg_historical_gen_solar
    )



def generation_annual_shapes(historical_data_processor, start_date: str = '2015-01-01', end_date: str = '2024-12-31'):
    """
    Calcula perfis horários anuais (absolutos e normalizados) para 
    geração total, eólica e solar, para cada ano no intervalo.
    
    Otimizado para fazer um único download e agrupar por ano.
    """

    # --- 1. Função Auxiliar Interna (Modificada para agrupar por ANO) ---
    def _calculate_annual_avg_and_shape(filtered_series: pd.Series) -> (pd.DataFrame, pd.DataFrame):
        """
        Recebe uma Série já filtrada e retorna os perfis médios
        e normalizados (shape) para CADA ANO.
        """
        # 1. Pivota o submercado para as colunas
        pivoted_data = filtered_series.unstack('submarket')
        
        # 2. Agrupa por ANO e HORA
        #    Esta é a principal mudança
        avg_hourly_data = pivoted_data.groupby(
            [pivoted_data.index.year, pivoted_data.index.hour]
        ).mean()
        
        # O índice agora é (ano, hora), ex: (2015, 0), (2015, 1)...
        avg_hourly_data.index.names = ['year', 'hour']
        
        # 3. Calcula o 'shape' normalizado POR ANO
        #    Nós dividimos os valores de cada ano pela média daquele ano
        #    O groupby(level='year') aplica a operação .mean() a cada ano
        avg_hourly_shape = avg_hourly_data / avg_hourly_data.groupby(level='year').mean()
        
        return avg_hourly_shape, avg_hourly_data

    # --- 2. Carregamento de Dados (UMA VEZ) ---
    print(f"Carregando dados para o período completo: {start_date} a {end_date}...")
    historical_hourly_generation = historical_data_processor.historical_hourly_generation_processing(
        start_date=start_date, 
        end_date=end_date
    )
    total_generation, generation_RE, hourly_data = historical_data_processor.hourly_data_treatment(historical_hourly_generation)

    # --- 3. Filtragem (Garante o range) ---
    total_generation_filtered = total_generation.query(
        "@start_date <= date <= @end_date"
    )
    generation_RE_filtered = generation_RE.query(
        "@start_date <= date <= @end_date"
    )

    # --- 4. Processamento (Usando a nova função auxiliar) ---
    
    # Bloco 1: Geração Total
    (total_shape, total_avg) = _calculate_annual_avg_and_shape(total_generation_filtered)

    # Bloco 2: Geração Eólica (Wind)
    gen_wind = generation_RE_filtered['wind_generation_MWh']
    (wind_shape, wind_avg) = _calculate_annual_avg_and_shape(gen_wind)

    # Bloco 3: Geração Solar
    gen_solar = generation_RE_filtered['solar_generation_MWh']
    (solar_shape, solar_avg) = _calculate_annual_avg_and_shape(gen_solar)

    # --- 5. Retorno ---
    # Os 6 DataFrames retornados agora têm um MultiIndex (year, hour)
    return (
        total_shape, wind_shape, solar_shape,
        total_avg, wind_avg, solar_avg
    )




def price_historical_shape(historical_data_processor, start_date: str = '2010-01-01', end_date: str = '2025-07-01'):

    historical_hourly_price = historical_data_processor.historical_hourly_pld_processing()
    historical_hourly_price = historical_hourly_price[(historical_hourly_price.index >= start_date) & (historical_hourly_price.index <= end_date)]

    price_aggregated = historical_hourly_price.groupby([historical_hourly_price.index, 'submarket'])['Hourly_PLD'].mean()

    pivoted_prices = price_aggregated.unstack('submarket')

    avg_historical_prices = pivoted_prices.groupby(pivoted_prices.index.hour).mean()

    avg_historical_prices.index.name = None

    avg_historical_shape = avg_historical_prices / avg_historical_prices.mean()

    # avg_historical_shape.to_csv(r'Data\avg_historical_shape.csv')

    return pivoted_prices, avg_historical_shape, avg_historical_prices


if __name__ == "__main__":

    electric_sector_client_ccee = ElectricSectorOpenData("ccee")
    electric_sector_client_ons = ElectricSectorOpenData("ons")
    ons_generation_client = ONSHourlyGeneration()

    start_date='2015-01-01'
    # end_date='2024-12-31'
    end_date='2025-06-30'

    historical_data_processor = HistoricalDataProcessor(electric_sector_client_ccee, electric_sector_client_ons, ons_generation_client)

    historical_hourly_price, avg_historical_shape, avg_historical_prices = price_historical_shape(historical_data_processor, start_date=start_date, end_date=end_date)
    
    # historical_hourly_price -> Histórico de preço horário por submercado

    # avg_historical_gen_total_shape, avg_historical_gen_wind_shape, avg_historical_gen_solar_shape, avg_historical_gen_total, avg_historical_gen_wind, avg_historical_gen_solar  = generation_historical_shape(historical_data_processor,start_date=start_date, end_date=end_date)

    # print(avg_historical_gen_wind)

    # Exemplo de como usar a função
    (total_shape_anual, wind_shape_anual, solar_shape_anual, 
    total_avg_anual, wind_avg_anual, solar_avg_anual) = generation_annual_shapes(historical_data_processor)

    # Para ver o perfil de 24 horas APENAS para 2015:
    perfil_2015 = total_shape_anual.loc[2015]

    # Para ver o perfil de 24 horas APENAS para 2024:
    perfil_2024 = total_shape_anual.loc[2024]

    print(perfil_2015)


    




#     PLOT THE HOURLY SHAPE GRAPH
    # ax = avg_historical_shape.plot(
    # figsize=(12, 6),
    # title='Perfil Horário Médio do PLD por Submercado (2015-2024)',
    # grid=True,
    # style='-' )
    # ax.set_xlabel('Hora do Dia')
    # ax.set_ylabel('PLD (R$/MWh)')
    # ax.set_xticks(range(0, 24)) # Garante que todos os ticks das horas apareçam
    # ax.legend(title='Submercado')
    # plt.tight_layout()
    # plt.show()


    
    print("Fim")