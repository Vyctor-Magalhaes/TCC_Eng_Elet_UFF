from main import *


def generation_historical_shape(historical_data_processor):

    historical_hourly_generation = historical_data_processor.historical_hourly_generation_processing(start_date=start_date, end_date=end_date)




def price_historical_shape(historical_data_processor, start_date: str = '2010-01-01', end_date: str = '2025-07-01'):

    historical_hourly_price = historical_data_processor.historical_hourly_pld_processing()
    historical_hourly_price = historical_hourly_price[(historical_hourly_price.index >= start_date) & (historical_hourly_price.index <= end_date)]

    price_aggregated = historical_hourly_price.groupby([historical_hourly_price.index, 'submarket'])['Hourly_PLD'].mean()

    pivoted_prices = price_aggregated.unstack('submarket')

    avg_historical_prices = pivoted_prices.groupby(pivoted_prices.index.hour).mean()

    avg_historical_prices.index.name = None

    avg_historical_shape = avg_historical_prices / avg_historical_prices.mean()

    # avg_historical_shape.to_csv(r'Data\avg_historical_shape.csv')

    return historical_hourly_price, avg_historical_shape, avg_historical_prices


if __name__ == "__main__":

    electric_sector_client_ccee = ElectricSectorOpenData("ccee")
    electric_sector_client_ons = ElectricSectorOpenData("ons")
    ons_generation_client = ONSHourlyGeneration()

    start_date='2015-01-01'
    end_date='2024-12-31'

    historical_data_processor = HistoricalDataProcessor(electric_sector_client_ccee, electric_sector_client_ons, ons_generation_client)

    historical_hourly_price, avg_historical_shape, avg_historical_prices = price_historical_shape(historical_data_processor, start_date=start_date, end_date=end_date)

    print(avg_historical_shape)


    ax = avg_historical_shape.plot(
    figsize=(12, 6),
    title='Perfil Horário Médio do PLD por Submercado (2015-2024)',
    grid=True,
    style='-' 
)

    ax.set_xlabel('Hora do Dia')
    ax.set_ylabel('PLD (R$/MWh)')
    ax.set_xticks(range(0, 24)) # Garante que todos os ticks das horas apareçam
    ax.legend(title='Submercado')
    plt.tight_layout()
    plt.show()
    
    # ax = avg_historical_shape.plot(
    #     figsize=(15, 7), 
    #     title='PLD Horário por Submercado',
    #     colormap='Set2', 
    #     alpha=0.7
    #     )

    # ax.set_xlabel('Data')
    # ax.set_ylabel('PLD (R$/MWh)')
    # ax.legend(title='Submercado')
    # plt.grid(True, linestyle='--', alpha=0.5) # Grid mais suave
    # plt.tight_layout()
    # plt.show()

    # print(historical_hourly_price)

    # historical_hourly_generation = historical_data_processor.historical_hourly_generation_processing(start_date=start_date, end_date=end_date)

    # print(historical_hourly_generation)

    # hourly_data = historical_data_processor.hourly_data_treatment(historical_hourly_generation, historical_hourly_price)

    # capture_indicators = CaptureIndicators(historical_data_processor)

    # wind_cap_rate, solar_cap_rate, wind_cap_prices, solar_cap_prices = capture_indicators.capture_rate_calculate(hourly_data, start_date=start_date, end_date= end_date)

    # print(wind_cap_rate, solar_cap_rate, wind_cap_prices, solar_cap_prices)
    print("Fim")