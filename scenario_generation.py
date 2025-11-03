import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

scenarios_n = 6 # O numero total de cenários, precisa apenas ser divisível por 3 e maior que 4.
hours_n = 24
hours_range = np.array(range(hours_n))
columns_name = np.array(range(1, scenarios_n+1, 1)).astype(str) # O cenário base é o 1, n/3 é o cenário médio, 2n/3 é o cenário duck curve, o cenário n target
index_range = hours_range.astype(str)

base_scenario = [1] * hours_n


# Obtida através da média dos perfis horários mensais de 2024 do CAISO
duck_curve_scenario = [1.171741055, 1.137828773, 1.111666567, 1.102689792, 1.133085536, 1.233355995, 1.275684022, 1.012023572, 0.648603192, 0.502583005, 0.450235673, 0.421525523,
                         0.398678732, 0.409479977, 0.462696895, 0.648369634, 0.907412972, 1.215257902, 1.528552597, 1.716701144, 1.551493977, 1.426998158, 1.309888966, 1.223446342]

# Obtida através da perfil horário 28 de abril de 2024 do CAISO
canyon_curve_scenario = [1.721430317, 1.704336392, 1.685400875, 1.691909491, 1.699254331, 1.704667307, 1.647772854, 0.728341223, 0.192444629, 0.141791881, 0.055747559, 0.021898120,   
                         0.000000000, 0.009760425, 0.046013915, 0.025077403, 0.135418899, 0.645644685, 1.538757645, 1.744430676, 1.786880575, 1.713726435, 1.684768389, 1.674525975]

# Perfil horário médio histórico
average_scenario = [1.17285809, 1.20095986, 1.20357422, 1.16439478, 1.1527738 ,
       1.17201634, 1.15249601, 1.15313127, 1.15055555, 1.10572767,
       1.06226218, 0.94535046, 0.85826778, 0.77518794, 0.68442188,
       0.67617884, 0.6561201 , 0.70658047, 0.79980739, 0.92272058,
       0.98256103, 1.07106745, 1.11194132, 1.119045 ]

comments = """
Pensar em uma forma de distriuir os cenários de forma mais proporcional
"""    

scenarios = pd.DataFrame(index=index_range,
                         columns=columns_name,
                         dtype=float)

scenarios.iloc[:,0] = np.asarray(base_scenario)
scenarios.iloc[:,int(scenarios_n/3)-1] = np.asarray(average_scenario)

scenarios.iloc[:,int(2*scenarios_n/3)-1] = np.asarray(duck_curve_scenario)
scenarios.iloc[:,int(scenarios_n)-1] = np.asarray(canyon_curve_scenario)

scenarios.interpolate(method='linear', axis=1, inplace=True)


print(scenarios)


# # Configurações estéticas globais (opcional)
# plt.style.use('seaborn-v0_8-whitegrid')
# plt.figure(figsize=(10, 6))

# # Plota cada coluna com uma legenda identificando o cenário
# for col in scenarios.columns:
#     plt.plot(scenarios.index.astype(int), scenarios[col], label=f'Cenário {col}')

# plt.title('Evolução dos Cenários')
# plt.xlabel('Hora')
# plt.ylabel('Valor')
# plt.legend(title='Cenários', bbox_to_anchor=(1.05, 1), loc='upper left')
# plt.tight_layout()
# plt.show()


print("Fim")