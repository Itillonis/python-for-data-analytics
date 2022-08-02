import os
import yfinance as yf
import pandas as pd
import matplotlib.pyplot as plt
import seaborn
from tqdm.auto import tqdm
from pylab import rcParams
from datetime import datetime
from src.conf import customers, costs, discounts, prices_path

def calculate_prices():
    print('hello')
    seaborn.set()

    # Подгружаем котировки курсы
    print("Подгружаем котировки и курсы")
    df_dict = {}
    for ticker in tqdm(['CL=F', 'USDRUB=X', 'EURUSD=X', 'EURRUB=X']):
        df = yf.download(ticker, progress=False)
        df = df.Close.copy()
        df = df.resample('M').mean()
        df_dict[ticker] = df

    # Рассчитываем цены
    print("Рассчитываем цены")
    main_df = pd.concat(df_dict.values(), axis=1)
    main_df.columns = ['CRUDE_OIL_USD', 'USDRUB', 'EURUSD', 'EURRUB']
    main_df = main_df.loc['2019-06-30':].copy()
    main_df['MWP_PRICE_EUR'] = main_df.CRUDE_OIL_USD * 16 * (1 / main_df.EURUSD) + costs.get('PRODUCTION_COST')
    main_df['MWP_PRICE_USD'] = main_df.CRUDE_OIL_USD * 16 + costs.get('PRODUCTION_COST') * main_df.EURUSD
    main_df['MWP_PRICE_EUR_EU'] = main_df['MWP_PRICE_EUR'] + costs.get('EU_LOGISTIC_COST_EUR')
    main_df['MWP_PRICE_USD_CN'] = main_df['MWP_PRICE_USD'] + costs.get('CN_LOGISTIC_COST_USD')
    main_df['MWP_PRICE_EUR_EU_MA'] = main_df.MWP_PRICE_EUR_EU.rolling(window=3).mean()

    # Создаем отдельный файл для каждого из клиентов
    rcParams['figure.figsize'] = 15, 7

    print("Готовим отдельный файл для клиентов")
    for client, v in customers.items():

        # Создаем директорию и путь к файлу
        client_price_path = os.path.join(prices_path, f"{client.lower()}")
        if not os.path.exists(client_price_path):
            os.makedirs(client_price_path)

        calculation_date = datetime.today().date().strftime(format="%d%m%Y")
        client_price_file_path = os.path.join(client_price_path, f'{client}_mwp_price_{calculation_date}.xlsx')

        location = v.get('location')
        disc = 0.0
        if v.get('location') == "EU":
            fl = 0
            for k_lim, discount_share in discounts.items():
                if v.get('volumes') > k_lim:
                    continue
                else:
                    disc = discount_share
                    fl = 1
                    break
            if fl == 0:
                disc = discounts.get(max(discounts.keys()))

            if v.get('comment') == 'monthly':
                client_price = main_df['MWP_PRICE_EUR_EU'].mul((1 - disc)).add(costs.get('EU_LOGISTIC_COST_EUR')).round(2)
            elif v.get('comment') == 'moving_average':
                client_price = main_df['MWP_PRICE_EUR_EU_MA'].mul((1 - disc)).add(costs.get('EU_LOGISTIC_COST_EUR')).round(2)

        elif v.get('location') == 'CN':
            fl = 0
            for k_lim, discount_share in discounts.items():
                if v.get('volumes') > k_lim:
                    continue
                else:
                    disc = discount_share
                    fl = 1
                    break
            if fl == 0:
                disc = discounts.get(max(discounts.keys()))

            client_price = main_df['MWP_PRICE_USD_CN'].mul((1 - disc)).add(costs.get('CN_LOGISTIC_COST_USD')).round(2)
        with pd.ExcelWriter(client_price_file_path, engine='xlsxwriter') as writer:
            client_price.to_excel(writer, sheet_name='price')

            # Добавляем график с ценой
            plot_path = f'{client}_wbp.png'
            plt.title('Цена ВБП(DDP)', fontsize=16, fontweight='bold')
            plt.plot(client_price)
            plt.savefig(plot_path)
            plt.close()

            worksheet = writer.sheets['price']
            worksheet.insert_image('C2', plot_path)

        print(f"{client} готов")

    print("Удаляем ненужные файлы")
    for k, v in customers.items():
        if os.path.exists(f"{k}_wbp.png"):
            os.remove(f"{k}_wbp.png")

    print("Работа завершена!")

if __name__ == "__main__":
    calculate_prices()