import os
import yfinance as yf
import pandas as pd
import matplotlib.pyplot as plt
import seaborn
import requests
import numpy as np
from pylab import rcParams
from tqdm.auto import tqdm
from datetime import datetime
from src.conf import customers, costs, discounts, prices_path


def calculate_prices():
    print('hello')
    seaborn.set()

    PRODUCTION_COST = costs.get('PRODUCTION_COST')
    EU_LOGISTIC_COST_EUR = costs.get('EU_LOGISTIC_COST_EUR')
    CN_LOGISTIC_COST_USD = costs.get('CN_LOGISTIC_COST_USD')
    RU_LOGISTIC_COST_RUB = costs.get('RU_LOGISTIC_COST_RUB')
    disc_vals = list(discounts.values())

    # Подгружаем котировки курсы
    print("Подгружаем курсы валют")
    df_dict = {}
    for ticker in tqdm(['USDRUB=X', 'EURUSD=X', 'EURRUB=X']):
        df = yf.download(ticker)
        df = df.Close.copy()
        df = df.resample('M').mean()
        df_dict[ticker] = df

    main_df = pd.concat(df_dict.values(), axis=1)
    main_df.columns = ['USDRUB', 'EURUSD', 'EURRUB']

    # Подгружаем котировки курсы
    print("Подгружаем цены на каучук")
    datedict = main_df.reset_index()['Date'].tolist()
    rubber_dict = []
    for date in tqdm(datedict):
        rubber_year = date.year
        rubber_month = date.month
        url = f"https://www.lgm.gov.my/webv2api/api/rubberprice/month={rubber_month}&year={rubber_year}"
        res = requests.get(url)
        rj = res.json()
        rubber_dict.append(rj)
    rubber_df = pd.DataFrame()
    for item in rubber_dict:
        df1 = pd.json_normalize(item)
        df1['date'] = pd.to_datetime(df1['date'])
        df1 = df1.drop(columns=['rm', 'masa', 'tone'])
        df1['us'] = df1['us'].replace('', np.nan).astype(float)
        df1 = df1.groupby(['date', 'grade']).mean()
        prices = df1.reset_index().pivot(index='date', columns='grade', values='us')
        mean_prices = prices.mean()
        mean_prices = mean_prices.rename(prices.reset_index().date.iloc[-1])
        rubber_df = rubber_df.append(mean_prices)
    # Добавляем цены в датафрейм
    main_df = pd.concat([main_df, rubber_df], axis=1)
    main_df.reset_index(inplace=True)
    main_df = main_df.rename(columns={'index': 'Date'})
    main_df['Date'] = main_df['Date'].dt.to_period('M')
    main_df = main_df.groupby(['Date'], as_index=False).sum()

    rcParams['figure.figsize'] = 25, 10

    # Рассчитываем цены
    print("Рассчитываем цены в разных валютах:")
    rub_prices_df = main_df.iloc[:, 4:].mul(main_df['USDRUB'], axis=0).round(2)
    rub_prices_df['Date'] = main_df['Date']
    print("Рассчет в рублях готов")
    euro_prices_df = main_df.iloc[:, 4:].div(main_df['EURUSD'], axis=0).round(2)
    euro_prices_df['Date'] = main_df['Date']
    print("Рассчет в евро готов")
    dollar_prices_df = main_df.iloc[:, 4:].round(2)
    dollar_prices_df['Date'] = main_df['Date']
    print("Рассчет в долларах готов")

    # Создаем отдельный файл для каждого из клиентов

    rcParams['figure.figsize'] = 25, 10

    print("Готовим отдельный файл для клиентов")
    try:
        int(PRODUCTION_COST)
        int(EU_LOGISTIC_COST_EUR)
        int(CN_LOGISTIC_COST_USD)
        int(RU_LOGISTIC_COST_RUB)
    except:
        print('Некорректные данные в дополнительной стоимости (доставка и производство)', )
    else:
        for client, description in customers.items():
            try:
                int(description.get('volumes'))
            except:
                print('Некорректные данные в объеме заказа', client)
                break
            try:
                description.get('volumes') in ['EU', 'CN', 'RU']
            except:
                print('Некорректные данные в локации покупателя', client)
                break

            calculation_date = datetime.today().date().strftime(format="%d%m%Y")
            client_price_file_path = os.path.join(prices_path, f'{client}_mwp_price_{calculation_date}.xlsx')
            if description.get('volumes') < 100:
                discount = disc_vals[0]
            elif description.get('volumes') < 300:
                discount = disc_vals[1]
            else:
                discount = disc_vals[2]

            if description.get('location') == "EU":
                client_price = euro_prices_df.iloc[:, :-1] * (1 - discount) + EU_LOGISTIC_COST_EUR + PRODUCTION_COST
            elif description.get('location') == "CN":
                client_price = dollar_prices_df.iloc[:, :-1].add(PRODUCTION_COST / main_df['EURUSD'], axis=0) * (
                        1 - discount) + CN_LOGISTIC_COST_USD
            elif description.get('location') == "RU":
                client_price = rub_prices_df.iloc[:, :-1].add(PRODUCTION_COST * main_df['EURRUB'], axis=0) * (
                        1 - discount) + RU_LOGISTIC_COST_RUB
            if description.get('comment') == 'moving_average':
                client_price = client_price.rolling(window=3).mean()

            with pd.ExcelWriter(client_price_file_path, engine='xlsxwriter') as writer:
                client_price.to_excel(writer, sheet_name='price_proposal')
                # Добавляем график с ценой
                plot_path = f'{client}_wbp.png'
                plt.title('Цена каучука', fontsize=16, fontweight='bold')
                plt.plot(client_price)
                plt.savefig(plot_path)
                worksheet = writer.sheets['price_proposal']
                worksheet.insert_image('I2', plot_path)

        for k, v in customers.items():
            if os.path.exists(f"{k}_wbp.png"):
                os.remove(f"{k}_wbp.png")

        print(f"{client} готов")

    print("Удаляем ненужные файлы")
    for k, v in customers.items():
        if os.path.exists(f"{k}_wbp.png"):
            os.remove(f"{k}_wbp.png")

    print("Работа завершена!")


if __name__ == "__main__":
    calculate_prices()