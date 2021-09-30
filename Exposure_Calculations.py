import pandas as pd
import numpy as np
import os


def port_exposure_calculations():
    grouped_list = []
    ungrouped_list = []
    for i in os.listdir('Exposure/'):
        df = pd.read_excel(f'Exposure/{i}', skiprows=10)
        alloc = df.loc[0, 'Net']

        df.rename(
            columns={'Unnamed: 1': 'Issuer', 'Unnamed: 2': 'Security Name', 'Net.4': 'Tej Delta', ' .2': 'Ticker'},
            inplace=True)

        df2 = df[['Issuer', 'Security Name', 'Ticker', 'Net', 'Gross', 'Tej Delta']]
        df2['Issuer'] = df2['Issuer'].ffill()
        df2.dropna(subset=['Security Name'], inplace=True)

        df2['net_exp'] = np.where(pd.isnull(df2['Tej Delta']), df2['Net'] / alloc, df2['Tej Delta'] / alloc)
        df3 = df2.groupby('Issuer').sum()
        grouped_list.append(df3)
        ungrouped_list.append(df2)

    return grouped_list, ungrouped_list
