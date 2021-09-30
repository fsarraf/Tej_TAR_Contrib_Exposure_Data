import pandas as pd
from datetime import datetime
import Exposure_Calculations as ec
from eze_interface import connect

interface = connect(server='tri-ln-pma1v', username='PMADBUser', password='pmadb')

day = datetime.today().strftime(format='%d%m%Y')


def get_pma_nav(fnd):
    fund = interface.get_fund(fnd)
    pld = fund.position_level_data
    nav = fund.nav_ts.resample('B').ffill()
    nav = round(nav[['end_nav']], 2)

    return pld, nav


def create_cleaned_df():
    pld, nav = get_pma_nav('TAR')

    tj = pld[pld['manager_name'] == 'Tej Johar']

    allocations = pd.read_csv(r'T:\Fadi_Data\TAR_Allocations.csv')
    tj_alloc = allocations[['ref_date', 'Tej Johar']].dropna()
    tj_alloc.rename(columns={'Tej Johar': 'allocations'}, inplace=True)
    tj_alloc['ref_date'] = pd.to_datetime(tj_alloc['ref_date'])

    tj = tj.merge(tj_alloc, on='ref_date')

    tj['gross_exposure'] = tj['gross_exposure'].astype('float64')
    tj['gross_exposure_pct'] = tj['gross_exposure'] / tj['allocations']

    tj['net_exposure_pct'] = tj['net_exposure'] / tj['allocations']
    tj['pnl_pct'] = tj['pnl'] / tj['allocations']

    tj['Month'] = tj['ref_date'].dt.to_period('M')
    tj_alloc['Month'] = tj_alloc['ref_date'].dt.to_period('M')
    tj2 = tj_alloc.drop_duplicates('Month', keep='last').reset_index(drop=True)

    return tj, tj_alloc, tj2


def pma_exposure_calculations(tj, tj2):
    exp_list = []

    for date in tj2['ref_date'].unique():
        no_long = tj[(tj['ref_date'] == date) & (tj['long_short'] == 'LONG') & (tj['sec_type'] != 'Exchrate')].shape[0]
        no_short = tj[(tj['ref_date'] == date) & (tj['long_short'] == 'SHORT') & (tj['sec_type'] != 'Exchrate')].shape[
            0]

        gross_long = tj[(tj['ref_date'] == date) & (tj['long_short'] == 'LONG') & (tj['sec_type'] != 'Exchrate')][
            'gross_exposure_pct'].sum()
        gross_short = tj[(tj['ref_date'] == date) & (tj['long_short'] == 'SHORT') & (tj['sec_type'] != 'Exchrate')][
            'gross_exposure_pct'].sum()

        exp_list.append({'Date': date, 'no_long': no_long, 'no_short': no_short, 'gross_long': gross_long,
                         'gross_short': gross_short})

    exp_df = pd.DataFrame(exp_list)

    return exp_df


def get_contribution_data(tj, tj2):
    ctr_list = []

    for date in tj2['Month'].unique():
        ctr_long = tj[(tj['Month'] == date) & (tj['long_short'] == 'LONG')]['pnl'].sum() / \
                   tj2[tj2['Month'] == date]['allocations'].values[0]
        ctr_short = tj[(tj['Month'] == date) & (tj['long_short'] == 'SHORT')]['pnl'].sum() / \
                    tj2[tj2['Month'] == date]['allocations'].values[0]
        ctr = tj[(tj['Month'] == date)]['pnl'].sum() / tj2[tj2['Month'] == date]['allocations'].values[0]

        ctr_list.append(({'Date': date, 'ctr': ctr, 'ctr_long': ctr_long, 'ctr_short': ctr_short}))

    ctr_df = pd.DataFrame(ctr_list)

    return ctr_df


def main():
    tj, tj_alloc, tj2 = create_cleaned_df()
    exp_df = pma_exposure_calculations(tj, tj2)
    ctr_df = get_contribution_data(tj, tj2)

    grouped_list, ungrouped_list = ec.port_exposure_calculations(day)
    sheets = ['2020-11', '2020-12', '2021-01', '2021-02', '2021-03', '2021-04', '2021-05', '2021-06', '2021-07',
              '2021-08', '2021-09']
    writer = pd.ExcelWriter(f'exposure_output/Tej_CTR_Exp_{day}.xlsx')
    exp_df.to_excel(writer, 'Exposure Breakdown')
    ctr_df.to_excel(writer, 'Contribution Breakdown')

    for l, lst in enumerate([grouped_list, ungrouped_list]):

        if l == 0:
            name = 'Grouped'
        else:
            name = 'Ungrouped'

        for i, j in enumerate(lst):
            j.to_excel(writer, sheets[i] + name)

    writer.save()

    return ctr_df, exp_df, grouped_list, ungrouped_list


if __name__ == '__main__':
    main()
