import matplotlib.pyplot as plt
import matplotlib
import pandas as pd
import numpy as np
import datetime
from sklearn.metrics import r2_score, mean_absolute_error, mean_squared_error
import os

matplotlib.use('TkAgg')


def get_stats(df_all):
    df = pd.DataFrame(columns=['source', 'r2', 'mae', 'rmse'])
    if 'cds' in df_all.columns:
        df_metrics = df_all.dropna(subset=['obs', 'cds'])
        df = df.append({'source': 'cds', 'r2': round(r2_score(df_metrics['obs'], df_metrics['cds']), 4),
                        'mae': round(mean_absolute_error(df_metrics['obs'], df_metrics['cds']), 4),
                        'rmse': round(mean_squared_error(df_metrics['obs'], df_metrics['cds']), 4)}, ignore_index=True)
    if 'gw' in df_all.columns:
        df_metrics = df_all.dropna(subset=['obs', 'gw'])
        df = df.append({'source': 'gw', 'r2': round(r2_score(df_metrics['obs'], df_metrics['gw']), 4),
                        'mae': round(mean_absolute_error(df_metrics['obs'], df_metrics['gw']), 4),
                        'rmse': round(mean_squared_error(df_metrics['obs'], df_metrics['gw']), 4)}, ignore_index=True)
    if 'nasa' in df_all.columns:
        df_metrics = df_all.dropna(subset=['obs', 'nasa'])
        df = df.append({'source': 'nasa', 'r2': round(r2_score(df_metrics['obs'], df_metrics['nasa']), 4),
                        'mae': round(mean_absolute_error(df_metrics['obs'], df_metrics['nasa']), 4),
                        'rmse': round(mean_squared_error(df_metrics['obs'], df_metrics['nasa']), 4)}, ignore_index=True)
    if 'dah' in df_all.columns:
        df_metrics = df_all.dropna(subset=['obs', 'dah'])
        df = df.append({'source': 'dah', 'r2': round(r2_score(df_metrics['obs'], df_metrics['dah']), 4),
                        'mae': round(mean_absolute_error(df_metrics['obs'], df_metrics['dah']), 4),
                        'rmse': round(mean_squared_error(df_metrics['obs'], df_metrics['dah']), 4)}, ignore_index=True)
    df_metrics = df_all.dropna(subset=['obs', 'best'])
    df = df.append({'source': 'best', 'r2': round(r2_score(df_metrics['obs'], df_metrics['best']), 4),
                    'mae': round(mean_absolute_error(df_metrics['obs'], df_metrics['best']), 4),
                    'rmse': round(mean_squared_error(df_metrics['obs'], df_metrics['best']), 4)}, ignore_index=True)
    print(df.to_string(index=False))


def plot_data(lake_name, range=None, unbias='none', unbias_all=False):
    if unbias_all and not os.path.exists(f'Lakes/{lake_name}/{lake_name}_obs.csv'):
        print('No observations found to unbias all estimations')
        return
    if unbias_all:
        df_obs = pd.read_csv(f'Lakes/{lake_name}/{lake_name}_obs.csv', parse_dates=['date'])
    if range is None:
        [start, end] = [datetime.datetime(2000, 1, 1), datetime.date.today()]
    else:
        format = '%Y-%m-%d'
        [start, end] = datetime.datetime.strptime(range[0], format), datetime.datetime.strptime(range[1], format)
    df_all = pd.DataFrame(pd.date_range(start=start, end=end), columns=['date'])

    if os.path.exists(f'Lakes/{lake_name}/{lake_name}_obs.csv'):
        df_obs = pd.read_csv(f'Lakes/{lake_name}/{lake_name}_obs.csv', parse_dates=['date'])
        df_all = df_all.merge(df_obs[['date', 'obs']], on='date', how='left')
        plt.plot(df_obs['date'], df_obs['obs'].interpolate(limit_area='inside'), label='observations', color='k',
                 linewidth=3)
    if os.path.exists(f'Lakes/{lake_name}/{lake_name}_CDS.csv'):
        df_CDS = pd.read_csv(f'Lakes/{lake_name}/{lake_name}_CDS.csv', parse_dates=['date'])
        label = f"CDS (VITO)"
        if unbias_all:
            df_CDS = df_CDS.merge(df_all[['date', 'obs']], on='date', how='left')
            diff = (df_CDS['obs'] - df_CDS['cds']).dropna().values
            delta_cds = diff.mean() if len(diff) > 0 else 0
            df_CDS['cds'] = df_CDS['cds'] + delta_cds
            df_CDS = df_CDS.drop(columns=['obs'])
            label += f" delta: {np.round(delta_cds, 2)}"
        df_all = df_all.merge(df_CDS, on='date', how='left')
        cds_interpolate = df_CDS['cds'].interpolate(limit_area='inside')
        plt.plot(df_CDS['date'], cds_interpolate, label=label, color='C4')
        plt.fill_between(df_CDS['date'], cds_interpolate - df_CDS['cds_uncertainty'].interpolate(limit_area='inside'),
                         cds_interpolate + df_CDS['cds_uncertainty'].interpolate(limit_area='inside'),
                         alpha=0.2, color='C4')

    if os.path.exists(f'Lakes/{lake_name}/{lake_name}_GW.csv'):
        df_GW = pd.read_csv(f'Lakes/{lake_name}/{lake_name}_GW.csv', parse_dates=['date'])
        df_GW['date'] = df_GW['date'] + datetime.timedelta(days=15)
        label = "GlobalWater"
        if unbias_all:
            df_GW = df_GW.merge(df_all[['date', 'obs']], on='date', how='left')
            diff = (df_GW['obs'] - df_GW['gw']).dropna().values
            delta_GW = diff.mean() if len(diff) > 0 else 0
            df_GW['gw'] = df_GW['gw'] + delta_GW
            df_GW = df_GW.drop(columns=['obs'])
            label += f" delta: {np.round(delta_GW, 2)}"
        df_all = df_all.merge(df_GW, on='date', how='left')
        gw_interpolate = df_GW['gw'].interpolate(limit_area='inside')
        plt.plot(df_GW['date'], gw_interpolate, label=label, color='C3')

    if os.path.exists(f'Lakes/{lake_name}/{lake_name}_NASA.csv'):
        df_NASA = pd.read_csv(f'Lakes/{lake_name}/{lake_name}_NASA.csv', parse_dates=['date'])
        df_NASA = df_NASA.sort_values(by=['date'])
        label = "NASA"
        if unbias_all:
            df_NASA = df_NASA.merge(df_all[['date', 'obs']], on='date', how='left')
            diff = (df_NASA['obs'] - df_NASA['nasa']).dropna().values
            delta_NASA = diff.mean() if len(diff) > 0 else 0
            df_NASA['nasa'] = df_NASA['nasa'] + delta_NASA
            df_NASA = df_NASA.drop(columns=['obs'])
            label += f" delta: {np.round(delta_NASA, 2)}"
        df_all = df_all.merge(df_NASA, on='date', how='left')
        nasa_interpolate = df_NASA['nasa'].interpolate(limit_area='inside')
        plt.plot(df_NASA['date'], nasa_interpolate, label=label, color='C2')

    if os.path.exists(f'Lakes/{lake_name}/{lake_name}_DAHITI.csv'):
        df_DAH = pd.read_csv(f'Lakes/{lake_name}/{lake_name}_DAHITI.csv', parse_dates=['date'])
        label = "DAHITI"
        if unbias_all:
            df_DAH = df_DAH.merge(df_all[['date', 'obs']], on='date', how='left')
            diff = (df_DAH['obs'] - df_DAH['dah']).dropna().values
            delta_dah = diff.mean() if len(diff) > 0 else 0
            df_DAH['dah'] = df_DAH['dah'] + delta_dah
            df_DAH = df_DAH.drop(columns=['obs'])
            label += f" delta: {np.round(delta_dah, 2)}"
        df_all = df_all.merge(df_DAH, on='date', how='left')
        dah_interpolate = df_DAH['dah'].interpolate(limit_area='inside')
        plt.plot(df_DAH['date'], dah_interpolate, label=label, color='C1')
        plt.fill_between(df_DAH['date'], dah_interpolate - df_DAH['dah_uncertainty'].interpolate(limit_area='inside'),
                         dah_interpolate + df_DAH['dah_uncertainty'].interpolate(limit_area='inside'),
                         alpha=0.2, color='C1')

    if unbias_all:
        unbias = 'obs'
    df_best = pd.read_csv(f'Lakes/{lake_name}/{lake_name}_best_ub_{unbias}.csv', parse_dates=['date'])
    df_all = df_all.merge(df_best, on='date', how='left')

    get_stats(df_all)

    plt.plot(df_all['date'], df_all['best'], label='best', color='C0', linewidth=2)
    plt.xlim([start, end])
    plt.legend(loc='upper left')
    plt.tight_layout()
    mng = plt.get_current_fig_manager()
    mng.window.state('zoomed')
    plt.show()


plot_data('LakeDemo', range=['2016-01-01', '2024-09-01'], unbias='none', unbias_all=True)
