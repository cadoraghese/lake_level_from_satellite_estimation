import pandas as pd
import numpy as np


def generate_trajectory(lake_name, bias):
    altimetry_method = 'elev'

    if altimetry_method == 'all':
        df_alt = pd.read_csv(f'Lakes/{lake_name}/{lake_name}_alt1.csv', parse_dates=['date'])
        for key in df_alt.columns[1:]:
            df_alt.rename(columns={key: "alt1_" + key}, inplace=True)
        df = df_alt.copy()
        df_alt = pd.read_csv(f'Lakes/{lake_name}/{lake_name}_elev.csv', parse_dates=['date'])
        for key in df_alt.columns[1:]:
            df_alt.rename(columns={key: "elev_" + key}, inplace=True)
        df = df.merge(df_alt, on='date', how='left')
        df_alt = pd.read_csv(f'Lakes/{lake_name}/{lake_name}_alt2.csv', parse_dates=['date'])
        for key in df_alt.columns[1:]:
            df_alt.rename(columns={key: "alt2_" + key}, inplace=True)
        df = df.merge(df_alt, on='date', how='left')
    else:
        df = pd.read_csv(f'Lakes/{lake_name}/{lake_name}_{altimetry_method}.csv', parse_dates=['date'])
    keys = df.columns[1:]
    print(keys)
    df = df.replace(-9999, np.nan)

    unbias_name = '_ub'
    if bias is None:
        bias = []
        for key in keys:
            bias.append(np.nanmean(df[key]))
        bias = np.nanmean(bias) - bias
        df[keys] = df[keys].to_numpy() + bias
        unbias_name += '_none'

    elif bias == 'obs':
        df_obs = pd.read_csv(f'Lakes/{lake_name}/{lake_name}_obs.csv', parse_dates=['date'])
        df = df.merge(df_obs, on='date', how='left')
        for key in keys:
            diff = (df['obs'] - df[key]).dropna().values
            bias = diff.mean() if len(diff) > 0 else 0
            df[key] = df[key] + bias
        unbias_name += '_obs'

    else:
        df[keys] = df[keys].to_numpy() + bias
        unbias_name += '_custom'

    for i, key in enumerate(keys.copy()):
        if df[key].dropna().size < 10:
            continue
        nans = df[key].isna()
        mask = nans.groupby([(~nans).cumsum(), nans]).transform('size') > 45  # Interpolate only subsequent measurements
        df[key] = df[key].interpolate(method='linear', limit_area='inside').mask(mask)
    df['best'] = np.nanmean(df[keys].to_numpy(), axis=1)

    df[['date', 'best']].to_csv(f'Lakes/{lake_name}/{lake_name}_best{unbias_name}.csv', index=False)


generate_trajectory('LakeDemo', 'obs')
