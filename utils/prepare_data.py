from utils.extract_data_netCDF4 import extract_data_cds
import pandas as pd
import numpy as np
import json


def prepare_cds(lake_name):
    df = extract_data_cds(f'Lakes/{lake_name}/Raw/{lake_name}_CDS_raw.nc')
    df.to_csv(f'Lakes/{lake_name}/{lake_name}_CDS.csv', index=False)


def prepare_data_vito(lake_name):
    json_data = json.load(open(f'Lakes/{lake_name}/Raw/{lake_name}_VITO_raw.json'))
    df = pd.DataFrame.from_dict(json_data['data'])
    df['datetime'] = pd.to_datetime(df['datetime'], format='%Y/%m/%d %H:%M').dt.date
    df = df[['datetime', 'water_surface_height_above_reference_datum', 'water_surface_height_uncertainty']]
    df.rename(columns={"datetime": "date",
                       "water_surface_height_above_reference_datum": "vito",
                       "water_surface_height_uncertainty": "vito_uncertainty"}, inplace=True)
    df.to_csv(f'Lakes/{lake_name}/{lake_name}_VITO.csv', index=False)


def prepare_data_gw(lake_name):
    df = pd.read_csv(f'Lakes/{lake_name}/Raw/{lake_name}_GW_raw.csv', parse_dates=['date'])[['date', 'value']]
    lvl_volume = np.loadtxt(f'Lakes/{lake_name}/Raw/{lake_name}_lsv_rel.txt')
    lvl_volume[2] = lvl_volume[2] / 1e6
    # plt.plot(lvl_volume[0], lvl_volume[2], label='lsv', marker='.')
    new_y = np.interp(df['value'], lvl_volume[2], lvl_volume[0])
    # plt.scatter(new_y, df['value'], label='gw', marker='.', color='r')
    df['gw'] = new_y
    df = df[['date', 'gw', 'value']]
    df.to_csv(f'Lakes/{lake_name}/{lake_name}_GW.csv', index=False)


def prepare_data_nasa(lake_name):
    df = pd.read_fwf(f'Lakes/{lake_name}/Raw/{lake_name}_NASA_raw.txt')
    df.iloc[:, 2] = df.iloc[:, 2].replace(99999999, np.nan)
    df.iloc[:, 14] = df.iloc[:, 14].replace(9999.99, np.nan)
    df['date'] = pd.to_datetime(df.iloc[:, 2], format='%Y%m%d').dt.date
    df['nasa'] = df.iloc[:, 14]
    df = df[['date', 'nasa']]
    df.to_csv(f'Lakes/{lake_name}/{lake_name}_NASA.csv', index=False)


def prepare_data_dahiti(lake_name):
    json_data = json.load(open(f'Lakes/{lake_name}/Raw/{lake_name}_DAHITI_raw.json'))
    if 'target' in json_data:
        df = pd.DataFrame.from_dict(json_data['target']['data'])
        level_key = 'height'
    else:
        df = pd.DataFrame.from_dict(json_data['data'])
        level_key = 'water_level'
    df['date'] = pd.to_datetime(df['date']).dt.date
    df = df[['date', level_key, 'error']]
    df.rename(columns={"datetime": "date",
                       level_key: "dah",
                       "error": "dah_uncertainty"}, inplace=True)
    df.to_csv(f'Lakes/{lake_name}/{lake_name}_DAHITI.csv', index=False)


def prepare_data_usgs(lake_name):
    df = pd.read_fwf(f'Lakes/{lake_name}/Raw/{lake_name}_USGS_raw.txt')
    df.iloc[:, 4] = df.iloc[:, 4].replace('PST', '-08:00').replace('PDT', '-07:00')
    df['date'] = pd.to_datetime(df.iloc[:, 2] + ' ' + df.iloc[:, 3] + ' ' + df.iloc[:, 4],
                                format='%Y-%m-%d %H:%M %z', utc=True)
    df = df[['date', 'obs']]
    df = df.set_index('date')
    df = df.resample("1d").mean()
    df['date'] = df.index.date
    df = df[['date', 'obs']].reset_index(drop=True)
    df.to_csv(f'Lakes/{lake_name}/{lake_name}_obs.csv', index=False)
    pass
