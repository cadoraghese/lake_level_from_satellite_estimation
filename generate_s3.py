from utils.downloader import find_products, download_sentinel
from utils.extract_data_netCDF4 import extract_data_sentinel
from shapely import Polygon, Point
from dateutil import parser
import matplotlib
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import datetime
import json

matplotlib.use('TkAgg')

only_hy = 1
limit = 1000
S3_resolution = 0.235
altimetry_method = 'elev'


def plot_altitudes(data):
    ax = plt.figure().add_subplot(111, projection='3d')
    for d in data:
        ax.plot(d[0], d[1], zs=d[2], label=d[3], marker='.')
    # plt.legend(ncol=3)
    ax.set_xlabel('Longitude')
    ax.set_ylabel('Latitude')
    ax.set_zlabel('Altitude')
    plt.title('Altitude at a Specific Latitude and Longitude')
    mng = plt.get_current_fig_manager()
    mng.window.state('zoomed')
    plt.show()


def read_problem(lake_name):
    f = open(f'Lakes/{lake_name}/{lake_name}.json')
    coords = json.load(f)
    f.close()
    return coords


def collect_data(lake_name, start, end):
    coords = read_problem(lake_name)
    df_lake = pd.DataFrame(pd.date_range(start=start, end=end), columns=['date'])

    polygon = Polygon(coords)
    df = find_products(only_hy, limit, start, end, coords)
    # TODO handle more than 1000 products
    if df.empty:
        print('No products found')
        exit(1)
    else:
        print(f'Size: {len(df)}')

    for product_id, name in zip(df['Id'], df['Name']):
        print('Downloading:', name)
        download_sentinel(product_id, name)

    plot_data = []
    for date, name in zip([parser.parse(x['Start']) for x in df['ContentDate']], df['Name']):
        key = f"{name[:3]}_{name.split('______')[0][-3:]}"
        print('Computing', name, key)
        lat, lon, alt = extract_data_sentinel(name, altimetry_method)
        if lat is None or lon is None or alt is None:
            continue

        # Convert longitude to [-180, 180]
        lon = np.where(lon > 180, lon - 360, lon)
        idx = np.sort(np.ma.where([polygon.contains(Point(ll[0], ll[1])) for ll in zip(lat, lon)]), axis=None)
        if idx.size == 0:
            df_lake.loc[df_lake['date'].isin([f"{date:%Y-%m-%d}"]), key] = -9999
            continue
        lat = lat[idx]
        lon = lon[idx]
        alt = alt[idx]

        median_alt = np.median(alt)
        idx = np.where((alt > median_alt - S3_resolution) & (alt < median_alt + S3_resolution))
        lat = lat[idx]
        lon = lon[idx]
        alt = alt[idx]
        median_alt = np.median(alt)
        std_alt = np.std(alt)
        idx = np.where((alt > median_alt - std_alt) & (alt < median_alt + std_alt))
        lat = lat[idx]
        lon = lon[idx]
        alt = alt[idx]

        print(np.mean(alt), len(alt))
        if len(lat) == 0 or len(alt) < 5:
            df_lake.loc[df_lake['date'].isin([f"{date:%Y-%m-%d}"]), key] = -9999
            continue
        plot_data.append([lon, lat, alt, f"{name.split('HY_')[1].split('T')[0]}"])
        df_lake.loc[df_lake['date'].isin([f"{date:%Y-%m-%d}"]), key] = np.mean(alt)

    df_lake.drop(columns=df_lake.columns[df_lake.replace(-9999, np.nan).count() <= 5].values, axis=1, inplace=True)
    df_lake.to_csv(f'Lakes/{lake_name}/{lake_name}_new_{altimetry_method}.csv', index=False)

    plot_altitudes(plot_data)


collect_data('LakeDemo', datetime.datetime(2024, 1, 1), datetime.datetime(2024, 1, 31))
