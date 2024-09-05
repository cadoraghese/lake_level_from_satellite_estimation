import numpy as np
import pandas as pd
import netCDF4
import datetime


def extract_data_sentinel(name, method='elev'):
    file_path = f'S3/{name}/standard_measurement.nc'

    # Open the netCDF file with netCDF4
    with netCDF4.Dataset(file_path, 'r') as dataset:
        alt = dataset.variables['alt_20_ku'][:]
        if method == 'alt1':
            altimetry = alt - dataset.variables['range_ocog_20_ku'][:]
        elif method == 'elev':
            altimetry = dataset.variables['elevation_ocog_20_ku'][:]
        elif method == 'alt2':
            altimetry = alt - dataset.variables['range_water_20_ku'][:]
        else:
            print('Wrong method')
            exit(7)

        # Access the latitude and longitude coordinates
        latitude = dataset.variables['lat_20_ku'][:]
        latitude = np.ma.masked_where(np.ma.getmask(altimetry), latitude)
        longitude = dataset.variables['lon_20_ku'][:]
        longitude = np.ma.masked_where(np.ma.getmask(altimetry), longitude)

    return latitude, longitude, altimetry


def extract_data_cds(file_path):
    # Open the netCDF file with netCDF4
    with netCDF4.Dataset(file_path, 'r') as dataset:
        df = pd.DataFrame(dataset.variables['time'][:], columns=['date'])
        df['date'] = [datetime.date(1950, 1, 1) + datetime.timedelta(days=delta) for delta in df['date']]
        df['cds'] = dataset.variables['water_surface_height_above_reference_datum'][:]
        df['cds_uncertainty'] = dataset.variables['water_surface_height_uncertainty'][:]

    return df
