import requests
import json
import cdsapi
import zipfile
import os
import pandas as pd

access_token = None
refresh_token = None


def find_products(only_hy, limit, start, end, coordinates):
    coordinates += [coordinates[0]]
    coordinates_o_data = [x[::-1] for x in coordinates]
    conditions = [f"contains(Name,'SR_2_LAN{'_HY' if only_hy else ''}')",
                  f"not contains(Name,'LAN_SI')",
                  f"not contains(Name,'LAN_LI')",
                  f"not contains(Name,'0PP')",
                  f"ContentDate/Start gt {start.strftime('%Y-%m-%dT%H:%M:%SZ')}",
                  f"ContentDate/Start lt {end.strftime('%Y-%m-%dT%H:%M:%SZ')}",
                  f"OData.CSC.Intersects(area=geography'SRID=4326;POLYGON(("
                  f"{','.join([f'{x[0]} {x[1]}' for x in coordinates_o_data])}))')",
                  f"contains(Name,'_NT_')"]

    json_response = requests.get(f"https://catalogue.dataspace.copernicus.eu/odata/v1/Products?$filter="
                                 f"{' and '.join([x for x in conditions if x])}"
                                 f"&$orderby=ContentDate/Start{f'&$top={limit}' if limit else ''}").json()
    df = pd.DataFrame.from_dict(json_response['value'])
    return df


def get_access_token():
    url = "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token"
    headers = {
        "Content-Type": "application/x-www-form-urlencoded"
    }
    if not os.path.exists('utils/credentials.json'):
        print("Please create a credentials.json file in the utils folder with the following structure:\n")
        print("{\n\t\"username\": \"your_username\",\n\t\"password\": \"your_password\"\n}")
        exit(1)
    f = open('utils/credentials.json')
    credentials = json.load(f)
    f.close()
    params = {
        "grant_type": "password",
        "username": credentials['username'],
        "password": credentials['password'],
        "client_id": "cdse-public",
    }
    response = requests.post(url, headers=headers, data=params)
    response = response.json()
    global access_token, refresh_token
    access_token = response['access_token']
    refresh_token = response['refresh_token']


def download_sentinel(product_id, name):
    if os.path.exists(f"S3/{name}"):
        return
    if not access_token:
        get_access_token()
    url = f"https://zipper.dataspace.copernicus.eu/odata/v1/Products({product_id})/$value"

    headers = {"Authorization": f"Bearer {access_token}"}

    session = requests.Session()
    session.headers.update(headers)
    response = session.get(url, headers=headers, stream=True)

    with open(f"S3/{name}.zip", "wb") as file:
        for chunk in response.iter_content(chunk_size=8192):
            if chunk:
                file.write(chunk)
    extract_sentinel(name)
    os.remove(f"S3/{name}.zip")
    for file in os.listdir(f"S3/{name}"):
        if file != "standard_measurement.nc":
            os.remove(f"S3/{name}/{file}")


def download_cds(lake_name, lake, region):
    c = cdsapi.Client()

    c.retrieve(
        'satellite-lake-water-level',
        {
            'variable': 'all',
            'version': 'version_4_0',
            'format': 'zip',
            'lake': lake,
            'region': region,
        },
        f'Lakes/{lake_name}/Raw/{lake_name}_CDS.zip')
    with zipfile.ZipFile(f"Lakes/{lake_name}/Raw/{lake_name}_CDS.zip", 'r') as zip_ref:
        if os.path.exists(f"Lakes/{lake_name}/Raw/{lake_name}_CDS.nc"):
            os.remove(f"Lakes/{lake_name}/Raw/{lake_name}_CDS.nc")
        for file in zip_ref.namelist():
            zip_ref.extract(file)
            os.rename(file, f"Lakes/{lake_name}/Raw/{lake_name}_CDS.nc")
    os.remove(f"Lakes/{lake_name}/Raw/{lake_name}_CDS.zip")


def extract_sentinel(name, destination='S3'):
    with zipfile.ZipFile(f"S3/{name}.zip", 'r') as zip_ref:
        zip_ref.extractall(destination)
