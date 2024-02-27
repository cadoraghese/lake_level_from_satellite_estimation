from utils.prepare_data import *
import os.path

lake_name = 'LakeDemo'

if os.path.exists(f"Lakes/{lake_name}/Raw/{lake_name}_CDS_raw.nc"):
    prepare_cds(lake_name)

if os.path.exists(f"Lakes/{lake_name}/Raw/{lake_name}_VITO_raw.json"):
    prepare_data_vito(lake_name)

if os.path.exists(f"Lakes/{lake_name}/Raw/{lake_name}_GW_raw.csv") and \
        os.path.exists(f"Lakes/{lake_name}/Raw/{lake_name}_lsv_rel.txt"):
    prepare_data_gw(lake_name)

if os.path.exists(f"Lakes/{lake_name}/Raw/{lake_name}_NASA_raw.txt"):
    prepare_data_nasa(lake_name)

if os.path.exists(f"Lakes/{lake_name}/Raw/{lake_name}_DAHITI_raw.json"):
    prepare_data_dahiti(lake_name)

if os.path.exists(f"Lakes/{lake_name}/Raw/{lake_name}_USGS_raw.txt"):
    prepare_data_usgs(lake_name)
