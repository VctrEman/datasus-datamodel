import pandas as pd
from pysus.online_data import IBGE
from pysus.ftp.utils import zfill_year

dbfs_raw_path = "./adls/bronze/"


# Fetch available years
def get_available_years(source):
    return sorted(set([zfill_year(f.name[-2:]) for f in IBGE.ibge.get_files(source=source)]))

# Download population data
pop_list = [IBGE.get_population(year, source="POP") for year in get_available_years("POP")]

df = pd.concat(pop_list)

df.to_csv(dbfs_raw_path + "population.csv",index=False)