import os

# This file contains application-level constants

# 1. setting the path to /data folder
# get directory to the folder with this script
this_script_dir = os.path.dirname(os.path.abspath(__file__)) # package directory
# check whether a folder /data exists
data_dir = os.path.join(this_script_dir, "data")
if not os.path.exists(path=data_dir):
    os.system(command="mkdir " + data_dir)

# 2. other data-download constants
metadata_sets_dict = {"kody_stacji_pomiarowych": "102", "metadane_stacje_i_stanowiska": "243"}

my_yearly_datasets_dict = {2000: "223", 2001: "224", 2002: "225", 2003: "226", 2004: "202", 2005: "203",
                           2006: "227", 2007: "228", 2008: "229", 2009: "230", 2010: "231", 2011: "232",
                           2012: "233", 2013: "234", 2014: "235", 2015: "236", 2016: "242"}

my_giodo_pjp_url = "http://powietrze.gios.gov.pl/pjp/archives/downloadFile"

pollutants_dict = {"nitrogen_dioxide": "NO2",
                   "ozone": "O3",
                   "particles_10_micrometers": "PM10",
                   "particles_2.5_micrometers": "PM2.5",
                   "sulphur_dioxide": "SO2",
                   "benzene": "C6H6"}

data_frequency = "24g"


dict_cities_tab_colnames = {"id_city", "city_name"}


dict_of_stations_colnames = {'Stary Kod stacji': 'old_station_code',
                             'Kod stacji': 'new_station_code',
                             'Nazwa stacji': 'station_name',
                             'index': 'id_station'}

stations_cities_cols = ['Kod stacji', 'Stary Kod stacji', 'Miejscowosc', 'Nazwa stacji']


dir_data_stations = os.path.expanduser("~/github_repos/PAQA/data/metadane_stacje_i_stanowiska.xlsx")
sheetname_data_stations = "Stacje"

# list below stores names of stations that are present in the data but are
# not present in the data dictionaries
list_of_incorrect_stations_names = ["PkRzeszWIOSLang", "KpPASBTXBydgKujawskie",
                                    "DsOlesnicaPM.1", 'KpPASBTXBydgLPKiW',
                                    'KpPASBTXBydgTorunskie', 'KpPASBTXBydgZachem',
                                    'KpPASTorunWalyGenSik']

# dictionary below stores manually added mappings of stations' codes
dict_of_manually_added_stations = {"LbKra": "LbKra-awWSSE",
                                   "LbZaoscHrubieszowsk": "LbZamoHrubie"}
