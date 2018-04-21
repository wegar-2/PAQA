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

my_yearly_datasets_dict = {2007: "228", 2008: "229", 2009: "230", 2010: "231"}

    # {2000: "223", 2001: "224", 2002: "225", 2003: "226", 2004: "202", 2005: "203",
    #                        2006: "227", 2007: "228", 2008: "229", 2009: "230", 2010: "231", 2011: "232",
    #                        2012: "233", 2013: "234", 2014: "235", 2015: "236", 2016: "242"}

my_giodo_pjp_url = "http://powietrze.gios.gov.pl/pjp/archives/downloadFile"

pollutants_dict = {"nitrogen_dioxide": "NO2",
                   "ozone": "O3",
                   "particles_10_micrometers": "PM10",
                   "particles_2.5_micrometers": "PM2.5",
                   "sulphur_dioxide": "SO2",
                   "benzene": "C6H6"}

