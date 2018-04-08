import os
import sys
import requests
import zipfile
import io


# data access URL
my_yearly_datasets_dict = {2000: "223", 2001: "224", 2002: "225", 2003: "226", 2004: "202", 2005: "203",
                           2006: "227", 2007: "228", 2008: "229", 2009: "230", 2010: "231", 2011: "232",
                           2012: "233", 2013: "234", 2014: "235", 2015: "236", 2016: "242"}

my_giodo_pjp_url = "http://powietrze.gios.gov.pl/pjp/archives/downloadFile"


def download_data(data_dir, yearly_datasets_dict = my_yearly_datasets_dict, giodo_pjp_url = my_giodo_pjp_url):
    """
    This function is used to download the data from the website and unpack it into the "data" folder of the package
    :param yearly_datasets_dict: dictionary with mappings of years covered by data to
    last elements of paths to zipped data
    :param giodo_pjp_url: main address part
    :param data_dir: directory to the folder into which the downloaded data should be unpacked
    """

    # what should be added here is checking whether file exists before actually starting the extraction

    for iter_year, iter_address in yearly_datasets_dict.items():
        print("Data for year: ", iter_year)
        # iterate over consecutive years
        iter_url = "/".join((giodo_pjp_url, iter_address))
        print("Downloading from address: ", iter_url)
        r = requests.get(url=iter_url, stream=True) # make a query and save the response into 'r'
        z = zipfile.ZipFile(io.BytesIO(r.content)) # save the content of request result into zipfile
        # ZipFile.namelist() <== use it to check whether the files are already available
        # z.namelist()
        z.extractall(path=data_dir) # extract into a specific location


