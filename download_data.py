import os
import sys
import requests
import zipfile
import io

# data access URL
yearly_datasets_dict = {2000: 223, 2001: 224, 2002: 225, 2003: 226, 2004: 202, 2005: 203,
                        2006: 227, 2007: 228, 2008: 229, 2009: 230, 2010: 231, 2011: 232,
                        2012: 233, 2013: 234, 2014: 235, 2015: 236, 2016: 242}
giodo_pjp_url = "http://powietrze.gios.gov.pl/pjp/archives/downloadFile"

for iter_key, iter_item in yearly_datasets_dict.items():
    print(iter_key, ": ", iter_item)

def download_data(yearly_datasets_dict, giodo_pjp_url):
    for iter_key in yearly_datasets_dict:
        pass

test_url = "/".join((giodo_pjp_url, "223"))

r = requests.get(url=test_url, stream=True)
z = zipfile.ZipFile(io.BytesIO(r.content))
z.extractall(path=data_dir)

for key in list(yearly_measurements_dict.keys()):
    print(key)

