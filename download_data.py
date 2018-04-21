import requests
import zipfile
import io
import logging
import constants
import os

# 0. logger setup
main_logger = logging.getLogger(name="main_logger")


# 2. main function
def download_data(data_dir=constants.data_dir, yearly_datasets_dict=constants.my_yearly_datasets_dict,
                  giodo_pjp_url=constants.my_giodo_pjp_url, metadata_sets_dict=constants.metadata_sets_dict):
    """
    This function is used to download the data from the website and unpack it into the "data" folder of the package
    :param yearly_datasets_dict: dictionary with mappings of years covered by data to
    last elements of paths to zipped data
    :param giodo_pjp_url: main address part
    :param data_dir: directory to the folder into which the downloaded data should be unpacked
    :param metadata_sets_dict: dictionary of files that contain metadata
    """
    main_logger.info(msg="Inside the download_data function: starting the data download/update...")
    # A. metadata files
    main_logger.info(msg="\n\n\n---------- Downloading metadata ----------")
    for iter_key, iter_val in metadata_sets_dict.items():
        main_logger.info(msg="\n\n")
        iter_url = "/".join((giodo_pjp_url, iter_val))
        main_logger.info(msg="Extracting: " + str(iter_key))
        main_logger.info(msg="Getting data from URL: " + iter_url)
        r = requests.get(url=iter_url)
        iter_name = iter_key + ".xlsx"
        main_logger.info(msg="iter_name: " + iter_name)
        main_logger.info(msg="saving to path: " + os.path.join(data_dir, iter_name))
        iter_output = open(file=os.path.join(data_dir, iter_name), mode='wb')
        iter_output.write(r.content)
        iter_output.close()

    # B. data files
    main_logger.info(msg="\n\n\n---------- Downloading data ----------")
    for iter_year, iter_address in yearly_datasets_dict.items():
        main_logger.info(msg="Data for year: " + str(iter_year))
        # iterate over consecutive years
        iter_url = "/".join((giodo_pjp_url, iter_address))
        main_logger.info("Downloading data from address: " + iter_url)
        r = requests.get(url=iter_url, stream=True) # make a query and save the response into 'r'
        z = zipfile.ZipFile(io.BytesIO(r.content)) # save the content of request result into zipfile
        # ZipFile.namelist() <== use it to check whether the files are already available
        main_logger.info(msg="Displaying the z.namelist(): ")
        main_logger.info(msg=z.namelist())
        z.extractall(path=data_dir) # extract into a specific location

