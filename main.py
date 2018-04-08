import os
import sys
import logging
import download_data as dd

# ----------------------------------------------------------------------------------------------------------------------
# 1. Set up logging
# create a logger and set its severity
mlogger = logging.getLogger(name="my_logger")
mlogger.setLevel(level=logging.INFO)

# create a handler and set its severity
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)

# create a formatter and add it to the handler
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)

# add the handler to the logger
mlogger.addHandler(ch)
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# 2. Prepare some other useful variables
# get directory to the folder with this script
this_script_dir = os.path.dirname(os.path.abspath(__file__)) # package directory
# check whether a folder /data exists
data_dir = os.path.join(this_script_dir, "data")
if not os.path.exists(path=data_dir):
    mlogger.info("Folder for data does not exist yet - creating a new one. ")
    os.system(command="mkdir " + data_dir)
else:
    mlogger.info("Folder for data already exists.")
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# 3. Setting up the data base
if __name__ == "__main__":
    # 1. calling the data-downloading function
    mlogger.info("---------------------------------------------------")
    mlogger.info("1. ")
    mlogger.info("Checking the contents of /data.")
    mlogger.info("The /data folder contains the following files: ")
    for iter_num, iter_el in enumerate(os.listdir(data_dir)):
        mlogger.info(msg="\t" + str(iter_num) + ": " + iter_el)
    bool_d = input("Do you want to dowload/re-download files from GIOS archive? "
                   "Type in y/Y for yes or any other input for no. ")
    if bool_d in ("y", "Y"):
        dd.download_data(data_dir=data_dir)
    else:
        pass
    # 2. logging in as root to MySQL DB server to set up a schema and a user
    mlogger.info("---------------------------------------------------")
    mlogger.info("2. ")
    mlogger.info("Uploading the data into MySQL server. ")


