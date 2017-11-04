import os
import sys
import logging


### 0. prepare a logger
mlogger = logging.getLogger(name="main_logger") # create a logger
mlogger.setLevel(level=logging.INFO) # set the level on the mlogger logger
# create a handler for mlogger
ch = logging.StreamHandler() # create a handler
ch.setLevel(level=logging.INFO) # set the level for the handler
# configure the formatting of the output produced by console handler
formatter = logging.Formatter(fmt="%(asctime)s - %(name)s - %(level)s - %(message)s")
ch.setFormatter(fmt=formatter)
# add the console handler to mlogger
mlogger.addHandler(hdlr=ch)


# 1. Prepare some other useful variables
mlogger.info(msg="Setting up variables and configuring the directories. ")

this_script_path = os.path.abspath(__file__)
this_script_dir = os.path.dirname(this_script_path) # package directory
# this_script_dir = os.getcwd()
# check whether there is a folder for data
data_dir = os.path.join(this_script_dir, "data")
if not os.path.isfile(path=data_dir):
    os.system(command="mkdir " + data_dir)


if __name__ == "__main__":
    # call the script that downloads the data from http://powietrze.gios.gov.pl/pjp/archives and unpacks it
    mlogger.info(msg="Looking for data on disk and/or getting the data from GIOS website.")
    os.system("python3.6 download_data.py")
