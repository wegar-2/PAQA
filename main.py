import logging
import download_data as dd
import os
import prepare_DB_server
import setup_of_the_DB

# ----------------------------------------------------------------------------------------------------------------------
# 1. Set up logging
# create a logger and set its severity
main_logger = logging.getLogger(name="main_logger")
main_logger.setLevel(level=logging.INFO)

# create a handler and set its severity
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)

# create a formatter and add it to the handler
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)

# add the handler to the logger
main_logger.addHandler(hdlr=ch)
# ----------------------------------------------------------------------------------------------------------------------


# ----------------------------------------------------------------------------------------------------------------------
# 2. Setting up the data base
if __name__ == "__main__":
    # 1. calling the data-downloading function
    main_logger.info("---------------------------------------------------")
    main_logger.info(msg="Displaying general info: ")
    main_logger.info(msg="os.getcwd(): " + os.getcwd())

    main_logger.info("---------------------------------------------------")
    main_logger.info("1. Downloading data from GIOS website. ")
    main_logger.info("Calling the download_data function...")
    # dd.download_data()

    # 2. logging in as root to MySQL DB server to set up a schema and a user
    main_logger.info(msg="---------------------------------------------------")
    main_logger.info(msg="2. ")
    main_logger.info(msg="Setting up the database before uploading data... ")
    main_logger.info(msg="Creation of user and schema: ")
    prepare_DB_server.prepare_DB_server()
    main_logger.info(msg="Executing the script: setup_of_the_DB.py")
    setup_of_the_DB.setup_of_the_DB()

