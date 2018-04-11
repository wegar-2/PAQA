import logging
import download_data as dd

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
    main_logger.info("1. Downloading and/or uploaging data from GIOS website. ")
    main_logger.info("Calling the download_data function...")
    dd.download_data()

    # 2. logging in as root to MySQL DB server to set up a schema and a user
    main_logger.info("---------------------------------------------------")
    main_logger.info("2. ")
    main_logger.info("Uploading the data into MySQL server. ")


