import logging
import pandas as pd
import sys
import sqlalchemy as sa
import os


# 0. create a logger
main_logger = logging.getLogger(name="main_logger")


# ----------------------------------------------------------------------------------------------------------------------
# List of tables to create in the DB:
#