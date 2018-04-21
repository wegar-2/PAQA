import logging
import pandas as pd
import sys
import sqlalchemy as sa
import os


# 0. create a logger
main_logger = logging.getLogger(name="main_logger")


def setup_of_the_DB():
    # ------------------------------------------------------------------------------------------------------------
    # List of tables to create in the DB:
    # 1. CITIES
    # 2. STATIONS
    # 3. POLLUTION_DATA
    main_logger.info("Inside function setup_of_the_DB. Starting creation of tables. ")
    main_logger.info("\n\nThe following tables will be created: ")
    main_logger.info("\n\t1. CITIES\n\t2. STATIONS\n\t3. POLLUTION_DATA\n\n\n")

    mysqldb_engine = sa.create_engine("mysql+mysqldb://PAQA_USER:pass@localhost/PAQA_DB")

    # ------------------------------------------------------------------------------------------------------------
    # ------------------------------------------------------------------------------------------------------------
    # 1. Creating table: CITIES
    main_logger.info("\n\nCreating table: CITIES")
    metadata1 = sa.MetaData(bind=mysqldb_engine)
    try:
        sa.Table('CITIES', metadata1,
                 sa.Column('id_city', sa.INT, primary_key=True,
                           autoincrement=True, nullable=False),
                 sa.Column('city_name', sa.VARCHAR(100), nullable=False))
        metadata1.create_all()
    except Exception as exc:
        main_logger.error(msg="Error occurred when trying to create table CITIES")
        main_logger.error(msg=exc)

    # ------------------------------------------------------------------------------------------------------------
    # ------------------------------------------------------------------------------------------------------------
    # 2. Creating table: STATIONS
    main_logger.info("\n\nCreating table: STATIONS")
    try:
        sa.Table('STATIONS', metadata1,
                 sa.Column('id_station', sa.INT, primary_key=True, autoincrement=True, nullable=False),
                 sa.Column('old_station_name', sa.VARCHAR(100), nullable=False),
                 sa.Column('new_station_name', sa.VARCHAR(100), nullable=False),
                 sa.Column('id_city', sa.INT, sa.ForeignKey('CITIES.id_city'), nullable=False))
        metadata1.create_all()
    except Exception as exc:
        main_logger.error(msg="Error occurred when trying to create table STATIONS")
        main_logger.error(msg=exc)

    # ------------------------------------------------------------------------------------------------------------
    # ------------------------------------------------------------------------------------------------------------
    main_logger.info("\n\nCreating table: POLLUTION_DATA")
    try:
        sa.Table('POLLUTION_DATA', metadata1,
                 sa.Column('id_pollution_data', sa.INT, primary_key=True, autoincrement=True, nullable=False),
                 sa.Column('id_station', sa.INT, sa.ForeignKey('STATIONS.id_station'), nullable=False),
                 sa.Column('measurement_date', sa.DATE, nullable=False),
                 sa.Column('measurement_value', sa.FLOAT(18, 4), nullable=False),
                 sa.Column('measurement_unit', sa.VARCHAR(10), nullable=False),
                 sa.Column('pollutant', sa.VARCHAR(5), nullable=False))
        metadata1.create_all()
    except Exception as exc:
        main_logger.error(msg="Error occurred when trying to create table POLLUTION_DATA")
        main_logger.error(msg=exc)

