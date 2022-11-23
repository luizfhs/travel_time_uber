#!/usr/bin/env python
# coding: utf-8

# In[ ]:


###########################################################################################################################
# Name: Luiz Fernando Sanches
# Date: 23/11/2022
# Query pipeline: Dimensional modeling 
#                 Better business analysis
###########################################################################################################################

import os
import pyspark
import pandas as pd
from pyspark.sql import SparkSession
from datetime import date
spark = SparkSession.builder                     .master('local[1]')                     .appName('qry_travel_time_uber')                     .getOrCreate()

today = date.today()
date_query = today.strftime("%d%m%Y")
job_name = "qry_travel_time_uber"

in_manage_path =  "/Loka/data_lake/managed/"
out_query_path = "/Loka/data_lake/query/"

try: 
    
    df_origin = spark.read.parquet(in_manage_path +"origin_movement/*.parquet")
    df_destination = spark.read.parquet(in_manage_path +"destination_movement/*.parquet")
    df_travel_time = spark.read.parquet(in_manage_path +"travel_time/*.parquet")

    df_origin.createOrReplaceTempView("origin_movement")
    df_destination.createOrReplaceTempView("destination_movement")
    df_travel_time.createOrReplaceTempView("travel_time")

    df_dim_travel_address = spark.sql("SELECT travel_address_origin_id travel_address_id "                                        ", UPPER(travel_address_origin_nm) travel_address_nm "                                        ", origin_longitude_id travel_address_longitude_id "                                        ", origin_latitude_id travel_address_latitude_id "                                       "FROM origin_movement "                                       "UNION "                                       "SELECT travel_address_destination_id travel_address_id "                                       ", UPPER(travel_address_destination_nm) travel_address_nm "                                       ", destination_longitude_id travel_address_longitude_id "                                       ", destination_latitude_id travel_address_latitude_id "                                       "FROM destination_movement ")

    df_ft_travel_time = spark.sql("SELECT travel_address_origin_id "                                       ", travel_address_destination_id "                                       ", date_range_ds "                                       ", mean_travel_time_sec_vl "                                       ", lower_travel_time_sec_vl "                                       ", upper_travel_time_sec_vl "                                       ", travel_distance_vl "                                   "FROM travel_time " )      

    df_dim_travel_address.write.mode("overwrite").parquet(out_query_path + "/dim_travel_address/")
    df_ft_travel_time.write.mode("overwrite").parquet(out_query_path + "/ft_travel_time/")
except:
    print ("Query process not completed successfully, check log info for more details!")
else:
    print ("Query process completed successfully!")

