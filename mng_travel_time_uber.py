#!/usr/bin/env python
# coding: utf-8

# In[ ]:


###########################################################################################################################
# Name: Luiz Fernando Sanches
# Date: 23/11/2022
# Management pipeline: Write files in parquet for better performance
#                      Objects padronization, pre stage for dimensional objects
#                      Permanent storage, pre stage for dimensional objects
#                      Data enrichment (longitude, latitude and distance)
#                      Use GeoPandas for geometry map and space calculations
###########################################################################################################################

import os
import pandas as pd
import shapely
import geopandas as gpd
from shapely.geometry import Point, Polygon
from datetime import date

today = date.today()
date_managed = today.strftime("%d%m%Y")
job_name = "mng_travel_time_uber"

in_integration_path = "/Loka/data_lake/managed/integration/travel_time/"
out_manage_path = "/Loka/data_lake/managed/"

if len(file) > 1:
    print("Verificar pasta de entrada, deve existir apenas um arquivo!")
    exit()
    
in_file_path  = in_integration_path + file[0]

try: 
    
    df_pd = pd.read_table(in_file_path, sep = ',')

    #Data enrichment: Calculate poligon based on geometry list on columns origin geometry and destination geometry
    df_pd["origin_geometry_polygon_id"] = df_pd["Origin Geometry"].apply(lambda x: Polygon(eval(x)))
    df_pd["destination_geometry_polygon_id"] = df_pd["Destination Geometry"].apply(lambda x: Polygon(eval(x)))

    df_gpd_origin = gpd.GeoDataFrame(df_pd)

    df_gpd_origin = df_gpd_origin.set_geometry("origin_geometry_polygon_id")

    #Data enrichment:Calculate center of origin area
    df_gpd_origin["origin_geometry_center_id"] = df_gpd_origin.centroid.centroid

    df_gpd_destination = gpd.GeoDataFrame(df_pd)

    df_gpd_destination = df_gpd_destination.set_geometry("destination_geometry_polygon_id")

    #Data enrichment:Calculate center of destination area
    df_gpd_destination["destination_geometry_center_id"] = df_gpd_destination.centroid.centroid

    df_gpd_origin["destination_geometry_center_id"] = df_gpd_destination["destination_geometry_center_id"]

    #Data enrichment: distance value between origin and source
    df_gpd_origin["travel_distance_vl"] = df_gpd_origin.distance(df_gpd_destination)

    #Data enrichment:Calculate longitude and latitude on origin and destination
    df_gpd_origin['origin_longitude_id'] = df_gpd_origin['origin_geometry_center_id'].x
    df_gpd_origin['origin_latitude_id'] = df_gpd_origin['origin_geometry_center_id'].y
    df_gpd_origin['destination_longitude_id'] = df_gpd_origin['destination_geometry_center_id'].x
    df_gpd_origin['destination_latitude_id'] = df_gpd_origin['destination_geometry_center_id'].y

    df_gpd_origin = df_gpd_origin.drop(["origin_geometry_polygon_id"], axis=1)
    df_gpd_origin = df_gpd_origin.drop(["destination_geometry_polygon_id"], axis=1)
    df_gpd_origin = df_gpd_origin.drop(["origin_geometry_center_id"], axis=1)
    df_gpd_origin = df_gpd_origin.drop(["destination_geometry_center_id"], axis=1)

    #Objects column names padronization
    df_gpd_origin = df_gpd_origin.rename(columns={"Origin Movement ID": "travel_address_origin_id", "Origin Display Name": "travel_address_origin_nm", "Origin Geometry": "travel_address_origin_geometry_ds"})
    df_gpd_origin_movement = df_gpd_origin[["travel_address_origin_id", "travel_address_origin_nm", "travel_address_origin_geometry_ds", "origin_longitude_id", "origin_latitude_id"]]
    df_gpd_origin_movement = df_gpd_origin_movement.drop_duplicates(subset=['travel_address_origin_id'])

    #Objects column names padronization
    df_gpd_origin = df_gpd_origin.rename(columns={"Destination Movement ID": "travel_address_destination_id", "Destination Display Name": "travel_address_destination_nm", "Destination Geometry": "travel_address_origin_geometry_ds"})
    df_gpd_destination_movement = df_gpd_origin[["travel_address_destination_id", "travel_address_origin_nm", "travel_address_destination_nm", "destination_longitude_id", "destination_latitude_id"]]
    df_gpd_destination_movement = df_gpd_destination_movement.drop_duplicates(subset=['travel_address_destination_id'])

    df_gpd_origin = df_gpd_origin.rename(columns={"Date Range": "date_range_ds", "Mean Travel Time (Seconds)": "mean_travel_time_sec_vl", "Range - Lower Bound Travel Time (Seconds)": "lower_travel_time_sec_vl", "Range - Upper Bound Travel Time (Seconds)": "upper_travel_time_sec_vl"})
    df_gpd_travel_time = df_gpd_origin[["travel_address_origin_id", "travel_address_destination_id", "date_range_ds", "mean_travel_time_sec_vl", "lower_travel_time_sec_vl", "upper_travel_time_sec_vl", "travel_distance_vl"]]

    #Generate parquet files
    df_gpd_destination_movement.to_parquet(out_manage_path + "destination_movement/" + "destination_movement_" + date_managed + ".parquet")
    df_gpd_origin_movement.to_parquet(out_manage_path + "origin_movement/" + "origin_movement_" + date_managed + ".parquet")
    df_gpd_travel_time.to_parquet(out_manage_path + "travel_time/" + "travel_time_" + date_managed + ".parquet")
except:
    print ("Management not completed successfully, check log info for more details!")
else:
    print ("Management completed successfully!")

