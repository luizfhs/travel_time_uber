#!/usr/bin/env python
# coding: utf-8

# In[ ]:


###########################################################################################################################
# Name: Luiz Fernando Sanches
# Date: 23/11/2022
# Ingestion pipeline: connects do source (data world URL) using a parameter file 'etl\ingestion_parameters.json', 
#                     load data keeping its original format (raw data)
###########################################################################################################################

import json
import os
import pandas as pd
from urllib import request
from datetime import date

A = 1

today = date.today()
date_ingestion = today.strftime("%d%m%Y")
job_name = "ing_travel_time_uber"

ingestion_parameters = "ingestion_parameters.json"
out_ingestion_path = "c:/PythonFundamentos/Loka/data_lake/ingestion/raw/travel_time_uber/"

try:
    
    params = open("ingestion_parameters.json", "r")
    content = params.read()

    ingestion = json.loads(content)

    for i in ingestion:
        out_ing_file_name = out_ingestion_path + "travel_time_" + i["location"] + "_" + date_ingestion + ".csv"
        response = request.urlretrieve(i["url"], out_ing_file_name)

except:
    print ("Ingestion not completed successfully, check log info for more details!")
else:
    print ("Ingestion completed successfully!")

