#!/usr/bin/env python
# coding: utf-8

# In[ ]:


###########################################################################################################################
# Name: Luiz Fernando Sanches
# Date: 23/11/2022
# Integration pipeline: Create one repository for all travel data 
#                       Match file format and layout for further processing
#                       Temporary storage for integration and small transformations
###########################################################################################################################

import os
import pandas as pd
from datetime import date

today = date.today()
date_integration = today.strftime("%d%m%Y")
job_name = "int_travel_time_uber"

in_ingestion_path = "/Loka/data_lake/ingestion/raw/travel_time_uber/"
out_integration_path = "/Loka/data_lake/managed/integration/travel_time/"

lstGeo =[]
lstNon_Geo =[]

try:
    
    #clear target folder
    for t in os.listdir(out_integration_path):
        os.remove(out_integration_path + t)
    
    #create one repository for all travel data and match file format and layout since some files have geometry columns missing
    for f in os.listdir(in_ingestion_path):
        filename = in_ingestion_path + f
        file = open(filename, "r")
        content = file.read()
        if content.find("Geometry") > 0:
            lstGeo.append(filename) 
        else:
            lstNon_Geo.append(filename) 

    df_geo = pd.concat(map(pd.read_csv, lstGeo))

    df_non_geo = pd.concat(map(pd.read_csv, lstNon_Geo))

    df_non_geo["Origin Geometry"] = '[]'
    df_non_geo["Destination Geometry"] = '[]'

    df_integration = pd.concat([df_geo, df_non_geo])

    df_integration.to_csv(out_integration_path + 'travel_time_' + date_integration + '.csv', header=True)

except:
    print ("Integration not completed successfully, check log info for more details!")
else:
    print ("Integration completed successfully!")

