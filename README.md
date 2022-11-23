# travel_time_uber
SOLUTION TECHNICAL OVERVIEW
---------------------------
	- Python using Pandas, Geopandas and Pyspark to build pipelines
		
	- Order of execution 
			1 - ing_travel_time_uber.py 	INGESTION
			2 - int_travel_time_uber.py		INTEGRATION
			3 - mng_travel_time_uber.py 	MANAGEMENT
			4 - qry_travel_time_uber.py		QUERY
		
	- INGESTION 
		Storage path: loka/data_lake/ingestion
		Connect do source (data world URL) using a parameter file 'etl\ingestion_parameters.json', 
		Load data keeping its original format (raw data)
		
	- INTEGRATION
		Storage path: loka/data_lake/managed/integration
		Create one repository for all travel data 
        	Match file format and layout for further processing
        	Temporary storage for integration and small transformations

	- MANAGED 
		Storage path: loka/data_lake/managed
		Write files in parquet for better performance
        	Objects padronization, pre stage for dimensional objects
        	Permanent storage, pre stage for dimensional objects
       		Data enrichment (longitude, latitude and distance)
		Generates three tables:	origin_movement, destination_movement and travel_time
				
	- QUERY 
		Dimensional modeling: dim_travel_address and ft_travel_time
		Better business analysis

