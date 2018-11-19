# Easy-CSVtoParquet

A simple script to convert a list of csv files into parquet files.

**Author:** Baptiste Durand

**License**: MIT Licensed  

Created and used for a personal project where a pre-requisite was to have a list of tables in parquet format.


## Pre-requisites
Python 3.6 (not tested with other versions)

Imported libraries :

	- os
	
	- sys
	
	- pandas
	
	- numpy
	
	- dask (dask.dataframe)
	

## How-to

Call program with the following argument : 

	- Source folder of csv files
	
	- Target folder for parquet files (created if not exist)
	
	- Target table name
	
	- Partition fields (Optional)
	

`easyConvert_CSVtoParquet.py Source_CSV_Folder Target_Parquet_Folder Table_Name [Partition_fields]`

Example : 

`easyConvert_CSVtoParquet.py ./mySourceFolder/ ./myTargetFolder/ myParquetTable Country,Year`

Create parquet files from csv files located in mySourceFolder to myTargetFolder in folders Table_Name.table/Table_Name.parquet/ 

Create partitions based on fields Country and Year


## Context

Created for an easy and one-shot project.

Parquet Partitions should used with 1 csv source file.

If you data source is already split in different CSV, your data will be split in different parquet in the same folder (originally used by this way).

