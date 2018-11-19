#!/usr/bin/python

########## Import ########################################
import pandas as pd
import dask.dataframe.io.parquet as ddp
import dask.dataframe as dd
import numpy as np

import os
import sys

########## Variables Init ################################
# Init
# Source files
csvfilePath = sys.argv[1]
# Target files
parquetfilePath = sys.argv[2]
# File/table name
fileName = sys.argv[3]

# To create parquet partitions (Optional argument)
parquetPartition = []
if len(sys.argv) == 5:
    parquetPartition = sys.argv[4].split(',')
    print('Split Parquet files by :')
    print(parquetPartition)
    # Example : Country,Year

tableName = fileName+'.table/'+fileName+'.parquet/'
parquetDataPath = parquetfilePath+tableName

print('Source = '+csvfilePath)
print('Output = '+parquetDataPath)

########## Parquet Convertor ################################

fileArray = os.listdir(csvfilePath)
parquetDataPath = parquetfilePath+tableName

# Browse csv files
for filename in fileArray:
    if filename.endswith(".csv"):
        print('file = '+filename)
        # Create parquet Folder if needed
        if not(os.path.isdir(parquetDataPath)):
            os.makedirs(parquetDataPath)
            print('Create = '+parquetDataPath)
        print('Read file = '+csvfilePath+filename)
        # Read CSV
        df = pd.read_csv(csvfilePath+filename,sep=',', quotechar='"')
        print('to parquet = '+parquetDataPath+filename[:-4]+'.parquet')
        # Parquet partition
        df = dd.from_pandas(df, npartitions=1)
        # Parquet conversion
        ddp.to_parquet(df,parquetDataPath, write_index=False, partition_on=parquetPartition)

##############################################################