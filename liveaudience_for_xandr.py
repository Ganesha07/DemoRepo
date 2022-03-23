from operator import ne
import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, col, lit, struct, collect_list     
from pyspark.sql.types import StructType,StructField, StringType, IntegerType
from pyspark import SparkContext as sc
from smart_open import smart_open
import json
from fastavro import json_reader, parse_schema, writer
import parser
import argparse
import json


# Parsing arguments from the command line
parser = argparse.ArgumentParser(description='live_audiance_script')
parser.add_argument("gz_path",type=str,help="enter gz path")
parser.add_argument("tsv_path",type=str,help="enter tsv path")
parser.add_argument("json_path",type=str,help="enter json output path")
parser.add_argument("avro_schema_path",type=str,help="enter avro schema path")
parser.add_argument("avro_file_path",type=str,help="enter avro output path")
args=parser.parse_args()

gz_path = args.gz_path
tsv_path = args.tsv_path
json_output = args.json_path
avro_schema_path = args.avro_schema_path
avro_output = args.avro_file_path 


# Create a Spark session.
spark = SparkSession.builder.appName('live_audiance').getOrCreate()


# Reading the gz file 
gz_file_Schema = StructType([StructField('id',StringType(),True),
                             StructField('segments',StringType(),True)
                             ])
gz_file_data = spark.read.options(delimiter='\t').schema(gz_file_Schema).csv(gz_path)
selected_gz_file_data = gz_file_data.select('id',split(col('segments'),',').alias("new_segments")).drop('segments')
explod_gz_file_data = selected_gz_file_data.select(selected_gz_file_data.id,
                     explode(selected_gz_file_data.new_segments).alias('segment'))


# Reading the tsv file 
tsv_file_Schema = StructType([StructField('Audience_Name',StringType(),True),
                              StructField('LiveAudience_ID',IntegerType(),True),
                              StructField('Xandr_ID',IntegerType(),True)
                              ])
tsv_file_data = spark.read.options(delimiter='\t').schema(tsv_file_Schema).csv(tsv_path).dropna()
joined_data = explod_gz_file_data.join(tsv_file_data,explod_gz_file_data.segment==tsv_file_data.LiveAudience_ID,'inner').drop('LiveAudience_ID','Audience_Name','segment')


# selected_columns, data frame according to selected columns
selected_columns = joined_data.select('id', 
                                      lit("liveintent.com").alias('source'),
                                      ('Xandr_ID'), 
                                      lit("").alias('code'),
                                      lit(13191).cast('long').alias('member_id'),
                                      lit(0).cast('long').alias('expiration'),
                                      lit(0).cast('long').alias('timestamp'),
                                      lit(0).cast('long').alias('value')
                                      )

# Creating Dataframe according to the nested schema
nested_schema_data=selected_columns.select(struct( 
    struct('id','member_id').alias("external_id")).alias('uid'),
    struct(selected_columns['Xandr_ID'].cast('long').alias('id'),'expiration').alias('segments')).groupby('uid').agg(collect_list('segments').alias('segments'))


# Writing file in json format
nested_schema_data.write.mode('overwrite').format('json').save(f"{json_output}")


# Reading AVRO schema 
with smart_open(avro_schema_path) as fp:
    schema = parse_schema(json.load(fp))


# Writing file in avro format with avro schema
nested_schema_data.write.options(schema="schema").mode('overwrite').format('avro').save(f"{avro_output}")
