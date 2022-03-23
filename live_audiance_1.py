from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, col, lit, struct, collect_list     
from pyspark.sql.types import StructType,StructField, StringType, IntegerType
from pyspark import SparkContext as sc
from smart_open import smart_open
from fastavro import parse_schema
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
    struct(selected_columns['Xandr_ID'].cast('long').alias('id'),'expiration' ).alias('segments')).groupby('uid').agg(collect_list('segments').alias('segments'))
# ,"timestamp", "value", "code"

# schema = avro.schema.parse(smart_open(avro_schema_path, "rb").read())

# Writing file in json format
nested_schema_data.write.mode('overwrite').format('json').save(f"{json_output}")

with smart_open(avro_schema_path) as fp:
    schema = parse_schema(json.load(fp))

nested_schema_data.write.options(schema="schema").mode('overwrite').format('com.databricks.spark.avro').save(f"{avro_output}")


# schema_list = []
# custom_json = json.loads(smart_open(f"{avro_schema_path}", 'rb').read())
# schema_list.append(custom_json)

# schema_json = json.dumps(schema_list)
# full_msg_schema = avro.schema.parse(schema_json)

# nested_schema_data.write.options(schema="full_msg_schema").mode('overwrite').format('com.databricks.spark.avro').save(f"{avro_output}")



# import avro.schema
# from avro.datafile import DataFileReader, DataFileWriter
# from avro.io import DatumReader, DatumWriter
# import json


# schema = avro.schema.parse(smart_open(avro_schema_path, "rb").read())
# writer = DataFileWriter(smart_open(f"{avro_output}", "wb"), DatumWriter(), schema)
# with smart_open("s3://dummybucketpankaj/liveaudiance_output/json/part-00000-c592c10c-ff36-4316-91f5-11ee852ecca5-c000.json") as fp:
# 	contents = json.loads(fp)
# 	for record in contents:
# 		writer.append(record)
#       print(contents)
	



# nd=spark.read.json(f"{json_output}*", multiLine=True)
# from avro.datafile import DataFileReader, DataFileWriter
# from avro.io import DatumReader, DatumWriter
# import avro.schema
# import json
# json_schema_str = json.dumps(my_schema)
# schema = avro.schema.parse(json_schema_str)
# nested_schema_data.write.format("avro").mode('overwrite').option("avroSchema", schema).save("s3://dummybucketpankaj/liveaudiance_output/avro_sample/")


# writer = DataFileWriter(smart_open("s3://dummybucketpankaj/liveaudiance_output/avro_sample/part-00000-f5229c41-304d-4399-9a89-4db76e8e1672-c000.avro", "wb"), DatumWriter(), schema)
# writer.append(nested_schema_data)
# writer.close()


# from avro.io import DatumWriter, BinaryEncoder
# import io
# #write binary
# file = smart_open("s3://dummybucketpankaj/liveaudiance_output/avro_sample/part-00000-f5229c41-304d-4399-9a89-4db76e8e1672-c000.avro", 'wb')
# #append binary
# file = smart_open("s3://dummybucketpankaj/liveaudiance_output/avro_sample/part-00000-f5229c41-304d-4399-9a89-4db76e8e1672-c000.avro", 'rb')
# bytes_writer = io.BytesIO()
# datum_writer = DatumWriter()
# encoder = BinaryEncoder(bytes_writer)
# writer_binary = DatumWriter(my_schema)
# writer_binary.write_record(nested_schema_data)
# file.write(bytes_writer.getvalue())







# file = smart_open("s3://dummybucketpankaj/liveaudiance_output/avro_sample/*", 'wb')
 
# datum_writer = DatumWriter()
# fwriter = DataFileWriter(file, datum_writer)
# fwriter.append({"uid":{"external_id":{"id":"--9TEX-BpFCaXJGt21dkcFdDWz7omGyUmUPk7Q","member_id":13191}},"segments":[{"id":12368,"expiration":0},{"id":12376,"expiration":0}]})
# fwriter.close()




# nested_schema_data.write.option("schema",schema_A).avro("s3://dummybucketpankaj/liveaudiance_output/avro_sample/")





# schema = avro.schema.parse(smart_open(f"{avro_schema_path}", "rb").read())
# schemaa = avro.schema.parse(smart_open(f"{avro_schema_path}", "r").read())
# jsonFormatSchema = smart_open(avro_schema_path, "r").read()
# dataFile    = smart_open("s3://dummybucketpankaj/liveaudiance_output/avro_sample/*", "wb")
# writer = DataFileWriter(dataFile, DatumWriter(), schema)

# nested_schema_data.write.format("avro").options(schema="schema_avro").save(f"{avro_output}", mode="overwrite")
# nested_schema_data.write.format("avro").save(f"{avro_output}", mode="overwrite")
# nested_schema_data.write.options(schema="schema_avro").mode('overwrite').format("org.apache.spark.sql.avro.AvroFileFormat").save(f"{avro_output}")


# jsonFormatSchema = smart_open("s3://dummybucketpankaj/script/main_script/xandr_avro_sample.avsc", "r").read()

# nested_schema_data.select(from_avro("uid", jsonFormatSchema)).show(truncate=False)
# nested_schema_data.write.mode('append').option("avroSchema", schema_avro).avro("s3://dummybucketpankaj/liveaudiance_output/avro_sample/")


'''
driver = create_clidriver()


# Collecting file's path from S3 bucket where avro files are stored in the 
# "avro_files_list",for deleting avro file if they already exist to remove 
# duplicacy.
avro_output_str=f"{avro_output}"
avro_full_path = f"{avro_output_str}*.avro"
avro_files_list = []
hadoopPath = sc._jvm.org.apache.hadoop.fs.Path(avro_full_path)
hadoopFs = hadoopPath.getFileSystem(sc._jvm.org.apache.hadoop.conf.Configuration())
statuses = hadoopFs.globStatus(hadoopPath)
for status in statuses:
    avro_files_list.append(avro_output_str+status.getPath().getName())
for avro_file in avro_files_list:
    driver.main(f's3 rm {avro_file}'.split())


# # Collecting file's path from S3 bucket where json files are stored 
# # in the "json_files_list"
json_output_str=f"{json_output}"
json_full_path = f"{json_output_str}*.json"
json_files_list = []
hadoopPath = sc._jvm.org.apache.hadoop.fs.Path(json_full_path)
hadoopFs = hadoopPath.getFileSystem(sc._jvm.org.apache.hadoop.conf.Configuration())
valid_file = hadoopFs.globStatus(hadoopPath)
for file in valid_file:
    json_files_list.append(json_output_str+file.getPath().getName())



# with smart_open(avro_schema_path) as fp:
#     # schema_avro = parse_schema(json.dump(fp))
#     schema_avro = avro.schema.parse(fp.read())

# Reading AVRO schema 
with smart_open(avro_schema_path) as fp:
    schema = parse_schema(json.load(fp))


# Itreating json files from "json_files_list" and converting to Avro 
# format, Storing avro files with the same name as json file. 
for json_file in json_files_list:
	path=json_file.split('/')[-1].split('.json')[-2]
	with smart_open(f"{avro_output}{path}.avro", "wb") as avro_file:
		with smart_open(json_file) as fp:
			writer(avro_file, schema, json_reader(fp, schema))
'''