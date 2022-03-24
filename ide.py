import json
from fastavro import json_reader, parse_schema, writer

with open("xandr_avro_sample.avsc") as fp:
    schema = parse_schema(json.load(fp))

with open("1.avro", "wb") as avro_file:
    with open("j.json") as fp:
        writer(avro_file, schema, json_reader(fp, schema))

        
https://github.com/Ganesha07/DemoRepo.git
https://github.com/Ganesha07/DemoRepo.git