﻿# kafkaConnectJsonToParquetConverter
This repo is used to read data as json and add it the schema from schema registry in order to be able to write it to destination locations/formats that requires schema.

for example, to write data as parquest to s3 we should write the data to kafka in avro/jsonSchema formats because it uses schema that the outputed parquet file will use. if we will just use normal json we will get an error saying we should use data with schema.
This repo comes to answer exactly that with using schema registry and topic named schema strategy (<topic-name>-value) to fetch the schema abd add it to the data to be json with schema.