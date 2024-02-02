CREATE EXTERNAL TABLE `machine_learning_curated`(
  `timestamp` bigint COMMENT 'from deserializer',
  `serialnumber` string COMMENT 'from deserializer',
  `distancefromobject` bigint COMMENT 'from deserializer')
ROW FORMAT SERDE
  'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  'case.insensitive'='TRUE',
  'dots.in.keys'='FALSE',
  'ignore.malformed.json'='FALSE',
  'mapping'='TRUE')
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://abreu-stedi/machine_learning/curated'
TBLPROPERTIES (
  'classification'='json',
  'transient_lastDdlTime'='1706049786')
