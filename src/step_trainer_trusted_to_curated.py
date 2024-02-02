import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame


def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node accelerometer trusted
accelerometertrusted_node1706811954581 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi-lake",
    table_name="accelerometer_trusted",
    transformation_ctx="accelerometertrusted_node1706811954581",
)

# Script generated for node step trainer trusted
steptrainertrusted_node1706811957800 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi-lake",
    table_name="step_trainer_trusted",
    transformation_ctx="steptrainertrusted_node1706811957800",
)

# Script generated for node join
SqlQuery662 = """
SELECT 
t1.*,
t2.timestamp
FROM 
step_trainer_trusted t1 
INNER JOIN 
accelerometer_trusted t2 
ON 
t1.sensorreadingtime = t2.timestamp;
"""
join_node1706812756428 = sparkSqlQuery(
    glueContext,
    query=SqlQuery662,
    mapping={
        "accelerometer_trusted": accelerometertrusted_node1706811954581,
        "step_trainer_trusted": steptrainertrusted_node1706811957800,
    },
    transformation_ctx="join_node1706812756428",
)

# Script generated for node Drop Fields
DropFields_node1706829961255 = DropFields.apply(
    frame=join_node1706812756428,
    paths=["sensorReadingTime"],
    transformation_ctx="DropFields_node1706829961255",
)

# Script generated for node machine learning curated
machinelearningcurated_node1706813110341 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1706829961255,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://abreu-stedi/machine_learning/curated/",
        "partitionKeys": [],
    },
    transformation_ctx="machinelearningcurated_node1706813110341",
)

job.commit()
