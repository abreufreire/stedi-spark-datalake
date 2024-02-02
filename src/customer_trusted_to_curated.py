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

# Script generated for node customer trusted
customertrusted_node1706731385220 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi-lake",
    table_name="customer_trusted",
    transformation_ctx="customertrusted_node1706731385220",
)

# Script generated for node accelerometer trusted
accelerometertrusted_node1706731387277 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi-lake",
    table_name="accelerometer_trusted",
    transformation_ctx="accelerometertrusted_node1706731387277",
)

# Script generated for node join & drop duplicates
SqlQuery437 = """
SELECT DISTINCT t1.* 
FROM customer_trusted t1 
JOIN accelerometer_trusted t2 
ON t1.email = t2.user;
"""
joindropduplicates_node1706731394309 = sparkSqlQuery(
    glueContext,
    query=SqlQuery437,
    mapping={
        "accelerometer_trusted": accelerometertrusted_node1706731387277,
        "customer_trusted": customertrusted_node1706731385220,
    },
    transformation_ctx="joindropduplicates_node1706731394309",
)

# Script generated for node customer curated
customercurated_node1706731399500 = glueContext.write_dynamic_frame.from_options(
    frame=joindropduplicates_node1706731394309,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://abreu-stedi/customer/curated/",
        "partitionKeys": [],
    },
    transformation_ctx="customercurated_node1706731399500",
)

job.commit()
