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

# Script generated for node customer curated
customercurated_node1706746043280 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi-lake",
    table_name="customer_curated",
    transformation_ctx="customercurated_node1706746043280",
)

# Script generated for node step trainer landing
steptrainerlanding_node1706746069174 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi-lake",
    table_name="step_trainer_landing",
    transformation_ctx="steptrainerlanding_node1706746069174",
)

# Script generated for node join & drop duplicates
SqlQuery557 = """
SELECT DISTINCT t2.*
FROM customer_curated t1 
JOIN step_trainer_landing t2 
ON t1.serialnumber = t2.serialnumber;
"""
joindropduplicates_node1706746139238 = sparkSqlQuery(
    glueContext,
    query=SqlQuery557,
    mapping={
        "customer_curated": customercurated_node1706746043280,
        "step_trainer_landing": steptrainerlanding_node1706746069174,
    },
    transformation_ctx="joindropduplicates_node1706746139238",
)

# Script generated for node step trainer trusted
steptrainertrusted_node1706778727418 = glueContext.write_dynamic_frame.from_options(
    frame=joindropduplicates_node1706746139238,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://abreu-stedi/step_trainer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="steptrainertrusted_node1706778727418",
)

job.commit()
