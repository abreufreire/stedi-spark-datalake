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

# Script generated for node customer landing
customerlanding_node1706119853232 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi-lake",
    table_name="customer_landing",
    transformation_ctx="customerlanding_node1706119853232",
)

# Script generated for node privacy filter
SqlQuery2814 = """
SELECT *
FROM customer_landing
WHERE sharewithresearchasofdate IS NOT NULL;
"""
privacyfilter_node1705997052437 = sparkSqlQuery(
    glueContext,
    query=SqlQuery2814,
    mapping={"customer_landing": customerlanding_node1706119853232},
    transformation_ctx="privacyfilter_node1705997052437",
)

# Script generated for node customer trusted
customertrusted_node1705998330821 = glueContext.write_dynamic_frame.from_options(
    frame=privacyfilter_node1705997052437,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://abreu-stedi/customer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="customertrusted_node1705998330821",
)

job.commit()
