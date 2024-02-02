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
customertrusted_node1706121701938 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi-lake",
    table_name="customer_trusted",
    transformation_ctx="customertrusted_node1706121701938",
)

# Script generated for node accelerometer landing
accelerometerlanding_node1706121852675 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi-lake",
    table_name="accelerometer_landing",
    transformation_ctx="accelerometerlanding_node1706121852675",
)

# Script generated for node join
SqlQuery409 = """
SELECT
t1.*,
t2.*
FROM
customer_trusted t1
INNER JOIN
accelerometer_landing t2
ON
t1.email = t2.user;
"""
join_node1706122139032 = sparkSqlQuery(
    glueContext,
    query=SqlQuery409,
    mapping={
        "customer_trusted": customertrusted_node1706121701938,
        "accelerometer_landing": accelerometerlanding_node1706121852675,
    },
    transformation_ctx="join_node1706122139032",
)

# Script generated for node drop fields
dropfields_node1706134231903 = DropFields.apply(
    frame=join_node1706122139032,
    paths=[
        "customerName",
        "email",
        "phone",
        "birthDay",
        "serialNumber",
        "registrationDate",
        "lastUpdateDate",
        "shareWithResearchAsOfDate",
        "shareWithPublicAsOfDate",
        "shareWithFriendsAsOfDate",
    ],
    transformation_ctx="dropfields_node1706134231903",
)

# Script generated for node accelerometer trusted
accelerometertrusted_node1706131141174 = glueContext.write_dynamic_frame.from_options(
    frame=dropfields_node1706134231903,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://abreu-stedi/accelerometer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="accelerometertrusted_node1706131141174",
)

job.commit()
