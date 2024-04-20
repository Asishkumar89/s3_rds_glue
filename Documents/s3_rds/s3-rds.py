import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import gs_concat

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Amazon S3
AmazonS3_node1712830996134 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://mylistofnames/"]}, transformation_ctx="AmazonS3_node1712830996134")

# Script generated for node Change Schema
ChangeSchema_node1712831856596 = ApplyMapping.apply(frame=AmazonS3_node1712830996134, mappings=[("id", "string", "id", "int"), ("firstname", "string", "firstname", "string"), ("lastname", "string", "lastname", "string")], transformation_ctx="ChangeSchema_node1712831856596")

# Script generated for node Concatenate Columns
ConcatenateColumns_node1712894572351 = ChangeSchema_node1712831856596.gs_concat(colName="fullname", colList=["firstname", "lastname"], spacer=" ")

# Script generated for node PostgreSQL
PostgreSQL_node1712894611518 = glueContext.write_dynamic_frame.from_catalog(frame=ConcatenateColumns_node1712894572351, database="rdstarget", table_name="rdsdev_public_names", transformation_ctx="PostgreSQL_node1712894611518")

job.commit()