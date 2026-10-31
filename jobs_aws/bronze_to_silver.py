import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
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


DEFAULT_DATA_QUALITY_RULESET = """
Rules = [
    ColumnCount > 0
]
"""


AWSGlueDataCatalog_node1770243786397 = glueContext.create_dynamic_frame.from_catalog(
    database="trade_data_imat3a02",
    table_name="bronze_ada_ada",
    transformation_ctx="AWSGlueDataCatalog_node1770243786397",
)


ChangeSchema_node1770243842406 = ApplyMapping.apply(
    frame=AWSGlueDataCatalog_node1770243786397,
    mappings=[
        ("symbol", "string", "symbol", "string"),
        ("open", "double", "open", "double"),
        ("high", "double", "high", "double"),
        ("low", "double", "low", "double"),
        ("close", "double", "close", "double"),
        ("volume", "double", "volume", "double"),
        ("day", "long", "day", "long"),
        ("year", "string", "year", "int"),
        ("month", "string", "month", "int"),
    ],
    transformation_ctx="ChangeSchema_node1770243842406",
)


SqlQuery0 = """
SELECT
    to_timestamp(
        concat(
            cast(year as string), '-',
            lpad(cast(month as string), 2, '0'), '-',
            lpad(cast(day as string), 2, '0'),
            ' 00:00:00'
        )
    ) AS trade_ts,
    symbol,
    open,
    high,
    low,
    close,
    volume,
    day,
    month,
    year
FROM myDataSource
"""


SQLQuery_node1770244110477 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={"myDataSource": ChangeSchema_node1770243842406},
    transformation_ctx="SQLQuery_node1770244110477",
)


EvaluateDataQuality().process_rows(
    frame=SQLQuery_node1770244110477,
    ruleset=DEFAULT_DATA_QUALITY_RULESET,
    publishing_options={
        "dataQualityEvaluationContext": "EvaluateDataQuality_node1770242681584",
        "enableDataQualityResultsPublishing": True,
    },
    additional_options={
        "dataQualityResultsPublishing.strategy": "BEST_EFFORT",
        "observations.scope": "ALL",
    },
)


AmazonS3_node1770244827629 = glueContext.getSink(
    path="s3://adaquant-data-bucket/silver/ada/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=["year", "month"],
    enableUpdateCatalog=True,
    transformation_ctx="AmazonS3_node1770244827629",
)

AmazonS3_node1770244827629.setCatalogInfo(
    catalogDatabase="trade_data_imat3a02",
    catalogTableName="silver_ada_ada",
)

AmazonS3_node1770244827629.setFormat("glueparquet", compression="snappy")
AmazonS3_node1770244827629.writeFrame(SQLQuery_node1770244110477)

job.commit()
