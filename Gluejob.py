import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
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

# Script generated for node Users Data Source
UsersDataSource_node1709137617650 = glueContext.create_dynamic_frame.from_options(
    format_options={
        "quoteChar": '"',
        "withHeader": False,
        "separator": "|",
        "optimizePerformance": False,
    },
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": [
            "s3://labstack-502883f7-b4ce-49fe-bc70-c4f0be-databucket-3q1vijyn877r/tickitdb/allusers_pipe.txt"
        ],
        "recurse": True,
    },
    transformation_ctx="UsersDataSource_node1709137617650",
)

# Script generated for node Events Data Source
EventsDataSource_node1709137729589 = glueContext.create_dynamic_frame.from_options(
    format_options={
        "quoteChar": '"',
        "withHeader": False,
        "separator": "|",
        "optimizePerformance": False,
    },
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": [
            "s3://labstack-502883f7-b4ce-49fe-bc70-c4f0be-databucket-3q1vijyn877r/tickitdb/allevents_pipe.txt"
        ],
        "recurse": True,
    },
    transformation_ctx="EventsDataSource_node1709137729589",
)

# Script generated for node Sales Data Source
SalesDataSource_node1709137689084 = glueContext.create_dynamic_frame.from_options(
    format_options={
        "quoteChar": '"',
        "withHeader": False,
        "separator": "\t",
        "optimizePerformance": False,
    },
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": [
            "s3://labstack-502883f7-b4ce-49fe-bc70-c4f0be-databucket-3q1vijyn877r/tickitdb/sales_tab.txt"
        ],
        "recurse": True,
    },
    transformation_ctx="SalesDataSource_node1709137689084",
)

# Script generated for node Category Data Source
CategoryDataSource_node1709137752661 = glueContext.create_dynamic_frame.from_options(
    format_options={
        "quoteChar": '"',
        "withHeader": False,
        "separator": "|",
        "optimizePerformance": False,
    },
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": [
            "s3://labstack-502883f7-b4ce-49fe-bc70-c4f0be-databucket-3q1vijyn877r/tickitdb/category_pipe.txt"
        ],
        "recurse": True,
    },
    transformation_ctx="CategoryDataSource_node1709137752661",
)

# Script generated for node Users Data Schema
UsersDataSchema_node1709137674128 = ApplyMapping.apply(
    frame=UsersDataSource_node1709137617650,
    mappings=[
        ("col0", "string", "userid", "int"),
        ("col1", "string", "username", "char"),
        ("col2", "string", "firstname", "varchar"),
        ("col3", "string", "lastname", "varchar"),
        ("col4", "string", "city", "varchar"),
        ("col5", "string", "state", "char"),
        ("col6", "string", "email", "varchar"),
        ("col7", "string", "phone", "char"),
        ("col8", "string", "likesports", "boolean"),
        ("col9", "string", "liketheatre", "boolean"),
        ("col10", "string", "likeconcerts", "boolean"),
        ("col11", "string", "likejazz", "boolean"),
        ("col12", "string", "likeclassical", "boolean"),
        ("col13", "string", "likeopera", "boolean"),
        ("col14", "string", "likerock", "boolean"),
        ("col15", "string", "likevegas", "boolean"),
        ("col16", "string", "likebroadway", "boolean"),
        ("col17", "string", "likemusicals", "boolean"),
    ],
    transformation_ctx="UsersDataSchema_node1709137674128",
)

# Script generated for node Events Data Schema
EventsDataSchema_node1709137779622 = ApplyMapping.apply(
    frame=EventsDataSource_node1709137729589,
    mappings=[
        ("col0", "string", "eventid_right", "int"),
        ("col1", "string", "venueid", "smallint"),
        ("col2", "string", "catid", "smallint"),
        ("col3", "string", "dateid_right", "smallint"),
        ("col4", "string", "eventname", "varchar"),
        ("col5", "string", "starttime", "timestamp"),
    ],
    transformation_ctx="EventsDataSchema_node1709137779622",
)

# Script generated for node Sales Data Schema
SalesDataSchema_node1709137719094 = ApplyMapping.apply(
    frame=SalesDataSource_node1709137689084,
    mappings=[
        ("col0", "string", "salesid", "int"),
        ("col1", "string", "listid", "int"),
        ("col2", "string", "sellerid", "int"),
        ("col3", "string", "buyerid", "int"),
        ("col4", "string", "eventid", "int"),
        ("col5", "string", "dateid", "smallint"),
        ("col6", "string", "qtysold", "smallint"),
        ("col7", "string", "pricepaid", "decimal"),
        ("col8", "string", "commission", "decimal"),
        ("col9", "string", "saletime", "timestamp"),
    ],
    transformation_ctx="SalesDataSchema_node1709137719094",
)

# Script generated for node Category Data Schema
CategoryDataSchema_node1709137792327 = ApplyMapping.apply(
    frame=CategoryDataSource_node1709137752661,
    mappings=[
        ("col0", "string", "catid_right", "smallint"),
        ("col1", "string", "catgroup", "varchar"),
        ("col2", "string", "catname", "varchar"),
        ("col3", "string", "catdesc", "varchar"),
    ],
    transformation_ctx="CategoryDataSchema_node1709137792327",
)

# Script generated for node Users and Sales Join
UsersDataSchema_node1709137674128DF = UsersDataSchema_node1709137674128.toDF()
SalesDataSchema_node1709137719094DF = SalesDataSchema_node1709137719094.toDF()
UsersandSalesJoin_node1709138221676 = DynamicFrame.fromDF(
    UsersDataSchema_node1709137674128DF.join(
        SalesDataSchema_node1709137719094DF,
        (
            UsersDataSchema_node1709137674128DF["userid"]
            == SalesDataSchema_node1709137719094DF["buyerid"]
        ),
        "left",
    ),
    glueContext,
    "UsersandSalesJoin_node1709138221676",
)

# Script generated for node Events and Category Join
EventsDataSchema_node1709137779622DF = EventsDataSchema_node1709137779622.toDF()
CategoryDataSchema_node1709137792327DF = CategoryDataSchema_node1709137792327.toDF()
EventsandCategoryJoin_node1709138246217 = DynamicFrame.fromDF(
    EventsDataSchema_node1709137779622DF.join(
        CategoryDataSchema_node1709137792327DF,
        (
            EventsDataSchema_node1709137779622DF["catid"]
            == CategoryDataSchema_node1709137792327DF["catid_right"]
        ),
        "left",
    ),
    glueContext,
    "EventsandCategoryJoin_node1709138246217",
)

# Script generated for node Final Join
UsersandSalesJoin_node1709138221676DF = UsersandSalesJoin_node1709138221676.toDF()
EventsandCategoryJoin_node1709138246217DF = (
    EventsandCategoryJoin_node1709138246217.toDF()
)
FinalJoin_node1709138267759 = DynamicFrame.fromDF(
    UsersandSalesJoin_node1709138221676DF.join(
        EventsandCategoryJoin_node1709138246217DF,
        (
            UsersandSalesJoin_node1709138221676DF["eventid"]
            == EventsandCategoryJoin_node1709138246217DF["eventid_right"]
        ),
        "left",
    ),
    glueContext,
    "FinalJoin_node1709138267759",
)

# Script generated for node Drop Columns
DropColumns_node1709138297636 = ApplyMapping.apply(
    frame=FinalJoin_node1709138267759,
    mappings=[
        ("userid", "int", "userid", "int"),
        ("city", "varchar", "city", "varchar"),
        ("state", "char", "state", "char"),
        ("likesports", "boolean", "likesports", "boolean"),
        ("liketheatre", "boolean", "liketheatre", "boolean"),
        ("likeconcerts", "boolean", "likeconcerts", "boolean"),
        ("likejazz", "boolean", "likejazz", "boolean"),
        ("likeclassical", "boolean", "likeclassical", "boolean"),
        ("likeopera", "boolean", "likeopera", "boolean"),
        ("likerock", "boolean", "likerock", "boolean"),
        ("likevegas", "boolean", "likevegas", "boolean"),
        ("likebroadway", "boolean", "likebroadway", "boolean"),
        ("likemusicals", "boolean", "likemusicals", "boolean"),
        ("qtysold", "smallint", "qtysold", "smallint"),
        ("pricepaid", "decimal", "pricepaid", "decimal"),
        ("commission", "decimal", "commission", "decimal"),
        ("catid_right", "smallint", "catid_right", "smallint"),
        ("catgroup", "varchar", "catgroup", "varchar"),
        ("catname", "varchar", "catname", "varchar"),
        ("catdesc", "varchar", "catdesc", "varchar"),
    ],
    transformation_ctx="DropColumns_node1709138297636",
)

# Script generated for node SQL Query
SqlQuery0 = """
select userid,
    city,
    state,
    likesports,
    liketheatre,
    likeconcerts,
    likejazz,
    likeclassical,
    likeopera,
    likerock,
    likevegas,
    likebroadway,
    likemusicals,
    count(case when catname = 'Musicals' then 1 end) as musicals,
    count(case when catname = 'Plays' then 1 end) as plays,
    count(case when catname = 'Opera' then 1 end) as opera,
    count(case when catname = 'Pop' then 1 end) as pop,
    coalesce(sum(qtysold), 0) as tickets_sold,
    coalesce(avg(pricepaid / qtysold), 0) as average_price_paid_per_ticket,
    coalesce(avg(commission / qtysold), 0) as average_commission_per_ticket
from myDataSource
group by userid,
    city,
    state,
    likesports,
    liketheatre,
    likeconcerts,
    likejazz,
    likeclassical,
    likeopera,
    likerock,
    likevegas,
    likebroadway,
    likemusicals
"""
SQLQuery_node1709147949030 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={"myDataSource": DropColumns_node1709138297636},
    transformation_ctx="SQLQuery_node1709147949030",
)

# Script generated for node Amazon Redshift
AmazonRedshift_node1709138488818 = glueContext.write_dynamic_frame.from_options(
    frame=SQLQuery_node1709147949030,
    connection_type="redshift",
    connection_options={
        "redshiftTmpDir": "s3://aws-glue-assets-456938221590-us-west-2-138089968/temporary/",
        "useConnectionProperties": "true",
        "dbtable": "public.users_sales",
        "connectionName": "GlueRedshiftConnection",
        "preactions": "CREATE TABLE IF NOT EXISTS public.users_sales (userid INTEGER, city VARCHAR, state VARCHAR, likesports BOOLEAN, liketheatre BOOLEAN, likeconcerts BOOLEAN, likejazz BOOLEAN, likeclassical BOOLEAN, likeopera BOOLEAN, likerock BOOLEAN, likevegas BOOLEAN, likebroadway BOOLEAN, likemusicals BOOLEAN, musicals INTEGER, plays INTEGER, opera INTEGER, pop INTEGER, tickets_sold INTEGER, average_price_paid_per_ticket DECIMAL, average_commission_per_ticket DECIMAL); TRUNCATE TABLE public.users_sales;",
    },
    transformation_ctx="AmazonRedshift_node1709138488818",
)

job.commit()
