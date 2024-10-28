import requests

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import explode, split, current_date


spark = SparkSession.builder.appName("User Data Processing").getOrCreate()


def fetch_data(url: str):

    response = requests.get(url)
    return response.json()


def create_custom_schema() -> StructType:
    data_schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("email", StringType(), True),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("avatar", StringType(), True)
    ])

    custom_schema = StructType([
        StructField("page", IntegerType(), True),
        StructField("per_page", IntegerType(), True),
        StructField("total", IntegerType(), True),
        StructField("total_pages", IntegerType(), True),
        StructField("data", ArrayType(data_schema), True),
        StructField("support", MapType(StringType(), StringType()), True)
    ])

    return custom_schema


def transform_data(json_data, custom_schema):

    df = spark.createDataFrame([json_data], custom_schema)
    df = df.drop('page', 'per_page', 'total', 'total_pages', 'support')
    df = df.withColumn('data', explode('data'))
    df = df.withColumn("id", df.data.id) \
        .withColumn('email', df.data.email) \
        .withColumn('first_name', df.data.first_name) \
        .withColumn('last_name', df.data.last_name) \
        .withColumn('avatar', df.data.avatar) \
        .drop(df.data)
    return df


def add_derived_columns(df):

    df = df.withColumn("site_address", split(df["email"], "@")[1])
    return df.withColumn('load_date', current_date())


def save_data(df, path: str):

    df.write.format('delta').mode('overwrite').save(path)


def load_data(path: str):

    return spark.read.format('delta').load(path)



url = 'https://reqres.in/api/users?page=2'
json_data = fetch_data(url)
schema = create_custom_schema()
transformed_df = transform_data(json_data, schema)
final_df = add_derived_columns(transformed_df)


data_path = 'dbfs:/FileStore/assignments/question2/site_info/person_info'
save_data(final_df, data_path)
testing_df = load_data(data_path)


display(testing_df)
