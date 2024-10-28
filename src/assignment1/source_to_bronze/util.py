from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date, col
from pyspark.sql.types import StructType


spark = SparkSession.builder.appName("CSV Processing").getOrCreate()

def read_csv(path: str):

    return spark.read.csv(path, header=True, inferSchema=True)

def write_csv(df, path: str):

    df.write.format('csv').save(path)

def read_with_custom_schema(data: str, schema: StructType):

    return spark.read.csv(data, schema=schema)

def read_with_custom_schema_format(data: str, schema: StructType):

    return spark.read.format('csv').schema(schema).load(data)

def camel_to_snake_case(df):

    for col_name in df.columns:
        new_col_name = col_name.replace(" ", "_").lower()
        df = df.withColumnRenamed(col_name, new_col_name)
    return df

def add_current_date(df):

    return df.withColumn("load_date", current_date())
