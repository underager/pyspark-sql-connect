from pyspark.sql import SparkSession
from datetime import datetime, date
import pandas as pd
from pyspark.sql import Row


spark = SparkSession.builder\
        .appName('Read from SQL Server')\
        .config("spark.jars", "C:\JDBC\sqljdbc_4.2\enu\jre8\sqljdbc42.jar")\
        .getOrCreate()

# Define JDBC URL
jdbc_url = "jdbc:sqlserver://localhost:1433;databaseName=DockerDB"

connection_properties = {
    "user": "sa",
    "password": "DockerMSSql@123",
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

query = "(SELECT * FROM Cars) AS data"

df = spark.read.jdbc(url=jdbc_url, table=query, properties=connection_properties)

df.show()
