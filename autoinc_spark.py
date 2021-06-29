from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import getpass
username = getpass.getuser()


spark = SparkSession.builder.config('spark.ui.port', '0').master("yarn").appName("test_CarSales").getOrCreate()


df1=spark.read.csv('/user/itv000696/data.csv',schema='''
                                                incident_id INT,
                                                incident_type STRING,
                                                vin_number STRING,
                                                make STRING,
                                                model STRING,
                                                year STRING,
                                                incident_date STRING,
                                                description STRING
                                            ''')



infoDF = df1.filter('incident_type == "I"').select('incident_type','vin_number','make','year')
accidentDF = df1.filter('incident_type == "A"').select('incident_type','vin_number','make','year')
intdf=infoDF.join(accidentDF, 'vin_number').select(infoDF.make, infoDF.year)
finaldf=intdf.groupBy("make","year").count()
finaldf.write.format("com.databricks.spark.csv").save("/user/itv000696/output.csv")
