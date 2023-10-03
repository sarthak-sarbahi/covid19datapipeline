from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("ProcessingData").getOrCreate()

INPUT_FILE = "gs://covid19data_ss/covidtrackingdata.txt"
OUTPUT_FILE = "gs://covid19data_ss/covidtrackingdata.json"

# Read data
rdd = spark.sparkContext.textFile(INPUT_FILE)

def replace_content(line):
    line = line.replace("'", '"')  # Convert single quotes to double quotes
    line = line.replace(": None", ": null")  # Convert None to null
    return line

processed_rdd = rdd.map(replace_content)
df = spark.read.json(processed_rdd)
df.show(1, truncate = False)
df.write.json(OUTPUT_FILE)

###

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

schema = StructType([
    StructField("date", StringType(), True),
    StructField("states", IntegerType(), True),
    StructField("cases", StructType([
        StructField("total", StructType([
            StructField("value", IntegerType(), True),
            StructField("calculated", StructType([
                StructField("population_percent", DoubleType(), True),
                StructField("change_from_prior_day", IntegerType(), True),
                StructField("seven_day_change_percent", DoubleType(), True)
            ]))
        ]))
    ])),
    StructField("testing", StructType([
        StructField("total", StructType([
            StructField("value", IntegerType(), True),
            StructField("calculated", StructType([
                StructField("population_percent", DoubleType(), True),
                StructField("change_from_prior_day", IntegerType(), True),
                StructField("seven_day_change_percent", DoubleType(), True)
            ]))
        ]))
    ])),
    StructField("outcomes", StructType([
        StructField("hospitalized", StructType([
            StructField("currently", StructType([
                StructField("value", IntegerType(), True),
                StructField("calculated", StructType([
                    StructField("population_percent", DoubleType(), True),
                    StructField("change_from_prior_day", IntegerType(), True),
                    StructField("seven_day_change_percent", DoubleType(), True),
                    StructField("seven_day_average", IntegerType(), True)
                ]))
            ])),
            StructField("in_icu", StructType([
                StructField("currently", StructType([
                    StructField("value", IntegerType(), True),
                    StructField("calculated", StructType([
                        StructField("population_percent", DoubleType(), True),
                        StructField("change_from_prior_day", IntegerType(), True),
                        StructField("seven_day_change_percent", DoubleType(), True),
                        StructField("seven_day_average", IntegerType(), True)
                    ]))
                ]))
            ])),
            StructField("on_ventilator", StructType([
                StructField("currently", StructType([
                    StructField("value", IntegerType(), True),
                    StructField("calculated", StructType([
                        StructField("population_percent", DoubleType(), True),
                        StructField("change_from_prior_day", IntegerType(), True),
                        StructField("seven_day_change_percent", DoubleType(), True),
                        StructField("seven_day_average", IntegerType(), True)
                    ]))
                ]))
            ]))
        ])),
        StructField("death", StructType([
            StructField("total", StructType([
                StructField("value", IntegerType(), True),
                StructField("calculated", StructType([
                    StructField("population_percent", DoubleType(), True),
                    StructField("change_from_prior_day", IntegerType(), True),
                    StructField("seven_day_change_percent", DoubleType(), True),
                    StructField("seven_day_average", IntegerType(), True)
                ]))
            ]))
        ]))
    ]))
])

df = sqlContext.createDataFrame(processed_rdd, schema=schema)
df = spark.read.json(processed_rdd,schema = schema)