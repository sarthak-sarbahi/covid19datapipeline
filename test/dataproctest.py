from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, ArrayType
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("ProcessingData").getOrCreate()
INPUT_FILE = "gs://covid19data_ss/covidtrackingdata.json"
PROJECT_ID = "sunlit-vortex-394519"
DATASET_ID = "covid19data"
TABLE_ID = "covid19table"

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

df = spark.read.option("multiline","true").json(INPUT_FILE,schema = schema)
transform_df = (
    df
    .select("*")
    
    .withColumn("cases_total_value",col("cases.total.value"))
    .withColumn("cases_total_calculated_population_percent",col("cases.total.calculated.population_percent"))
    .withColumn("cases_total_calculated_change_from_prior_day",col("cases.total.calculated.change_from_prior_day"))
    .withColumn("cases_total_calculated_seven_day_change_percent",col("cases.total.calculated.seven_day_change_percent"))
    .drop("cases")
    
    .withColumn("testing_total_value",col("testing.total.value"))
    .withColumn("testing_total_calculated_population_percent",col("testing.total.calculated.population_percent"))
    .withColumn("testing_total_calculated_change_from_prior_day",col("testing.total.calculated.change_from_prior_day"))
    .withColumn("testing_total_calculated_seven_day_change_percent",col("testing.total.calculated.seven_day_change_percent"))
    .drop("testing")
    
    .withColumn("outcomes_hospitalized_currently_value",col("outcomes.hospitalized.currently.value"))
    .withColumn("outcomes_hospitalized_currently_calculated_population_percent",col("outcomes.hospitalized.currently.calculated.population_percent"))
    .withColumn("outcomes_hospitalized_currently_calculated_change_from_prior_day",col("outcomes.hospitalized.currently.calculated.change_from_prior_day"))
    .withColumn("outcomes_hospitalized_currently_calculated_seven_day_change_percent",col("outcomes.hospitalized.currently.calculated.seven_day_change_percent"))
    .withColumn("outcomes_hospitalized_currently_calculated_seven_day_average",col("outcomes.hospitalized.currently.calculated.seven_day_average"))
    
    .withColumn("outcomes_hospitalized_in_icu_currently_value",col("outcomes.hospitalized.in_icu.currently.value"))
    .withColumn("outcomes_hospitalized_in_icu_currently_calculated_population_percent",col("outcomes.hospitalized.in_icu.currently.calculated.population_percent"))
    .withColumn("outcomes_hospitalized_in_icu_currently_calculated_change_from_prior_day",col("outcomes.hospitalized.in_icu.currently.calculated.change_from_prior_day"))
    .withColumn("outcomes_hospitalized_in_icu_currently_calculated_seven_day_change_percent",col("outcomes.hospitalized.in_icu.currently.calculated.seven_day_change_percent"))
    .withColumn("outcomes_hospitalized_in_icu_currently_calculated_seven_day_average",col("outcomes.hospitalized.in_icu.currently.calculated.seven_day_average"))

    .withColumn("outcomes_hospitalized_on_ventilator_currently_value",col("outcomes.hospitalized.on_ventilator.currently.value"))
    .withColumn("outcomes_hospitalized_on_ventilator_currently_calculated_population_percent",col("outcomes.hospitalized.on_ventilator.currently.calculated.population_percent"))
    .withColumn("outcomes_hospitalized_on_ventilator_currently_calculated_change_from_prior_day",col("outcomes.hospitalized.on_ventilator.currently.calculated.change_from_prior_day"))
    .withColumn("outcomes_hospitalized_on_ventilator_currently_calculated_seven_day_change_percent",col("outcomes.hospitalized.on_ventilator.currently.calculated.seven_day_change_percent"))
    .withColumn("outcomes_hospitalized_on_ventilator_currently_calculated_seven_day_average",col("outcomes.hospitalized.on_ventilator.currently.calculated.seven_day_average"))

    .withColumn("outcomes_death_total_value",col("outcomes.death.total.value"))
    .withColumn("outcomes_death_total_calculated_population_percent",col("outcomes.death.total.calculated.population_percent"))
    .withColumn("outcomes_death_total_calculated_change_from_prior_day",col("outcomes.death.total.calculated.change_from_prior_day"))
    .withColumn("outcomes_death_total_calculated_seven_day_change_percent",col("outcomes.death.total.calculated.seven_day_change_percent"))
    .withColumn("outcomes_death_total_calculated_seven_day_average",col("outcomes.death.total.calculated.seven_day_average")) 
    .drop("outcomes")   
)

transform_df.printSchema()
transform_df.show(1,truncate = False)

# write to BigQuery
(
    transform_df
    .write
    .format("bigquery")
    .option("temporaryGcsBucket","covid19data_ss_bq_write")
    .option("table", f"{PROJECT_ID}:{DATASET_ID}.{TABLE_ID}")
    .mode('overwrite')
    .save()
)
print("data written to bigquery!")

# gs://covid19data_ss/scripts/dataproctest.py