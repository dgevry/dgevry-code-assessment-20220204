"""
# Title : Code Assessment
# Description : Code Asssement for JP Morgan Chase Data Engineer position
# Author : David Gevry
# Date : 2022-02-03
# Version : 1.0
"""

import textwrap
import sys
import logging
import os
import argparse
import tempfile
import shutil
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, datediff, date_format, max as spark_max
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StringType, IntegerType, DateType

# Logging configuration
log_formatter = logging.Formatter('[%(asctime)s] %(levelname)s @ line %(lineno)d: %(message)s')
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
handler.setFormatter(log_formatter)
logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.addHandler(handler)

def parse_input():
    """
    Parse script arguments
    """
    parser =  argparse.ArgumentParser(
        prog=sys.argv[0],
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=textwrap.dedent('''\
example:
    # Pass current and history datasets from csv files and produce the results to a csv file
    python3 ./creator.py -ds1 dataset1.csv -ds2 dataset2.csv -r results.csv

    # Pass current and history datasets from csv files and produce the results to a csv file
    # and show the resulting dataset truncated
    python3 ./creator.py -ds1 dataset1.csv -ds2 dataset2.csv -r results.csv --show Yes

    # dataset1 Current dataset file format
    # delimiter: ','
    # headers: Primary_key,Attribute_a,Attribute_B,Attribute_C,Current_date
    # Primary_key: dataType is assumed to be only integer values and that in the current 
    #   dataset these values are unique by value
    # Attribute_a,Attribute_B,Attribute_C: dataType is assumed to be a string of the values 
    #   in ['Yes', 'No'] only
    # Current_date: dataType is assumed to be provided as a string in the format MM/DD/YYYY

    # dataset2 History dataset File format
    # delimiter: ','
    # headers: Primary_key,Attribute_a,Attribute_B,Attribute_C,Cob_date
    # Primary_key: dataType is assumed to be only integer values and that in the history dataset
    #   these values are unique by value and Cob_date
    # Attribute_a,Attribute_B,Attribute_C: dataType is assumed to be a string of the values in 
    #   ['Yes', 'No'] only
    # Cob_date: dataType is assumed to be provided as a string in the format MM/DD/YYYY

    # result export file format
    # delimiter: ','
    # headers: 
    # Primary_key,Attribute_a,Attribute_B,Attribute_C,Current_date,
    # Days_since_attribute_a,Days_since_attribute_b,Days_since_attribute_c
    ''')
    )

    parser.add_argument("-ds1","--dataset1_path", help = "Current Dataset input csv file path",
        required=True)
    parser.add_argument("-ds2","--dataset2_path", help = "History Dataset input csv file path",
        required=True)
    parser.add_argument("-r","--result_path", help="Result Dataset export csv file path",
        required=True)
    parser.add_argument("-s","--show", help="Print final dataset to screen with option to truncate",choices = ["No", "Yes", "Full"],
        default="No", required=False)
    args = parser.parse_args()
    return args

def main(args):
    """
    Process the provided files into current and history dataset
    Determines the date difference from the current date by Primary_key values in dataset1
    to the historical values in dataset2 and then groups these based on Primary_key to return the
    last time each attribute was true or Yes with the days since attribute results.
    """
    try:
        spark = SparkSession.builder.appName("DataSetCompare").getOrCreate()
    except Exception as err: # pylint: disable=broad-except
        logger.exception("spark session failure - %s", err)
        sys.exit()
    # Setup schemas for data set import and imports sets into dataframes
    dataset1_schema = StructType()
    dataset1_schema.add('Primary_key', IntegerType(), True)
    dataset1_schema.add('Attribute_a', StringType(), True)
    dataset1_schema.add('Attribute_B', StringType(), True)
    dataset1_schema.add('Attribute_C', StringType(), True)
    dataset1_schema.add('Current_date', DateType(), True)

    dataset2_schema = StructType()
    dataset2_schema.add('Primary_key', IntegerType(), True)
    dataset2_schema.add('Attribute_a', StringType(), True)
    dataset2_schema.add('Attribute_B', StringType(), True)
    dataset2_schema.add('Attribute_C', StringType(), True)
    dataset2_schema.add('Cob_date', DateType(), True)

    dataset1 = get_dataset(args["dataset1_path"], dataset1_schema, spark)
    dataset2 = get_dataset(args["dataset2_path"], dataset2_schema, spark)

    # Join the current dataset to the history dataset
    # Set columns with the date difference for the Current_date to Cob_date
    # for attribute with a Yes and equal dates for No
    # Group by the key and take the max date diff result for each key and attribute

    dataset_grouped = (
        dataset1
        .join(dataset2, dataset1.Primary_key==dataset2.Primary_key, how='inner')
        .withColumn(
            "a1",
            datediff(
                dataset1.Current_date,
                when(dataset2.Attribute_a=="Yes",
                dataset2.Cob_date).otherwise(dataset1.Current_date))
        )
        .withColumn(
            "b1",
            datediff(
                dataset1.Current_date,
                when(dataset2.Attribute_B=="Yes",
                dataset2.Cob_date).otherwise(dataset1.Current_date))
        )
        .withColumn(
            "c1",
            datediff(
                dataset1.Current_date,
                when(dataset2.Attribute_C=="Yes",
                dataset2.Cob_date).otherwise(dataset1.Current_date))
        )
        .groupBy(dataset1.Primary_key)
        .agg(
            spark_max(col("a1")),
            spark_max(col("b1")),
            spark_max(col("c1"))
        )
        .withColumnRenamed('max(a1)','Days_since_attribute_a')
        .withColumnRenamed('max(b1)','Days_since_attribute_b')
        .withColumnRenamed('max(c1)','Days_since_attribute_c')
        .select(
            "Primary_key",
            "Days_since_attribute_a",
            "Days_since_attribute_b",
            "Days_since_attribute_c"
        )
        .orderBy("Primary_key")
    )

    # Combines final result set into dataframe
    results=dataset1.alias("dataset1").join(dataset_grouped, dataset1.Primary_key == \
        dataset_grouped.Primary_key) \
    .withColumn("Current_date",date_format(dataset1.Current_date,"MM/dd/yyyy")) \
    .select("dataset1.Primary_key","Attribute_a","Attribute_B","Attribute_C","Current_date", \
        "Days_since_attribute_a","Days_since_attribute_b","Days_since_attribute_c") \
    .orderBy("dataset1.Primary_key")

    # shows the final result set on the screen based on selected option
    if args["show"] != "No":
        trunc =  args["show"] == "Yes"
        results.show(truncate=trunc)

    # Exports results to csv file based on result_path option
    with tempfile.TemporaryDirectory(suffix="tmp") as tmp_dir:
        temp_file = os.path.join(tmp_dir,"temp_file")
        results.coalesce(1).write\
        .options(header='True', dateFormat="MM/dd/yyyy")\
        .mode("overwrite").csv(temp_file)
        csv_file = [x for x in os.listdir(temp_file) if x.endswith(".csv")][0]
        shutil.move(os.path.join(temp_file,csv_file), args["result_path"])

def get_dataset(dataset_path, schema, spark):
    """
    Returns a dataframe of the provided csv file path with the provided schema
    """
    try:
        return spark.read.options(header='True',dateFormat="MM/dd/yyyy")\
        .schema(schema).csv(dataset_path)
    except Exception as err: # pylint: disable=broad-except
        logger.exception("dataset file failed to load - %s - %s", dataset_path, err)
        sys.exit()

def validate_args(args):
    """
    Validates the input arguments to check file paths exists
    """
    call_error_found = False
    if args.dataset1_path is None or not os.path.exists(args.dataset1_path):
        logger.info("dataset1_path is required to exist")
        call_error_found =  True
    if args.dataset2_path is None or not os.path.exists(args.dataset2_path):
        logger.info("dataset2_path is required to exist")
        call_error_found =  True
    if call_error_found:
        sys.exit(1)
    return {
            'dataset1_path': args.dataset1_path,
            'dataset2_path': args.dataset2_path,
            'result_path': args.result_path,
            'show': args.show
        }

if __name__ == '__main__':
    main(validate_args(parse_input()))
    sys.exit()
