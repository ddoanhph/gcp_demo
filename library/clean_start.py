import logging
from pyspark.sql import functions as F
from datetime import datetime, timedelta
from transforms.api import transform, Input, Output, incremental

def cleanStart(current_df, idColumns, columnsToTrack):
    logging.warning("RUNNING CLEAN START")
    logging.warning("version 5.4")
    current_date = datetime.today().date()

    current_df = current_df.withColumn(
        "idHash", F.concat_ws("_!#!_", *idColumns)
    )  # absurd seperator that will "never" occur naturally in the data
    current_df = current_df.withColumn(
        "trackingHash", F.concat_ws("_!#!_", *columnsToTrack)
    )
    current_df = current_df.withColumn("valid_from", F.lit(current_date))  # dummy
    current_df = current_df.withColumn("valid_to", F.lit(current_date))  # dummy
    current_df = current_df.withColumn("current_version", F.lit(True))  # dummy

    current_df = current_df.toDF(*(c.replace("crnt_", "") for c in current_df.columns))

    return current_df
