from pyspark.sql import functions as F
from transforms.api import transform_df, Input, Output
from pyspark.sql.functions import col, to_date, dayofweek


@transform_df(
    Output("ri.foundry.main.dataset.68d6dc4a-705d-4fcd-97cb-9bdbebe6384d"),
    historic_monthly_df=Input("ri.foundry.main.dataset.e1b9d146-cf37-436e-b55b-d7bac3d96a01"),
    nominative_monthly=Input("ri.foundry.main.dataset.26c1c4e9-6594-4001-8451-9dbc41aee364")
)

def compute(nominative_monthly, historic_monthly_df):
    historic_monthly_df = historic_monthly_df.drop_duplicates()
    merged_df = nominative_monthly.unionByName(historic_monthly_df)
    merged_df = merged_df.withColumn("Effective_Date", to_date(col("Effective_Date")))

    latest_date = merged_df.agg(F.max("Effective_Date")).collect()[0][0]  # Compute the max date
    is_end_of_month = F.expr("last_day(`Effective_Date`) = `Effective_Date`")  # EOM check
    is_latest_date = F.col("Effective_Date") == F.lit(latest_date)
    output_df = merged_df.filter(is_end_of_month | is_latest_date).drop_duplicates()  # Filter rows
    return output_df
