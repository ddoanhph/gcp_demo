# NOTE:
# 1. Process to update library:
#     a. Make changes to the code here
#     b. Commit the changes to the master branch
#     c. Open the Branches tab seen at the top of the page
#     d. Click on Tags
#     e. Click on New Tag
#     f. Select minor/major change (it has some prompts to help understand the difference)
#     g. Go to the code that is using this library, and make sure it is updated to the latest library version
#     h. IMPORTANT: Note, despite updating the library, and verifying that the consumer code is using the latest
#                   version, I have seen that sometimes the actual version used is lagging behind. This can
#                   be verified by adding a print statement to the code below which shows the version, so that
#                   you can see the print statement on the consumer end to make sure the correct code is being
#                   used (often times this is not the case, and apart from waiting, I don't know how to solve this).
#                   For example, add "print (version 5)" when you shift to version 5, and if the consumer code
#                   is still showing an older print statement, you know that the correct code is not being used.
#     i. As a workaround (when debugging/developing), just copy the function to the consumer code and use it from
#        there instead of importing. Once you fix the function there, copy it back here to update the library.

# 2. WARNING: Debugging this code is particularly tricky! This is because of the peculiarities of pyspark and the incremental
#    dataset reading. Basically, this is what happens:
#     a. Simply doing df.show() on the dataset being read incrementally shows an empty dataset (even if it is not empty)
#     b. Important! Additionally, whenever the code is "previewed" it uses the same logic as above and uses an empty dataset for the
#        dataset that we are trying to read incrementally. This is extremely confusing because nothing seems logically wrong
#        with the code, and no warnings/errors are shown, yet the output seems inexplicably wrong.
#     c. Therefore, the only time we can actually see how the code is supposed to function is when we build it. Unfortunately,
#        I think print/show statements are tricky when building stuff (and besides sometimes you don't want the final dataset
#        to be built yet)
#     d. Therefore, the final workaround I used was to use the python logger library to log messages to the build log...


# IMPORTANT: Note, the following function assumes the correct usage of the "incremental" decorator
# on the transform function of the file calling this function. The following code snippet demonstrates
# such a use case

# @incremental(require_incremental=False, semantic_version=2, snapshot_inputs=['in_obj'], allow_retention=False, strict_append=False)
# @transform(
#     in_obj=Input("ri.foundry.main.dataset.b06dea4c-8074-41b9-9521-8a0938985962"),
#     out_obj=Output("ri.foundry.main.dataset.aa1c081d-7dd4-40bb-ac6f-cec9ba6ed6d2"),
# )
# def compute(in_obj, out_obj):
#    current_df = in_obj.dataframe()
#    idColumns = ["material", "batch", "plant", "storage_location", "[sales|ven_consi|cust_consi|special_ven]-special_stock_indicator"]
#    columnsToTrack = ["valuation_type", "unrestricted_stock", "scheduled_for_delivery", "restricted_stock"]

#    schema = Copy as is from the in_obj: go to dataset, click on columns, click on copy, pyspark schema

#    column_names = Same as above, except select "Copy names in quotes" and then put this in a list []

#    df = standardArchive(current_df, out_obj, schema, column_names, idColumns, columnsToTrack)

#    out_obj.set_mode('replace')
#    out_obj.write_dataframe(df)

import logging
from pyspark.sql import types as T
from pyspark.sql import functions as F
from datetime import datetime, timedelta
from pyspark.sql.utils import AnalysisException
from transforms.api import transform, Input, Output, incremental


def standardArchive(
    current_df, out_obj, schema, column_names, idColumns, columnsToTrack
):
    version = "5.4"
    logging.warning(f"running library version {version}")
    try:
        logging.warning("inside try block")
        out_obj_schema = schema
        out_obj_schema.add(T.StructField("idHash", T.StringType()))
        out_obj_schema.add(T.StructField("trackingHash", T.StringType()))
        out_obj_schema.add(T.StructField("valid_from", T.DateType()))
        out_obj_schema.add(T.StructField("valid_to", T.DateType()))
        out_obj_schema.add(T.StructField("current_version", T.BooleanType()))

        additionalColumns = [
            "idHash",
            "trackingHash",
            "valid_from",
            "valid_to",
            "current_version",
        ]
        out_obj_column_names = column_names + additionalColumns

        # these lines will raise an error if this is the first time the code is being run,
        # because "current_version" will not be defined
        archive_df = out_obj.dataframe(mode="previous", schema=out_obj_schema)
        archive_df_currently_valid = archive_df.filter(F.col("current_version"))
        archive_df_already_retired = archive_df.filter(~F.col("current_version"))

        # "_!#!_" is an absurd seperator that should never occur naturally in the data
        current_df = current_df.withColumn("idHash", F.concat_ws("_!#!_", *idColumns))
        current_df = current_df.withColumn(
            "trackingHash", F.concat_ws("_!#!_", *columnsToTrack)
        )

        # There are some columns that are expected in this table, but don't naturally exist
        # These are added here and dummy values are stored to them

        current_date = datetime.today().date()
        current_df = current_df.withColumn("valid_from", F.lit(current_date))  # dummy
        current_df = current_df.withColumn("valid_to", F.lit(current_date))  # dummy

        # Obviously, any record in the current df, should be assumed to be currently valid
        current_df = current_df.withColumn("current_version", F.lit(True))  # dummy

        current_df = current_df.select(
            [F.col(c).alias("crnt_" + c) for c in current_df.columns]
        )

        # outputString = "!!!!!!!" + str(archive_df.head(10)) + "!!!!!!!" + str(archive_df_currently_valid.head(10)) + "!!!!!!!" + str(archive_df_already_retired.head(10))
        # raise ValueError(outputString)

        column_names = out_obj_column_names

        column_names_crnt = ["crnt_" + str(col_name) for col_name in column_names]

        # high_date = datetime.strptime('9999-12-31', '%Y-%m-%d').date()

        merged_df = archive_df_currently_valid.join(
            current_df, (current_df.crnt_idHash == archive_df.idHash), how="fullouter"
        )

        # Explanation of cases:

        # The full outer join leads exhaustively to the following relevant cases, which are described along with how they
        # are handled. Any cases not described here are irrelevant and their occurence shouldbe ignored (explained further
        # below).

        # case 1: Archive and Current id's match (ie, they are both not null) and archive_track = current_track. This means
        # that the archive and current hashes (both id and tracking) match exactly. Therefore, we are looking at the same
        # exact record in the current data that was described in the archives. Nothing has changed, not even the different
        # inventory quantities. All we need to do is update the validity dates for the archive.

        # case 2: Archive_ID hash is not null, but the current ID hash is null. This means that we no longer have any
        #         information about the previous record that has been described in the archive (think of the idHash as a
        #         unique name. And current_id hash being null means we can no longer find this name in the data, and so the
        #         "person" must have disappeared). The only thing to do here is to "retire" the person. Ie, we leave the
        #         valid_to datetime as is, and update the current_version to False.

        # case 3: Archive_ID is null, but current_ID hash is not null. This means that the so called "name" from the new
        #         data is not found in the old data. Think of it as a new "person" having being added to the data. So we
        #         simply add a new row, with this new data. We set valid_from and valid_to dates to the timestamp available,
        #         and set current_version to True.

        # case 4: Archive_ID is not null, and current_ID is not null, but tracking id's are not identical. This means that
        #         we found the same "person" as before, but some information about them has changed since last time. This
        #         means that all the information under the archive table is no longer valid.
        #         Two steps need to be carried out:
        #         step 4.a: We add a new row to our data, with the id_hash, and new tracking_hash, and set the valid_from
        #                   and valid_to datetime to the timestamp of this record, and current_version flag to True.
        #         step 4.b: We need to "retire" the corresponding archive row. We leave valid_to, valid_from as is, and set
        #                   "current_version" to False.

        # case 5: Both idHash and tracking_Hash are null. AFAIK this is not possible. Please investigate if this ever
        #         happens.

        # case 6: Cases 1 to 5 are exhaustive. Case 6 is just a syntactic necessity (I think I have to have an "otherwise"
        #    -    clause).

        merged_df = merged_df.withColumn(
            "action",
            F.when(
                ~merged_df.idHash.isNull()
                & ~merged_df.crnt_idHash.isNull()
                & (merged_df.trackingHash == merged_df.crnt_trackingHash),
                "CASE_1",
            )
            .when(~merged_df.idHash.isNull() & merged_df.crnt_idHash.isNull(), "CASE_2")
            .when(merged_df.idHash.isNull() & ~merged_df.crnt_idHash.isNull(), "CASE_3")
            .when(
                ~merged_df.idHash.isNull()
                & ~merged_df.crnt_idHash.isNull()
                & (merged_df.trackingHash != merged_df.crnt_trackingHash),
                "CASE_4",
            )
            .when(merged_df.idHash.isNull() & merged_df.crnt_idHash.isNull(), "CASE_5")
            .otherwise("CASE_6"),
        )
        # note: case 5, and case 6 should be impossible, unless there is some data with nulls in the id columns maybe

        # Now, we handle each case

        # testString = str(merged_df.select([c for c in merged_df.columns if c in ["action", "batch", "crnt_batch","idHash", "crnt_idHash", "trackingHash","crnt_trackingHash", "current_timestamp_column_name"]]).show(truncate = False))
        # testString = str(merged_df.select([c for c in merged_df.columns if c in ["action", "batch", "crnt_batch","idHash", "crnt_idHash", "trackingHash","crnt_trackingHash", current_timestamp_column_name]]).head(10))

        # raise ValueError(testString)

        # case 1: Update the validity date
        merged_df_CASE_1 = merged_df.filter(merged_df.action == "CASE_1")
        merged_df_CASE_1 = merged_df_CASE_1.withColumn(
            "valid_to", F.lit(current_date)
        ).select(column_names)

        # case 2: The only thing to do here is to "retire" the record by setting "current_version" to False
        merged_df_CASE_2 = merged_df.filter(merged_df.action == "CASE_2")
        merged_df_CASE_2 = merged_df_CASE_2.withColumn(
            "current_version", F.lit(False)
        ).select(column_names)

        # case 3: Set valid_from and valid_to dates to the timestamp available, and set current_version to True
        merged_df_CASE_3 = merged_df.filter(merged_df.action == "CASE_3").select(
            column_names_crnt
        )  # the original df has no data, so we select the crnt_df columns
        merged_df_CASE_3 = (
            merged_df_CASE_3.withColumn("valid_to", F.lit(current_date))
            .withColumn("valid_from", F.lit(current_date))
            .withColumn("current_version", F.lit(True))
            .drop("crnt_valid_from")
            .drop("crnt_valid_to")
            .drop("crnt_current_version")
        )
        merged_df_CASE_3 = merged_df_CASE_3.toDF(
            *(c.replace("crnt_", "") for c in merged_df_CASE_3.columns)
        )  # we rename the crnt_df columns by removing the crnt_prefix so we can merge later on
        merged_df_CASE_3 = merged_df_CASE_3.select(column_names)

        # case 4 Step a: Add a new row to our data, with the id_hash, and new tracking_hash, and set the valid_from and valid_to datetime to the timestamp of this record,
        #          and current_version flag to True
        merged_df_CASE_4_Step_a = merged_df.filter(merged_df.action == "CASE_4").select(
            column_names_crnt
        )
        merged_df_CASE_4_Step_a = (
            merged_df_CASE_4_Step_a.withColumn(
                "idHash", merged_df_CASE_4_Step_a["crnt_idHash"]
            )
            .withColumn("trackingHash", merged_df_CASE_4_Step_a["crnt_trackingHash"])
            .withColumn("valid_to", F.lit(current_date))
            .withColumn("valid_from", F.lit(current_date))
            .withColumn("current_version", F.lit(True))
            .drop("crnt_valid_from")
            .drop("crnt_valid_to")
            .drop("crnt_current_version")
            .drop("crnt_idHash")
            .drop("crnt_trackingHash")
        )
        merged_df_CASE_4_Step_a = merged_df_CASE_4_Step_a.toDF(
            *(c.replace("crnt_", "") for c in merged_df_CASE_4_Step_a.columns)
        )  # we rename the crnt_df columns by removing the crnt_prefix so we can merge later on
        merged_df_CASE_4_Step_a = merged_df_CASE_4_Step_a.select(column_names)

        # case 4 Step b: Set "current_version" to False.
        merged_df_CASE_4_Step_b = merged_df.filter(merged_df.action == "CASE_4")
        merged_df_CASE_4_Step_b = merged_df_CASE_4_Step_b.withColumn(
            "current_version", F.lit(False)
        ).select(column_names)

        merged_df_CASE_5 = merged_df.filter(merged_df.action == "CASE_5")
        row_count_CASE_5 = merged_df_CASE_5.count()
        if row_count_CASE_5 > 0:
            logging.warning("NOTE! This shouldn't happen.")
            logging.warning(f"Row count for CASE 5 is {row_count_CASE_5}")

        merged_df_CASE_6 = merged_df.filter(merged_df.action == "CASE_6")
        row_count_CASE_6 = merged_df_CASE_6.count()
        if row_count_CASE_6 > 0:
            logging.warning("NOTE! This shouldn't happen.")
            logging.warning(f"Row count for CASE 6 is {row_count_CASE_6}")

        # merged_df_CASE_7b = merged_df.filter(merged_df.action == 'CASE_7b')
        # merged_df_CASE_7b = merged_df_CASE_7b.withColumn("current_version", F.lit(False)).select(column_names)

        # Union all records together
        merged_df = (
            merged_df_CASE_1.unionAll(merged_df_CASE_2)
            .unionAll(merged_df_CASE_3)
            .unionAll(merged_df_CASE_4_Step_a)
            .unionAll(merged_df_CASE_4_Step_b)
            .unionAll(archive_df_already_retired)
        )
        logging.warning("completed try block")
        return merged_df

    except TypeError as e:
        logging.warning("inside Type error exception")
        logging.warning(f"standard archive - in except block - version {version}")
        # only in the case of a TypeError, with the error message of "col should be Column" do we know that
        # we are looking at an empty dataset, and therefore need to start with a clean run. All other errors
        # are unexpected and are therefore raised as usual.
        if str(e) == "col should be Column":
            logging.warning("inside Type error col should be Column block")
            # basically running a "clean run" if this is the first time the code is being called

            current_date = datetime.today().date()

            current_df = current_df.withColumn(
                "idHash", F.concat_ws("_!#!_", *idColumns)
            )  # absurd seperator that will "never" occur naturally in the data
            current_df = current_df.withColumn(
                "trackingHash", F.concat_ws("_!#!_", *columnsToTrack)
            )
            current_df = current_df.withColumn(
                "valid_from", F.lit(current_date)
            )  # dummy
            current_df = current_df.withColumn("valid_to", F.lit(current_date))  # dummy
            current_df = current_df.withColumn("current_version", F.lit(True))  # dummy

            current_df = current_df.toDF(
                *(c.replace("crnt_", "") for c in current_df.columns)
            )

            return current_df
        else:
            logging.warning(
                "exception, but not exact error we are looking for. Look into this!"
            )
            logging.warning(e)
            logging.warning(
                "This was an unexpected error and needs attention!! Printing error below, and error has also been raised as usual"
            )
            logging.warning(e)
            raise (e)
