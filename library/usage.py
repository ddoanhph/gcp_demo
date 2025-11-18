from pyspark.sql import functions as F
from pyspark.sql import types as T
from transforms.api import transform, Input, Output, incremental
from pyspark.sql.types import IntegerType, StringType

from data_archiving_library.data_archiving import standardArchive
from data_archiving_library.clean_start import cleanStart

@incremental(
    require_incremental=False,
    semantic_version=2,
    snapshot_inputs=["in_obj"],
    allow_retention=False,
    strict_append=False,
)
@transform(
    in_obj=Input("ri.foundry.main.dataset.feec4914-a895-4591-bc66-7b891449ebac"),
    out_obj=Output(
        "/Shared/Airbus Group - HR (North America)/HRNA Data/Data Products/Employment/Historic_Worker_Core_Info_NA"
    ),
) 
def compute(in_obj, out_obj):
    # IMPORTANT WARNING: THIS SHOULD (almost) ALWAYS BE FALSE. ONLY CHANGE TO "TRUE" TO DELETE ALL
    # PREVIOUS ARCHIVE DATA AND START FRESH. REMEMBER TO CHANGE BACK TO FALSE AFTER STARTING A CLEAN RUN!
    cleanStartFlag = False

    # Filter by location
    # Track siglum, supervisory_organization, worker_type, woker_status
    current_df = in_obj.dataframe()
    current_df = current_df.drop("Job_Code_Canada")
    current_df = current_df.withColumn('Is_Manager', current_df['Is_Manager'].cast(StringType()))
    current_df = current_df.withColumn('Full_Time_Equivalent_Percentage', 
                                   current_df['Full_Time_Equivalent_Percentage'].cast(IntegerType()).cast(StringType()))

    idColumns = ["Corporate_ID"]
    
    all_input_columns = current_df.columns
    columnsToTrack = [col for col in all_input_columns if col not in idColumns]

    if cleanStartFlag:
        df = cleanStart(current_df, idColumns, columnsToTrack)
        df.show(5) 
    else:
        output_schema = current_df.schema
        all_columns_for_archive = all_input_columns

        df = standardArchive(
            current_df,
            out_obj,
            output_schema,
            all_columns_for_archive,
            idColumns,
            columnsToTrack
        )

        out_obj.set_mode("replace")
        out_obj.write_dataframe(df)
