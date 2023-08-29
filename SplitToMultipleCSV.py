import os
import time
from pandas import DataFrame
from pypeepa import (
    initLogging,
    createDirectory,
    getFilePath,
    listDir,
    readJSON,
    loggingHandler,
    askYNQuestion,
    processCSVInChunks,
    ProgressSaver,
)


def dropColumns(df, delete_columns):
    """Remove multiple columns from a dataframe"""
    columns_to_drop = [col for col in delete_columns if col in df.columns]
    df.drop(columns=columns_to_drop, errors="ignore")
    return df


def reorderColumns(df, ordered_columns):
    """Change the order of the columns according to a provided list of columns"""
    return df.reindex(columns=ordered_columns, fill_value="")


def removeNullFromColumn(df, check_for_null_columns):
    """Remove any null values in the column specified, if more than one column is specified then if all are null only
    remove those."""
    for col in check_for_null_columns:
        if col in df.columns:
            df = df[df[col].notnull()]
    return df


def splitByColumn(df: DataFrame, props):
    for ind_ethnic_code, group in df.groupby(props["column_to_split"]):
        ind_output_dir = os.path.join(
            props["output_dir"],
            str(ind_ethnic_code),
        )
        # Create output folder if it doesn't exist
        createDirectory(ind_output_dir)
        output_file = os.path.join(
            ind_output_dir,
            props["input_file"],
        )

        # Remove specified columns from the group

        group_dropped = dropColumns(group, props["delete_columns"])

        # Filter rows based on non-empty values in specified columns
        group_annulled = removeNullFromColumn(group_dropped, props["check_for_null"])

        # Reorder columns based on a predefined order
        group_filtered = reorderColumns(group_annulled, props["ordered_columns"])

        # Output the file to its destination
        group_filtered.to_csv(
            output_file, mode="a", header=not os.path.exists(output_file), index=False
        )


# Main function
# variables:
async def main():
    app_name = "SplitToMultipleCSV"
    # Initialising logging
    logger = initLogging(app_name)
    # User inputs
    input_dir = getFilePath(
        "Enter the input files location: ",
    )
    output_dir = getFilePath(
        "Enter the output location: ",
    )

    chunk_size = 100000
    progress = ProgressSaver(app_name)
    saved_data = progress.initialiseJSONSaver()

    app_config = readJSON(f"appConfig-{app_name}.json")
    if len(saved_data) > 0:
        continue_from_before = askYNQuestion("Continue from before?(y/n)")
        if not continue_from_before:
            progress.resetSavedData(logger)
    # Get the list of input directory files.
    input_files = listDir(input_dir, get="files")

    print("Files detected in input folder:", len(input_files))

    # Start the process on each input directory files
    for input_file in input_files:
        task_tick = time.time()
        input_full_path = os.path.join(input_dir, input_file)
        # Check saved_data for file
        if input_full_path not in progress.saved_data:
            process_config = {
                "output_dir": output_dir,
                "input_file": input_file,
                "delete_columns": app_config["delete_columns"],
                "check_for_null": app_config["check_for_null"],
                "ordered_columns": app_config["ordered_columns"],
                "column_to_split": app_config["column_to_split"],
            }
            try:
                processCSVInChunks(
                    input_full_path, splitByColumn, process_config, chunk_size
                )

                # Add to the completed files list
                progress.saveToJSON(input_full_path, input_file, logger)

            except Exception as err:
                loggingHandler(logger, f"Exception Occurred: { str(err)} ")
        else:
            loggingHandler(
                logger, f"Skipping file as already complete -> {input_full_path}"
            )
        loggingHandler(
            logger, f"Time taken to complete {input_file} -> {time.time()-task_tick}s"
        )


if __name__ == "__main__":
    import asyncio

    asyncio.run(main())
