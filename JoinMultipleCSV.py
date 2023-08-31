import os
import time
import numpy
from typing import List, Tuple
from traceback import format_exc
from pandas import read_csv, DataFrame
from pypeepa import (
    initLogging,
    ProgressSaver,
    askYNQuestion,
    createDirectory,
    getFilePath,
    listDir,
    loggingHandler,
    processCSVInChunks,
    askHeaderForMultipleCSV,
)


def removeNullFromColumn(df: DataFrame, check_for_null_columns: List[str]):
    """Remove any null values in the column specified, if more than one column is specified then if all are null only
    remove those."""
    for col in check_for_null_columns:
        if col in df.columns:
            df = df[df[col].notnull()]
    return df


def dropColumns(df: DataFrame, delete_columns: List[str]):
    """Remove multiple columns from a dataframe"""
    columns_to_drop = [col for col in delete_columns if col in df.columns]
    df.drop(columns=columns_to_drop, errors="ignore")
    return df


def innerJoinCSVFiles(chunk: DataFrame, config):
    """
    Perform left join on a DataFrame with multiple CSV files, removing duplicates from right CSVs.

    @param:`dataframe`: The initial DataFrame.
    @param:`config`: Dictionary containing configurations for the join.
        @key: `left_col`: The column of the left file\n
        @key: `right_files_and_headers`: The column of the left file\n
        Example config:
            {\n
            'left_col': 'id',\n
            'right_files_and_headers': [('path/to/1.csv', "E-mail"), ('path/to/2.csv', "email")]\n
            }\n

    @return:pd.DataFrame: DataFrame with all columns from right CSVs joined based on the specified configurations.
    """
    left_df = chunk.copy()
    left_col = config["left_col"]
    left_df[left_col] = left_df[left_col].astype(str)

    for right_file in config["right_files_and_headers"]:
        right_df = read_csv(right_file[0], low_memory=False, encoding_errors="ignore")

        right_col = right_file[1]
        right_df = removeNullFromColumn(right_df, [right_col])
        right_df[right_col] = right_df[right_col].astype(str)
        right_df.drop_duplicates(subset=right_col, keep="first", inplace=True)

        # print(
        #     "Shapes before merge - left_df:",
        #     left_df.info(),
        #     "right_df:",
        #     right_df.info(),
        # )

        merged_df = left_df.merge(
            right_df,
            how="left",
            left_on=left_col,
            right_on=right_col,
        )

        left_df = merged_df.drop(columns=[right_col])
        # print("Shape after merge and drop:", left_df.info())

    return left_df


def main():
    app_name = "JoinMultipleCSV"
    print(
        "Before running this, make sure you have the files you want to join to (left) in one folder.\nAnd the files you want to join with (right) in another folder."
    )
    logger = initLogging(app_name)
    # User inputs
    left_dir = getFilePath(
        "Enter the path to the .csv files, to which you want to join the [columns](left): ",
    )
    right_dir = getFilePath(
        "Enter the path to the .csv files, which has the [columns] to be joined(right): ",
    )
    output_dir = getFilePath(
        "Enter the output location: ",
    )
    createDirectory(output_dir)

    chunk_size = 100000

    progress = ProgressSaver(app_name)

    # If saved_data length more than 0 ask users if they want to continue previous process
    if len(progress.saved_data) > 0:
        continue_from_before = askYNQuestion("Continue from before?(y/n)")
        if not continue_from_before:
            progress.resetSavedData(logger)

    # Get the list of left directory files.
    left_files = listDir(left_dir, "files")
    # Get the list of right directory files.
    right_files = listDir(right_dir, "files")

    # Get the list of tuples containing the full paths and header names for left directory files.
    left_files_and_headers = askHeaderForMultipleCSV(left_files, left_dir)
    # Get the list of tuples containing the full paths and header names for right directory files.
    right_files_and_headers = askHeaderForMultipleCSV(right_files, right_dir)

    # Start the process on each input directory files
    tick = time.time()
    for left_file in left_files_and_headers:
        task_tick = time.time()
        left_full_path = left_file[0]
        if left_full_path not in progress.saved_data:
            try:
                process_config = {
                    "left_col": left_file[1],
                    "right_files_and_headers": right_files_and_headers,
                }
                df = processCSVInChunks(
                    left_full_path, innerJoinCSVFiles, process_config, chunk_size
                )
                output_path = os.path.join(output_dir, os.path.basename(left_file[0]))
                df.to_csv(output_path, index=False)
                # Add to the completed files list
                progress.saveToJSON(
                    left_full_path, os.path.basename(left_file[0]), logger
                )

            except Exception as err:
                # Log the exception message along with the traceback information
                traceback = format_exc()
                loggingHandler(
                    logger,
                    f"Exception occurred: {str(err)}\n{traceback}",
                )
        else:
            loggingHandler(
                logger, f"Skipping file as already complete -> {left_full_path}"
            )
        loggingHandler(
            logger, f"Time taken to complete {left_file[0]} -> {time.time()-task_tick}s"
        )
    loggingHandler(logger, f"Time taken to complete all files -> {time.time()-tick}s")


if __name__ == "__main__":
    main()
