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
    selectOptionQuestion,
    printArray,
)


def askHeaderForMultipleCSV(csv_list: List[str], csv_dir: str) -> List[Tuple[str, str]]:
    files_and_header = []
    prev_cols = []
    col_index = None
    for csv_file in csv_list:
        csv_full_path = os.path.join(csv_dir, csv_file)
        current_columns = read_csv(
            csv_full_path, nrows=1, encoding_errors="ignore", encoding="cp437"
        ).columns
        if not numpy.array_equal(numpy.array(prev_cols), numpy.array(current_columns)):
            printArray(current_columns)
            col_index = selectOptionQuestion(
                question=f"Enter the index of the column you want to match.",
                min=1,
                max=len(current_columns),
            )
            prev_cols = current_columns
        col_name = current_columns[col_index - 1]
        files_and_header.append((csv_full_path, col_name))

    return files_and_header


def removeNullFromColumn(df, check_for_null_columns):
    """Remove any null values in the column specified, if more than one column is specified then if all are null only
    remove those."""
    for col in check_for_null_columns:
        if col in df.columns:
            df = df[df[col].notnull()]
    return df


def innerJoinCSVFiles(dataframe: DataFrame, config):
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
    left_df = dataframe.copy()
    left_col = config["left_col"]
    left_df[left_col] = left_df[left_col].astype(str)
    print("before processing:left: \n", left_df.head())
    for right_file in config["right_files_and_headers"]:
        # Load the right CSV file into a DataFrame
        right_df = read_csv(right_file[0], low_memory=False, encoding_errors="ignore")

        right_col = right_file[1]
        print(
            f"before dropping dupes and removing null:right:{right_file[0]} \n",
            right_df.head(),
        )
        # Remove null values from right_df
        right_df = removeNullFromColumn(right_df, [right_col])

        # Remove duplicates keeping the first occurrence in the right DataFrame
        right_df[right_col] = right_df[right_col].astype(str)
        right_df.drop_duplicates(subset=right_col, keep="first", inplace=True)

        print("after dropping dupes and removing null:right: \n", right_df.head())

        # Perform the left join
        left_df = left_df.merge(
            right_df,
            how="inner",
            left_on=left_col,
            right_on=right_col,
        )
    print("after processing:left: \n", left_df.head())
    return left_df


def main():
    app_name = "JoinMultipleCSV"
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
    import asyncio

    asyncio.run(main())
