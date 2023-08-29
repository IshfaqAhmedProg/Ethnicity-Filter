import pandas
import time
import os
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
from typing import List, Tuple


def joinCSVFiles(dataframe, config):
    """
    Perform left join on a DataFrame with multiple CSV files, removing duplicates from right CSVs.

    @param:`dataframe`: The initial DataFrame.
    @param:`config`: Dictionary containing configurations for the join.
        @key: `join_column`: The column of the left file\n
        @key: `right_files`: The column of the left file\n
        Example config:
            {\n
            'join_column': 'id',\n
            'right_files_and_header': [('path/to/1.csv', "E-mail"), ('path/to/2.csv', "email")]\n
            }\n

    @return:pd.DataFrame: DataFrame with all columns from right CSVs joined based on the specified configurations.
    """
    result_df = dataframe.copy()

    for right_file in config["right_files_and_header"]:
        # Load the right CSV file into a DataFrame
        right_df = pandas.read_csv(
            right_file[0], low_memory=False, encoding_errors="ignore"
        )

        # Remove duplicates keeping the first occurrence in the right DataFrame
        right_df.drop_duplicates(
            subset=config["join_column"], keep="first", inplace=True
        )

        # Perform the left join
        result_df = result_df.merge(
            right_df,
            how="left",
            left_on=config["join_column"],
            right_on=right_file[1],
        )

    return result_df


def main():
    app_name = "LeftJoinMultipleCSV"
    logger = initLogging(app_name)
    # User inputs
    left_dir = getFilePath(
        "Enter the path to the .csv files, to which you want to join the [columns](left)\n Make sure all the files have the same columns: ",
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
        # Get the list of input directory files.
    left_files = listDir(left_dir, "files")
    right_files = listDir(right_dir, "files")
    right_files_and_header = askHeaderForMultipleCSV(right_files, right_dir)
    # Start the process on each input directory files
    tick = time.time()
    for left_file in left_files:
        task_tick = time.time()
        left_full_path = os.path.join(left_dir, left_file)
        if left_full_path not in progress.saved_data:
            try:
                process_config = {
                    "join_column": "email",
                    "right_files_and_header": right_files_and_header,
                }
                df = processCSVInChunks(
                    left_full_path, joinCSVFiles, process_config, chunk_size
                )
                df.to_csv()
                output_path = os.path.join(output_dir, left_file)
                loggingHandler(
                    logger,
                    f"Results for {left_file}, Time taken:{time.time()-task_tick}s -> {output_path}",
                )

                # Add to the completed files list
                progress.saveToJSON(left_full_path, left_file, logger)

            except Exception as err:
                loggingHandler(logger, f"Exception Occurred:  {str(err)}")

        else:
            loggingHandler(
                logger, f"Skipping file as already complete -> {left_full_path}"
            )
    loggingHandler(
        logger,
        f"Total time taken:{time.time()-tick}s",
    )


def askHeaderForMultipleCSV(csv_list, csv_dir) -> List[Tuple[str, str]]:
    files_and_header = []

    for csv_file in csv_list:
        csv_full_path = os.path.join(csv_dir, csv_file)
        all_columns = pandas.read_csv(csv_full_path, nrows=1).columns
        printArray(all_columns)
        col_index = selectOptionQuestion(
            question=f"Enter the index of the column you want to match.",
            min=1,
            max=len(all_columns),
        )
        col_name = all_columns[col_index - 1]
        files_and_header.append((csv_full_path, col_name))

    return files_and_header


if __name__ == "__main__":
    import asyncio

    asyncio.run(main())
