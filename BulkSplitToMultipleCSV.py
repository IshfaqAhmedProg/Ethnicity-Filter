import os
import time
from pandas import read_csv, DataFrame
from typing import Any
from pypeepa import (
    initLogging,
    createDirectory,
    getFilePath,
    listDir,
    loggingHandler,
    processCSVInChunks,
    printArray,
    selectOptionQuestion,
    askYNQuestion,
    ProgressSaver,
)


def split_and_save_data(chunk: DataFrame, process_config: Any):
    for category, names_list in process_config["common_vals"].items():
        # Filter the data based on the names
        filtered_data = chunk[chunk[process_config["column_name"]].isin(names_list)]

        # Output only if there is filtered data
        if len(filtered_data.index) != 0:
            category_dir = os.path.join(process_config["output_dir"], category)
            createDirectory(category_dir)
            output_file = os.path.join(category_dir, process_config["input_file"])
            filtered_data.to_csv(output_file, index=False)
            loggingHandler(
                logger=process_config["logger"],
                log_mssg=f"Saved {len(filtered_data)} records to {output_file}",
            )


# Main function
# variables:
async def main():
    app_name = "BulkSplitByColumn"
    # Initialising logging
    logger = initLogging(app_name)
    # User inputs
    input_dir = getFilePath(
        "Enter the input files location: ",
    )
    output_dir = getFilePath(
        "Enter the output location: ",
    )
    common_vals_path = getFilePath(
        message="Enter the path to the common values: ",
        endswith=tuple(".csv"),
        folder=False,
    )
    chunk_size = 100000
    progress = ProgressSaver(app_name)
    saved_data = progress.initialiseJSONSaver()

    common_vals = read_csv(common_vals_path, low_memory=False).to_dict(orient="list")
    # If saved_data length more than 0 ask users if they want to continue previous process
    if len(saved_data) > 0:
        continue_from_before = askYNQuestion("Continue from before?(y/n)")
        if not continue_from_before:
            progress.resetSavedData(logger)
    # Get the list of input directory files.
    input_files = listDir(input_dir, get="files")

    file_columns_same = askYNQuestion(
        "Are all the column names the same for all the files in the input dir?(y/n)"
    )
    # Start the process on each input directory files
    tick = time.time()
    first_file = True
    for input_file in input_files:
        task_tick = time.time()
        input_full_path = os.path.join(input_dir, input_file)
        # Check saved_data for file
        if input_full_path not in progress.saved_data:
            try:
                if not file_columns_same or first_file:
                    # Get the first line for header names
                    columns = read_csv(input_full_path, nrows=1).columns
                    printArray(columns)
                    column_index = selectOptionQuestion(
                        question=f"Enter the index of the column to filter for the file {input_file}",
                        min=1,
                        max=len(columns),
                    )
                process_config = {
                    "output_dir": output_dir,
                    "input_file": input_file,
                    "common_vals": common_vals,
                    "column_name": columns[int(column_index) - 1],
                    "logger": logger,
                }
                processCSVInChunks(
                    csv_file=input_full_path,
                    process_function=split_and_save_data,
                    pf_args=process_config,
                    chunk_size=chunk_size,
                )

                # Add to the completed files list
                progress.saveToJSON(input_full_path, input_file, logger)
                first_file = False
            except Exception as err:
                loggingHandler(logger, f"Exception Occurred: { str(err)} ")
        else:
            loggingHandler(
                logger, f"Skipping file as already complete -> {input_full_path}"
            )
        loggingHandler(
            logger, f"Time taken to complete {input_file} -> {time.time()-task_tick}s"
        )
    loggingHandler(logger, f"Time taken to complete all files -> {time.time()-tick}s")


if __name__ == "__main__":
    import asyncio

    asyncio.run(main())
