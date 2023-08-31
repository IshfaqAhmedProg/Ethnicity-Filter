import os
import time
from typing import List
from traceback import format_exc
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


def reorderColumns(df: DataFrame, ordered_columns: List[str]):
    """Change the order of the columns according to a provided list of columns"""
    columns_to_reorder = [col for col in ordered_columns if col in df.columns]
    df.reindex(columns=columns_to_reorder, fill_value="")
    return df


# Main function
# variables:
async def main():
    app_name = "ReorderColumns"
    print(
        f"Before running this make sure you have a .json file with all the\ncolumn names listed on an array. The order will be maintained.\n  eg:- ['column1','column2']"
    )
    # Initialising logging
    logger = initLogging(app_name)
    # User inputs
    input_dir = getFilePath(
        "Enter the input files location: ",
    )
    output_dir = getFilePath(
        "Enter the output location: ",
    )
    reorder_cols_path = getFilePath(
        "Enter the .json file containing the columns to reorder: ", (".json"), False
    )
    chunk_size = 100000
    progress = ProgressSaver(app_name)

    reorder_cols = readJSON(reorder_cols_path)
    if len(progress.saved_data) > 0:
        continue_from_before = askYNQuestion("Continue from before?(y/n)")
        if not continue_from_before:
            progress.resetSavedData(logger)
    # Get the list of input directory files.
    input_files = listDir(input_dir, get="files")

    # Start the process on each input directory files
    tick = time.time()
    for input_file in input_files:
        task_tick = time.time()
        input_full_path = os.path.join(input_dir, input_file)
        # Check saved_data for file
        if input_full_path not in progress.saved_data:
            try:
                df = processCSVInChunks(
                    input_full_path,
                    reorderColumns,
                    reorder_cols,
                    chunk_size,
                )
                # Output the file to output folder with same name.
                createDirectory(output_dir)
                output_path = os.path.join(output_dir, input_file)
                df.to_csv(output_path, index=False)
                loggingHandler(
                    logger,
                    f"Results for {input_file}, Time taken:{time.time()-task_tick}s -> {output_path}",
                )
                # Add to the completed files list
                progress.saveToJSON(input_full_path, input_file, logger)

            except Exception as err:
                traceback_info = format_exc()
                # Log the exception message along with the traceback information
                loggingHandler(
                    logger, f"Exception occurred: {str(err)}\n{traceback_info}"
                )
        else:
            loggingHandler(
                logger, f"Skipping file as already complete -> {input_full_path}"
            )
    loggingHandler(logger, f"Time taken to complete all files-> {time.time()-tick}s")


if __name__ == "__main__":
    import asyncio

    asyncio.run(main())
