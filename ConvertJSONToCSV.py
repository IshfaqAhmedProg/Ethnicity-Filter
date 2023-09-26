import os
from traceback import format_exc

from pandas import read_json, concat
from pypeepa import (
    getFilePath,
    initLogging,
    loggingHandler,
    listDir,
    createDirectory,
    ProgressSaver,
)
import time


def convertJSONToCSV(input_file, output_file, chunksize):
    # Initialize a JSON file iterator
    json_iterator = read_json(
        input_file, lines=True, chunksize=chunksize
    )  # Adjust the chunk size as needed

    # Initialize an empty list to store the DataFrames
    dfs = []

    # Process each chunk and append it to the list
    for chunk in json_iterator:
        # Append the chunk to the list
        dfs.append(chunk)

    # Concatenate all chunks into a single DataFrame
    final_df = concat(dfs, ignore_index=True)

    # Save the final DataFrame to a CSV file
    final_df.to_csv(output_file, index=False)


# Main function
# variables:
async def main():
    app_name = "ConvertJSONToCSV"
    print("\nConvert any JSON dataset to CSV\n")
    # Initialising Logger
    logger = initLogging(app_name)
    # User inputs
    input_dir = getFilePath(
        "Enter the input files location: ",
    )
    output_dir = getFilePath(
        "Enter the output location: ",
    )
    createDirectory(output_dir)

    # Initialise progress saver
    progress = ProgressSaver(app_name)
    chunksize = 10000
    # If saved_data length more than 0 ask users if they want to continue previous process
    progress.askToContinue(logger)

    # Get the list of input directory files.
    input_files = listDir(input_dir)

    # Start the process on each input directory files
    tick = time.time()
    count = 0
    for input_file in input_files:
        task_tick = time.time()
        input_full_path = os.path.join(input_dir, input_file)
        if input_full_path not in progress.saved_data:
            try:
                # Output the file to output folder with same name.
                output_path = os.path.join(output_dir, input_file)
                convertJSONToCSV(input_full_path, output_path, chunksize)
                loggingHandler(
                    logger,
                    f"Results for {input_file}, Time taken:{time.time()-task_tick}s -> {output_path}",
                )
                progress.saveToJSON(input_full_path, logger=logger)
                count += 1
            except Exception as err:
                traceback_info = format_exc()
                # Log the exception message along with the traceback information
                log_message = (
                    f"Exception occurred: {str(err)}\nTraceback:\n{traceback_info}"
                )
                loggingHandler(logger, log_message)
        else:
            loggingHandler(
                logger, f"Skipping file as already complete -> {input_full_path}"
            )
    loggingHandler(
        logger,
        f"Total time taken:{time.time()-tick}s",
    )


if __name__ == "__main__":
    import asyncio

    asyncio.run(main())
