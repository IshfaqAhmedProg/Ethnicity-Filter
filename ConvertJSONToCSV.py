import os
from traceback import format_exc

from pandas import json_normalize
from pypeepa import (
    getFilePath,
    initLogging,
    loggingHandler,
    listDir,
    createDirectory,
    ProgressSaver,
)
import time
import json


def convertJSONToCSV(input_file, output_file):
    # Open the JSON file for reading
    with open(input_file, "r") as json_file:
        # Initialize an empty list to store the parsed JSON data
        json_data = []

        # Iterate through each line in the JSON file
        for line in json_file:
            # Parse each line as JSON and append it to the list
            try:
                json_line = json.loads(line)
                json_data.append(json_line)
            except json.JSONDecodeError as e:
                # Handle JSON decoding errors if necessary
                print(f"Error decoding JSON: {e}")

    # Convert the list of JSON data to a Pandas DataFrame
    df = json_normalize(json_data)

    # Save the DataFrame to a CSV file
    df.to_csv(output_file, index=False)


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
                convertJSONToCSV(input_full_path, output_path)
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
