import os
from traceback import format_exc

from pandas import read_json
from pypeepa import (
    getFilePath,
    initLogging,
    loggingHandler,
    listDir,
    createDirectory,
    ProgressSaver,
    askSelectOptionQuestion,
    countTotalRows,
)
import time


def convertJSONToCSV(input_path, output_path, chunksize):
    # Open the input JSON file for reading
    with open(input_path, "r") as json_file:
        # Initialize a CSV output file for writing
        with open(f"{output_path}_chunk0.csv", "w") as csv_file:
            header_written = False  # To ensure the header is written only once

            # Process the JSON file line by line
            for line_count, line in enumerate(json_file, 1):
                # Load JSON data from the line
                data = read_json(line, lines=True, encoding_errors="ignore")

                # Write the header row if it hasn't been written yet
                if not header_written:
                    header = data.columns.tolist()
                    csv_file.write(",".join(header) + "\n")
                    header_written = True

                # Write the data to the CSV file
                data.to_csv(
                    csv_file,
                    header=False,
                    index=False,
                    mode="a",
                    lineterminator="\n",
                    errors="ignore",
                )

                # Check if it's time to create a new CSV file (chunk)
                if line_count % chunksize == 0:
                    csv_file.close()  # Close the current chunk
                    chunk_number = line_count // chunksize
                    new_output_file = f"{output_path}_chunk{chunk_number}.csv"
                    csv_file = open(new_output_file, "w")  # Open a new chunk file
                    header_written = False


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

    # Get the total lines in the file

    # Initialise progress saver
    progress = ProgressSaver(app_name)
    chunksize = 10000
    # If saved_data length more than 0 ask users if they want to continue previous process
    progress.askToContinue(logger)

    # Get the list of input directory files.
    input_files = listDir(input_dir)

    chunkify = False

    # Start the process on each input directory files
    tick = time.time()
    count = 0
    for input_file in input_files:
        task_tick = time.time()
        input_full_path = os.path.join(input_dir, input_file)
        if input_full_path not in progress.saved_data:
            try:
                # Output the file to output folder with same name.
                loggingHandler(
                    logger, f"Counting total lines for {input_file}, please wait..."
                )
                total_lines_in_input = countTotalRows(input_full_path)
                loggingHandler(
                    logger, f"Found {total_lines_in_input} lines in {input_file}!"
                )
                if total_lines_in_input > 10000:
                    chunkify = True
                if chunkify:
                    chunksize = askSelectOptionQuestion(
                        "Output is too large and will be split to chunks, select the line count for each chunk:",
                        100,
                        1000000,
                    )
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
