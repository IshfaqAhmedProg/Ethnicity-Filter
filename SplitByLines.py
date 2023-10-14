import os
import time
from pypeepa import (
    checkEncoding,
    loggingHandler,
    initLogging,
    createDirectory,
    getFilePath,
    askSelectOptionQuestion,
    loggingHandler,
    countTotalRows,
    progressBarIterator,
)
from traceback import format_exc


def splitByLines(input_file_path, output_dir, max_lines, file_encoding, total_lines):
    with open(input_file_path, "r", encoding=file_encoding) as input_file:
        file_number = 1
        line_count = 0
        output_file = None

        for line in progressBarIterator(input_file, total_lines):
            if line_count % max_lines == 0:
                if output_file:
                    output_file.close()
                output_file = open(
                    os.path.join(output_dir, f"{file_number}.txt"),
                    "w",
                    encoding=file_encoding,
                )
                file_number += 1
            output_file.write(line)
            line_count += 1

        if output_file:
            output_file.close()


async def main():
    app_name = "SplitByLines"
    print("Split a file to multiple files based on a number of lines")
    # Initialising logging
    logger = initLogging(app_name)
    # User inputs
    input_file = getFilePath(
        "Enter the path of the file you want to split: ",
    )
    output_dir = getFilePath(
        "Enter the output location: ",
    )
    createDirectory(output_dir)

    loggingHandler(log_mssg="Counting lines please wait...")
    total_lines = countTotalRows(input_file)
    loggingHandler(logger, f"Total lines found:{total_lines}")
    max_lines = askSelectOptionQuestion(
        "Select the max no. of lines in each part", 1, total_lines
    )

    task_tick = time.time()
    try:
        # Check if the file exists
        if not os.path.exists(input_file):
            raise Exception(f"File '{input_file}' does not exist.")

        # Check if the file is a regular file
        if not os.path.isfile(input_file):
            raise Exception(f"'{input_file}' is not a regular file.")

        # Detect the file's encoding
        loggingHandler(log_mssg="Checking encoding of the file, please wait...")
        file_encoding = checkEncoding(input_file)

        if not file_encoding:
            raise Exception(f"Unable to detect the encoding for '{input_file}'.")
        else:
            loggingHandler(logger, f"Encoding of the file is:{file_encoding}")

        # Start the splitting process
        splitByLines(input_file, output_dir, max_lines, file_encoding, total_lines)
    except Exception as err:
        traceback_info = format_exc()
        # Log the exception message along with the traceback information
        log_message = f"Exception occurred: {str(err)}\nTraceback:\n{traceback_info}"
        loggingHandler(logger, log_message)
    loggingHandler(
        logger, f"Time taken to complete {input_file} -> {time.time()-task_tick}s"
    )


if __name__ == "__main__":
    import asyncio

    asyncio.run(main())
