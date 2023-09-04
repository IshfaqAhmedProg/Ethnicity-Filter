"""
    Appends csv files in a directory and outputs it to a csv file with the same name as the directory
"""
import os
import glob
import pandas as pd
from pypeepa import (
    getFilePath,
    initLogging,
    listDir,
    createDirectory,
    progressBarIterator,
    loggingHandler,
)


def appendCSVFiles(directory_path, output_file_path, logger, chunk_size=1000):
    """
    Appends csv files in a directory and outputs it to a csv file with the same name as the directory
    """
    # Find all CSV files in the given directory
    csv_files = glob.glob(os.path.join(directory_path, "*.csv"))

    if len(csv_files) == 0:
        print("No CSV files found in the directory.")
        return

    # Initialize the output file
    header_written = False

    # Loop through each CSV file and append its contents to the output file
    with open(output_file_path, "wb") as output_file:
        for csv_file in progressBarIterator(
            iterable=csv_files,
        ):
            for chunk in pd.read_csv(csv_file, chunksize=chunk_size):
                if not header_written:
                    chunk.to_csv(output_file, index=False, header=True, mode="wb")
                    header_written = True
                else:
                    chunk.to_csv(output_file, index=False, header=False, mode="ab")

    loggingHandler(logger, f"Combined data saved to '{output_file_path}'.")


async def main():
    app_name = "AppendCSVFiles"
    # Initialising logging
    logger = initLogging(app_name)

    # User inputs
    input_dir = getFilePath(
        "Enter the location of the folders containing the files to combine: ",
    )
    output_dir = getFilePath(
        "Enter the output location: ",
    )
    # Get the folders from the input_dir
    input_dir_content = listDir(input_dir, "folders")

    # Create outputdirectory if doesnt exist
    createDirectory(output_dir)

    for directory in input_dir_content:
        input_path = os.path.join(input_dir, directory)
        output_file_path = os.path.join(output_dir, f"{directory}.csv")
        appendCSVFiles(input_path, output_file_path, logger, 10000)


if __name__ == "__main__":
    import asyncio

    asyncio.run(main())
